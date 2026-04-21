from __future__ import annotations

import csv
import logging
import re
import sqlite3
import unicodedata
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
from threading import Lock
from urllib.parse import urlparse

from backend.app.config import get_settings
from backend.app.cache.database import get_connection


logger = logging.getLogger(__name__)


IdentifierScheme = str

KNOWN_SCHEMES: frozenset[IdentifierScheme] = frozenset(
    {"eter", "deqar", "openalex", "ror", "neaa", "erasmus", "wikidata"}
)

_NAME_PUNCT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)
_NAME_WHITESPACE_RE = re.compile(r"\s+")
_SEED_SPLIT_RE = re.compile(r"\s*[;|]\s*")

FUZZY_NAME_THRESHOLD = 0.92

SEED_NAME_COLUMNS = (
    "canonical_name",
    "institution_name",
    "display_name",
    "name_primary",
    "name",
    "english_name",
    "name_en",
)
SEED_OFFICIAL_NAME_COLUMNS = (
    "official_name",
    "legal_name",
    "institution_name_native",
    "name_official",
)
SEED_ALIAS_COLUMNS = (
    "aliases",
    "alternate_names",
    "name_aliases",
    "other_names",
)
SEED_COUNTRY_COLUMNS = ("country_code", "country")
SEED_WEBSITE_COLUMNS = ("homepage_url", "website_url", "website", "url", "institution_url")
SEED_TYPE_COLUMNS = (
    "institution_type",
    "institution_type_label",
    "type",
    "sector",
    "category",
    "institution_category",
)
SEED_LEGAL_STATUS_COLUMNS = (
    "legal_status",
    "legal_form",
    "ownership",
    "institution_control",
    "control",
)

SEED_IDENTIFIER_COLUMNS: tuple[tuple[IdentifierScheme, tuple[str, ...]], ...] = (
    ("eter", ("eter_id", "eter", "eterID")),
    ("ror", ("ror", "ror_id", "ror_url")),
    ("openalex", ("openalex_id", "openalex")),
    ("wikidata", ("wikidata_id", "wikidata")),
    ("erasmus", ("erasmus_code", "erasmus_id", "erasmus")),
)

# Keep these keys aligned with the DEQAR client's MATCH_CONFIDENCE_BY_TYPE so
# consumers can reuse the same confidence metadata.
CASCADE_MATCH_TYPES = (
    "website",
    "exact_name",
    "exact_name_and_website",
    "website_and_fuzzy_name",
    "fuzzy_name",
)

IDENTIFIER_MATCH_TYPES = {
    "eter": "eter_id",
    "ror": "ror",
    "deqar": "deqar_id",
}

MERGE_MATCH_TYPES = frozenset({*CASCADE_MATCH_TYPES, *IDENTIFIER_MATCH_TYPES.values()})


@dataclass(slots=True)
class InstitutionRecord:
    institution_uid: str
    canonical_name: str
    country_code: str
    website_host: str | None
    eter_id: str | None
    institution_type: str | None
    legal_status: str | None
    status: str
    merged_into_uid: str | None
    first_seen_at: str
    last_verified_at: str
    identifiers: dict[IdentifierScheme, str] = field(default_factory=dict)


@dataclass(slots=True)
class IdentifierAssertion:
    scheme: IdentifierScheme
    value: str
    source: str
    confidence: float


@dataclass(slots=True)
class NameVariant:
    variant: str
    source: str
    language: str | None = None


@dataclass(slots=True)
class NameMatch:
    institution: InstitutionRecord
    matched_variant: str
    match_type: str
    confidence: float


@dataclass(slots=True)
class RegistrationRequest:
    canonical_name: str
    country_code: str
    identifiers: list[IdentifierAssertion]
    name_variants: list[NameVariant] = field(default_factory=list)
    website_host: str | None = None
    institution_type: str | None = None
    legal_status: str | None = None
    source: str = "unknown"


def _is_latin(ch: str) -> bool:
    return "LATIN" in unicodedata.name(ch, "")


def normalize_name(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    last_base: str | None = None
    kept_chars: list[str] = []
    for ch in decomposed:
        if unicodedata.category(ch) == "Mn":
            if last_base is not None and _is_latin(last_base):
                continue
            kept_chars.append(ch)
        else:
            kept_chars.append(ch)
            last_base = ch
    recomposed = unicodedata.normalize("NFC", "".join(kept_chars))
    folded = recomposed.casefold()
    folded = folded.replace("&", " and ")
    folded = folded.replace("st.", "st ")
    folded = folded.replace("saint ", "st ")
    no_punct = _NAME_PUNCT_RE.sub(" ", folded)
    return _NAME_WHITESPACE_RE.sub(" ", no_punct).strip()


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _record_from_row(row: sqlite3.Row) -> InstitutionRecord:
    return InstitutionRecord(
        institution_uid=row["institution_uid"],
        canonical_name=row["canonical_name"],
        country_code=row["country_code"],
        website_host=row["website_host"],
        eter_id=row["eter_id"],
        institution_type=row["institution_type"] if "institution_type" in row.keys() else None,
        legal_status=row["legal_status"] if "legal_status" in row.keys() else None,
        status=row["status"],
        merged_into_uid=row["merged_into_uid"],
        first_seen_at=row["first_seen_at"],
        last_verified_at=row["last_verified_at"],
    )


class InstitutionRegistry:
    """Canonical institution identity layer backed by the crosswalk tables.

    All identity lookups in the system should funnel through this class rather
    than each source client running its own match cascade at request time.
    """

    _seed_lock = Lock()
    _loaded_seed_signature: tuple[str, int, int] | None = None
    _failed_seed_signature: tuple[str, int, int] | None = None

    def resolve(
        self,
        scheme: IdentifierScheme,
        value: str,
        *,
        log_lookup: bool = True,
    ) -> InstitutionRecord | None:
        self._ensure_seed_loaded()
        if not value:
            return None
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT i.*
                FROM institution_identifiers id
                JOIN institutions i ON i.institution_uid = id.institution_uid
                WHERE id.scheme = ? AND id.value = ?
                """,
                (scheme, value),
            ).fetchone()
            if not row:
                if log_lookup:
                    self._log_match(conn, None, scheme, value, match_type="miss", confidence=0.0)
                    conn.commit()
                return None
            record = _record_from_row(row)
            record.identifiers = self._identifiers_for(conn, record.institution_uid)
            if log_lookup:
                self._log_match(
                    conn,
                    record.institution_uid,
                    scheme,
                    value,
                    match_type=f"{scheme}_id",
                    confidence=1.0,
                )
                conn.commit()
        return self._follow_merge(record)

    def resolve_by_name(
        self,
        name: str,
        country_code: str | None = None,
    ) -> list[NameMatch]:
        self._ensure_seed_loaded()
        normalized = normalize_name(name)
        if not normalized:
            return []
        with get_connection() as conn:
            params: list[object] = [normalized]
            query = """
                SELECT i.*, v.variant AS matched_variant
                FROM institution_name_variants v
                JOIN institutions i ON i.institution_uid = v.institution_uid
                WHERE v.normalized = ?
            """
            if country_code:
                query += " AND i.country_code = ?"
                params.append(country_code)
            rows = conn.execute(query, params).fetchall()
            matches: list[NameMatch] = []
            for row in rows:
                record = _record_from_row(row)
                record.identifiers = self._identifiers_for(conn, record.institution_uid)
                matches.append(
                    NameMatch(
                        institution=self._follow_merge(record),
                        matched_variant=row["matched_variant"],
                        match_type="exact_name",
                        confidence=0.96,
                    )
                )
            self._log_match(
                conn,
                matches[0].institution.institution_uid if len(matches) == 1 else None,
                "name",
                name,
                match_type="exact_name" if len(matches) == 1 else "name_ambiguous",
                confidence=0.96 if len(matches) == 1 else 0.0,
            )
            conn.commit()
        return matches

    def register(
        self,
        *,
        canonical_name: str,
        country_code: str,
        identifiers: list[IdentifierAssertion],
        name_variants: list[NameVariant] | None = None,
        website_host: str | None = None,
        institution_type: str | None = None,
        legal_status: str | None = None,
        source: str,
    ) -> InstitutionRecord:
        """Idempotent upsert. Existing institutions are found via identifiers;
        new ones get a freshly minted UID.
        """
        self._ensure_seed_loaded()
        if not identifiers:
            raise ValueError("register() requires at least one identifier assertion")

        request = RegistrationRequest(
            canonical_name=canonical_name,
            country_code=country_code,
            identifiers=identifiers,
            name_variants=list(name_variants or []),
            website_host=website_host,
            institution_type=institution_type,
            legal_status=legal_status,
            source=source,
        )
        now = _now_iso()
        with get_connection() as conn:
            institution_uid, match_type, confidence = self._resolve_or_mint_uid(conn, request, now)
            if match_type in MERGE_MATCH_TYPES:
                primary = identifiers[0]
                self._log_match(
                    conn,
                    institution_uid,
                    primary.scheme,
                    primary.value,
                    match_type=match_type,
                    confidence=confidence,
                )
            for ident in identifiers:
                self._assert_identifier(conn, institution_uid, ident, now)
            for variant in request.name_variants:
                self._assert_name_variant(conn, institution_uid, variant, source)

            conn.commit()
            row = conn.execute(
                "SELECT * FROM institutions WHERE institution_uid = ?",
                (institution_uid,),
            ).fetchone()
            record = _record_from_row(row)
            record.identifiers = self._identifiers_for(conn, institution_uid)
        return record

    def bulk_register(
        self,
        requests: list[RegistrationRequest],
        *,
        skip_cascade: bool = False,
    ) -> dict[str, str]:
        """Register many institutions in one transaction.

        Returns a mapping from the first identifier's ``value`` to the resolved
        ``institution_uid`` so callers can correlate input rows to stored UIDs.

        ``skip_cascade=True`` disables the fuzzy/website name cascade and relies
        purely on identifier matches. Use it when the source's own rows are
        authoritatively distinct (e.g. DEQAR CSV bulk load) — the cascade there
        is O(N²) and will false-merge genuinely separate institutions that
        happen to share a website or near-identical name.
        """
        self._ensure_seed_loaded()
        return self._bulk_register_requests(requests, skip_cascade=skip_cascade)

    def _bulk_register_requests(
        self,
        requests: list[RegistrationRequest],
        *,
        skip_cascade: bool = False,
    ) -> dict[str, str]:
        if not requests:
            return {}

        now = _now_iso()
        mapping: dict[str, str] = {}
        with get_connection() as conn:
            for request in requests:
                if not request.identifiers:
                    continue
                uid, match_type, confidence = self._resolve_or_mint_uid(
                    conn, request, now, skip_cascade=skip_cascade
                )
                if match_type in MERGE_MATCH_TYPES:
                    primary = request.identifiers[0]
                    self._log_match(
                        conn,
                        uid,
                        primary.scheme,
                        primary.value,
                        match_type=match_type,
                        confidence=confidence,
                    )
                for ident in request.identifiers:
                    self._assert_identifier(conn, uid, ident, now)
                for variant in request.name_variants:
                    self._assert_name_variant(conn, uid, variant, request.source)
                mapping[request.identifiers[0].value] = uid
            conn.commit()
        return mapping

    def _ensure_seed_loaded(self) -> None:
        seed_path = get_settings().eheso_eter_institutions_csv_path
        if not seed_path.is_file():
            return

        signature = seed_file_signature(seed_path)
        if self.__class__._loaded_seed_signature == signature or self.__class__._failed_seed_signature == signature:
            return

        with self.__class__._seed_lock:
            if self.__class__._loaded_seed_signature == signature or self.__class__._failed_seed_signature == signature:
                return
            try:
                requests, stats = load_eheso_seed_requests(seed_path)
                mapping = self._bulk_register_requests(requests, skip_cascade=True)
                logger.info(
                    "EHESO/ETER seed ingest loaded %d rows from %s (%d registered, %d skipped missing name, %d skipped missing country, %d skipped missing identifiers)",
                    stats["rows_seen"],
                    seed_path,
                    len(mapping),
                    stats["skipped_missing_name"],
                    stats["skipped_missing_country"],
                    stats["skipped_missing_identifiers"],
                )
                self.__class__._loaded_seed_signature = signature
                self.__class__._failed_seed_signature = None
            except Exception:  # noqa: BLE001
                logger.exception("EHESO/ETER seed ingest failed for %s", seed_path)
                self.__class__._failed_seed_signature = signature

    def _resolve_or_mint_uid(
        self,
        conn: sqlite3.Connection,
        request: RegistrationRequest,
        now: str,
        *,
        skip_cascade: bool = False,
    ) -> tuple[str, str, float]:
        """Return ``(uid, match_type, confidence)`` for the registration target.

        match_type is either an identifier-tier label (e.g. ``"eter_id"``, ``"ror"``),
        one of ``CASCADE_MATCH_TYPES``, or ``"new"`` when a fresh institution was minted.
        """
        identifier_hit = self._find_uid_by_identifiers(conn, request.identifiers)
        if identifier_hit is not None:
            existing_uid, matching_scheme = identifier_hit
            self._update_existing_institution(conn, existing_uid, request, now)
            match_type = IDENTIFIER_MATCH_TYPES.get(matching_scheme, f"{matching_scheme}_id")
            return existing_uid, match_type, 1.0

        cascade = None if skip_cascade else self._find_merge_candidate(conn, request)
        if cascade is not None:
            uid, match_type, confidence = cascade
            self._update_existing_institution(conn, uid, request, now)
            return uid, match_type, confidence

        uid = uuid.uuid4().hex
        eter_value = next(
            (ident.value for ident in request.identifiers if ident.scheme == "eter"),
            None,
        )
        conn.execute(
            """
            INSERT INTO institutions (
                institution_uid, canonical_name, country_code, website_host,
                eter_id, institution_type, legal_status, status,
                merged_into_uid, first_seen_at, last_verified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', NULL, ?, ?)
            """,
            (
                uid,
                request.canonical_name,
                request.country_code,
                request.website_host,
                eter_value,
                request.institution_type,
                request.legal_status,
                now,
                now,
            ),
        )
        return uid, "new", 1.0

    @staticmethod
    def _update_existing_institution(
        conn: sqlite3.Connection,
        uid: str,
        request: RegistrationRequest,
        now: str,
    ) -> None:
        eter_value = next(
            (ident.value for ident in request.identifiers if ident.scheme == "eter"),
            None,
        )
        conn.execute(
            """
            UPDATE institutions
            SET last_verified_at = ?,
                website_host = COALESCE(website_host, ?),
                eter_id = COALESCE(eter_id, ?),
                institution_type = COALESCE(institution_type, ?),
                legal_status = COALESCE(legal_status, ?)
            WHERE institution_uid = ?
            """,
            (
                now,
                request.website_host,
                eter_value,
                request.institution_type,
                request.legal_status,
                uid,
            ),
        )

    def merge(self, *, uid_from: str, uid_into: str) -> None:
        if uid_from == uid_into:
            return
        now = _now_iso()
        with get_connection() as conn:
            conn.execute(
                """
                UPDATE institution_identifiers
                SET institution_uid = ?
                WHERE institution_uid = ?
                """,
                (uid_into, uid_from),
            )
            conn.execute(
                """
                UPDATE institution_name_variants
                SET institution_uid = ?
                WHERE institution_uid = ?
                """,
                (uid_into, uid_from),
            )
            conn.execute(
                """
                UPDATE institutions
                SET status = 'merged', merged_into_uid = ?, last_verified_at = ?
                WHERE institution_uid = ?
                """,
                (uid_into, now, uid_from),
            )
            conn.commit()

    def identifiers_for(self, institution_uid: str) -> dict[IdentifierScheme, str]:
        self._ensure_seed_loaded()
        with get_connection() as conn:
            return self._identifiers_for(conn, institution_uid)

    def last_merge_match_type(self, institution_uid: str) -> str | None:
        """Most recent merge-tier match_type logged against this institution, if any."""
        self._ensure_seed_loaded()
        placeholders = ",".join("?" for _ in MERGE_MATCH_TYPES)
        params: list[object] = [institution_uid, *MERGE_MATCH_TYPES]
        with get_connection() as conn:
            row = conn.execute(
                f"""
                SELECT match_type
                FROM institution_match_log
                WHERE institution_uid = ?
                  AND match_type IN ({placeholders})
                ORDER BY id DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
        return row["match_type"] if row else None

    def _follow_merge(self, record: InstitutionRecord) -> InstitutionRecord:
        if record.status != "merged" or not record.merged_into_uid:
            return record
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM institutions WHERE institution_uid = ?",
                (record.merged_into_uid,),
            ).fetchone()
            if not row:
                return record
            target = _record_from_row(row)
            target.identifiers = self._identifiers_for(conn, target.institution_uid)
        return target

    @staticmethod
    def _find_uid_by_identifiers(
        conn: sqlite3.Connection,
        identifiers: list[IdentifierAssertion],
    ) -> tuple[str, str] | None:
        """Return ``(institution_uid, matching_scheme)`` for the first identifier that hits."""
        for ident in identifiers:
            row = conn.execute(
                "SELECT institution_uid FROM institution_identifiers WHERE scheme = ? AND value = ?",
                (ident.scheme, ident.value),
            ).fetchone()
            if row:
                return row["institution_uid"], ident.scheme
        return None

    def _find_merge_candidate(
        self,
        conn: sqlite3.Connection,
        request: RegistrationRequest,
    ) -> tuple[str, str, float] | None:
        """Run the non-identifier cascade against already-registered institutions.

        Tiers mirror the DEQAR in-memory cascade so confidence labels line up:
        website → exact_name → exact_name_and_website → website_and_fuzzy_name → fuzzy_name.
        Returns ``(institution_uid, match_type, confidence)`` for the strongest hit,
        or ``None`` if no tier clears its confidence bar.
        """
        country = request.country_code
        host = request.website_host
        normalized_inputs: list[str] = []
        for candidate in [request.canonical_name, *(v.variant for v in request.name_variants)]:
            normalized = normalize_name(candidate)
            if normalized and normalized not in normalized_inputs:
                normalized_inputs.append(normalized)

        # Tier: unique website host
        if host:
            host_rows = conn.execute(
                "SELECT institution_uid FROM institutions WHERE website_host = ? AND country_code = ? AND status = 'active'",
                (host, country),
            ).fetchall()
            if len(host_rows) == 1 and not normalized_inputs:
                return host_rows[0]["institution_uid"], "website", 0.99

        # Tier: exact normalized name (with optional website disambiguation)
        if normalized_inputs:
            placeholders = ",".join("?" for _ in normalized_inputs)
            exact_rows = conn.execute(
                f"""
                SELECT DISTINCT v.institution_uid, i.website_host
                FROM institution_name_variants v
                JOIN institutions i ON i.institution_uid = v.institution_uid
                WHERE v.normalized IN ({placeholders})
                  AND i.country_code = ?
                  AND i.status = 'active'
                """,
                (*normalized_inputs, country),
            ).fetchall()
            if len(exact_rows) == 1:
                return exact_rows[0]["institution_uid"], "exact_name", 0.96
            if len(exact_rows) > 1 and host:
                host_narrowed = [row for row in exact_rows if row["website_host"] == host]
                if len(host_narrowed) == 1:
                    return host_narrowed[0]["institution_uid"], "exact_name_and_website", 1.0

        # Tier: website host + fuzzy name across candidates sharing the host
        if host and normalized_inputs:
            host_candidates = conn.execute(
                "SELECT institution_uid FROM institutions WHERE website_host = ? AND country_code = ? AND status = 'active'",
                (host, country),
            ).fetchall()
            host_uids = [row["institution_uid"] for row in host_candidates]
            if host_uids:
                best = self._best_fuzzy_match(conn, normalized_inputs, host_uids)
                if best is not None:
                    uid, _score = best
                    return uid, "website_and_fuzzy_name", 0.9

        # Tier: global fuzzy (scoped to country)
        if normalized_inputs:
            country_uids = [
                row["institution_uid"]
                for row in conn.execute(
                    "SELECT institution_uid FROM institutions WHERE country_code = ? AND status = 'active'",
                    (country,),
                ).fetchall()
            ]
            if country_uids:
                best = self._best_fuzzy_match(conn, normalized_inputs, country_uids)
                if best is not None:
                    uid, _score = best
                    return uid, "fuzzy_name", 0.82

        return None

    @staticmethod
    def _best_fuzzy_match(
        conn: sqlite3.Connection,
        normalized_inputs: list[str],
        candidate_uids: list[str],
    ) -> tuple[str, float] | None:
        if not candidate_uids or not normalized_inputs:
            return None
        placeholders = ",".join("?" for _ in candidate_uids)
        variant_rows = conn.execute(
            f"""
            SELECT institution_uid, normalized
            FROM institution_name_variants
            WHERE institution_uid IN ({placeholders})
            """,
            candidate_uids,
        ).fetchall()

        best_uid: str | None = None
        best_score = 0.0
        for row in variant_rows:
            candidate_normalized = row["normalized"]
            for input_normalized in normalized_inputs:
                if input_normalized == candidate_normalized:
                    return row["institution_uid"], 1.0
                if (
                    input_normalized in candidate_normalized
                    or candidate_normalized in input_normalized
                ):
                    score = 0.95
                else:
                    score = SequenceMatcher(
                        a=input_normalized, b=candidate_normalized
                    ).ratio()
                if score > best_score:
                    best_score = score
                    best_uid = row["institution_uid"]

        if best_uid is not None and best_score >= FUZZY_NAME_THRESHOLD:
            return best_uid, best_score
        return None

    @staticmethod
    def _assert_identifier(
        conn: sqlite3.Connection,
        institution_uid: str,
        ident: IdentifierAssertion,
        now: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO institution_identifiers (
                scheme, value, institution_uid, source, confidence, asserted_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(scheme, value) DO UPDATE SET
                institution_uid = excluded.institution_uid,
                source = excluded.source,
                confidence = MAX(institution_identifiers.confidence, excluded.confidence),
                asserted_at = excluded.asserted_at
            """,
            (
                ident.scheme,
                ident.value,
                institution_uid,
                ident.source,
                ident.confidence,
                now,
            ),
        )

    @staticmethod
    def _assert_name_variant(
        conn: sqlite3.Connection,
        institution_uid: str,
        variant: NameVariant,
        source: str,
    ) -> None:
        normalized = normalize_name(variant.variant)
        if not normalized:
            return
        conn.execute(
            """
            INSERT INTO institution_name_variants (
                institution_uid, normalized, variant, language, source
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(institution_uid, normalized) DO UPDATE SET
                variant = excluded.variant,
                language = COALESCE(excluded.language, institution_name_variants.language),
                source = excluded.source
            """,
            (institution_uid, normalized, variant.variant, variant.language, source or variant.source),
        )

    @staticmethod
    def _identifiers_for(conn: sqlite3.Connection, institution_uid: str) -> dict[str, str]:
        rows = conn.execute(
            "SELECT scheme, value FROM institution_identifiers WHERE institution_uid = ?",
            (institution_uid,),
        ).fetchall()
        return {row["scheme"]: row["value"] for row in rows}

    @staticmethod
    def _log_match(
        conn: sqlite3.Connection,
        institution_uid: str | None,
        input_scheme: str,
        input_value: str,
        *,
        match_type: str,
        confidence: float,
    ) -> None:
        conn.execute(
            """
            INSERT INTO institution_match_log (
                institution_uid, input_scheme, input_value, match_type, confidence, resolved_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (institution_uid, input_scheme, input_value, match_type, confidence, _now_iso()),
        )


def seed_file_signature(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    return (str(path.resolve()), stat.st_mtime_ns, stat.st_size)


def load_eheso_seed_requests(
    seed_path: Path,
) -> tuple[list[RegistrationRequest], dict[str, int]]:
    requests: list[RegistrationRequest] = []
    stats = {
        "rows_seen": 0,
        "skipped_missing_name": 0,
        "skipped_missing_country": 0,
        "skipped_missing_identifiers": 0,
    }

    with seed_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stats["rows_seen"] += 1
            canonical_name = _first_nonempty(row, SEED_NAME_COLUMNS)
            if not canonical_name:
                stats["skipped_missing_name"] += 1
                continue

            country_code = _seed_country_code(row)
            if not country_code:
                stats["skipped_missing_country"] += 1
                continue

            identifiers = _seed_identifier_assertions(row)
            if not identifiers:
                stats["skipped_missing_identifiers"] += 1
                continue

            name_variants: list[NameVariant] = []
            seen_normalized_names: set[str] = set()
            _add_seed_name_variant(name_variants, seen_normalized_names, canonical_name)
            _add_seed_name_variant(
                name_variants,
                seen_normalized_names,
                _first_nonempty(row, SEED_OFFICIAL_NAME_COLUMNS),
            )
            for alias_value in _iter_seed_aliases(row):
                _add_seed_name_variant(name_variants, seen_normalized_names, alias_value)

            requests.append(
                RegistrationRequest(
                    canonical_name=canonical_name,
                    country_code=country_code,
                    identifiers=identifiers,
                    name_variants=name_variants,
                    website_host=_normalize_seed_host(_first_nonempty(row, SEED_WEBSITE_COLUMNS)),
                    institution_type=_clean_seed_profile_field(_first_nonempty(row, SEED_TYPE_COLUMNS)),
                    legal_status=_clean_seed_profile_field(_first_nonempty(row, SEED_LEGAL_STATUS_COLUMNS)),
                    source="eheso_eter_seed",
                )
            )

    return requests, stats


def _seed_identifier_assertions(row: dict[str, str | None]) -> list[IdentifierAssertion]:
    identifiers: list[IdentifierAssertion] = []
    seen: set[tuple[str, str]] = set()
    for scheme, columns in SEED_IDENTIFIER_COLUMNS:
        raw_value = _first_nonempty(row, columns)
        normalized_value = _normalize_seed_identifier(scheme, raw_value)
        if not normalized_value:
            continue
        key = (scheme, normalized_value)
        if key in seen:
            continue
        seen.add(key)
        identifiers.append(
            IdentifierAssertion(
                scheme=scheme,
                value=normalized_value,
                source="eheso_eter_seed",
                confidence=1.0 if scheme in {"eter", "ror", "openalex"} else 0.99,
            )
        )
    return identifiers


def _first_nonempty(row: dict[str, str | None], columns: tuple[str, ...]) -> str | None:
    for column in columns:
        value = (row.get(column) or "").strip()
        if value:
            return value
    return None


def _iter_seed_aliases(row: dict[str, str | None]) -> list[str]:
    aliases: list[str] = []
    for column in SEED_ALIAS_COLUMNS:
        raw_value = (row.get(column) or "").strip()
        if not raw_value:
            continue
        for alias in _SEED_SPLIT_RE.split(raw_value):
            cleaned = alias.strip()
            if cleaned:
                aliases.append(cleaned)
    return aliases


def _add_seed_name_variant(
    target: list[NameVariant],
    seen_normalized_names: set[str],
    value: str | None,
) -> None:
    candidate = (value or "").strip()
    if not candidate:
        return
    normalized = normalize_name(candidate)
    if not normalized or normalized in seen_normalized_names:
        return
    seen_normalized_names.add(normalized)
    target.append(NameVariant(variant=candidate, source="eheso_eter_seed"))


def _seed_country_code(row: dict[str, str | None]) -> str | None:
    raw_value = _first_nonempty(row, SEED_COUNTRY_COLUMNS)
    if raw_value:
        normalized = raw_value.strip().upper()
        if len(normalized) == 2 and normalized.isalpha():
            return normalized
        if len(normalized) == 3 and normalized in _ISO3_TO_ISO2:
            return _ISO3_TO_ISO2[normalized]

    eter_id = _normalize_seed_identifier("eter", _first_nonempty(row, ("eter_id", "eter", "eterID")))
    if eter_id and len(eter_id) >= 2 and eter_id[:2].isalpha():
        return eter_id[:2].upper()
    return None


def _normalize_seed_identifier(scheme: str, value: str | None) -> str | None:
    candidate = (value or "").strip()
    if not candidate:
        return None
    if scheme == "ror":
        return _normalize_seed_ror(candidate)
    if scheme == "openalex":
        return candidate.rstrip("/").split("/")[-1]
    return candidate


def _normalize_seed_ror(value: str) -> str | None:
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.upper().startswith("ROR:"):
        candidate = candidate.split(":", 1)[1].strip()
    if "ror.org/" in candidate.casefold():
        parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
        path_parts = [part for part in parsed.path.split("/") if part]
        candidate = path_parts[-1] if path_parts else candidate
    candidate = candidate.casefold().strip().strip("/")
    return candidate or None


def _normalize_seed_host(value: str | None) -> str | None:
    candidate = (value or "").strip()
    if not candidate:
        return None
    parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
    host = (parsed.netloc or parsed.path).casefold().strip("/")
    return host or None


def _clean_seed_profile_field(value: str | None) -> str | None:
    candidate = (value or "").strip()
    return candidate or None


_ISO3_TO_ISO2 = {
    "AUT": "AT",
    "BEL": "BE",
    "BGR": "BG",
    "CHE": "CH",
    "CYP": "CY",
    "CZE": "CZ",
    "DEU": "DE",
    "DNK": "DK",
    "ESP": "ES",
    "EST": "EE",
    "FIN": "FI",
    "FRA": "FR",
    "GBR": "GB",
    "GRC": "GR",
    "HRV": "HR",
    "HUN": "HU",
    "IRL": "IE",
    "ISL": "IS",
    "ITA": "IT",
    "LTU": "LT",
    "LUX": "LU",
    "LVA": "LV",
    "MLT": "MT",
    "NLD": "NL",
    "NOR": "NO",
    "POL": "PL",
    "PRT": "PT",
    "ROU": "RO",
    "SRB": "RS",
    "SVK": "SK",
    "SVN": "SI",
    "SWE": "SE",
    "TUR": "TR",
    "UKR": "UA",
    "XKX": "XK",
}
