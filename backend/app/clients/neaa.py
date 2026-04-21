from __future__ import annotations

import html
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from difflib import SequenceMatcher
from typing import Iterable
from urllib.parse import urljoin, urlparse

import httpx

from backend.app.config import get_settings
from backend.app.models.schemas import ExternalSourceStatus, InstitutionOption
from backend.app.services.institution_registry import InstitutionRecord, InstitutionRegistry, normalize_name


logger = logging.getLogger(__name__)

ENTRY_PATTERN = re.compile(
    r'<div class="vina-accordion-item"[^>]*>\s*<div class="title">(.*?)</div>.*?</div>\s*'
    r'<div class="vina-accordion-container">\s*<div class="content row-fluid">.*?<div class="introtext">(.*?)</div>\s*</div>\s*</div>',
    flags=re.IGNORECASE | re.DOTALL,
)
PARAGRAPH_PATTERN = re.compile(r"<p\b[^>]*>(.*?)</p>", flags=re.IGNORECASE | re.DOTALL)
LINK_PATTERN = re.compile(r'<a\b[^>]*href="([^"]+)"[^>]*>(.*?)</a>', flags=re.IGNORECASE | re.DOTALL)
TAG_PATTERN = re.compile(r"<[^>]+>")
WHITESPACE_PATTERN = re.compile(r"\s+")
LOCATION_SUFFIX_PATTERN = re.compile(r"\s+[–-]\s+[A-Za-z][A-Za-z\s.'\"()/-]+$")
DATE_TOKEN_PATTERN = re.compile(
    r"\b\d{2}/\d{2}/\d{4}\b|\b\d{1,2}\s+[A-Za-z]+\s+\d{4}\b|\b\d{2}\.\d{2}\.\d{4}\b"
)
RATING_NUMBER_PATTERN = re.compile(r"(\d+[.,]\d+|\d+)")
TITLE_MATCH_THRESHOLD = 0.88
TITLE_MARGIN_THRESHOLD = 0.03


@dataclass(slots=True)
class NeaaInstitutionRecord:
    neaa_id: str
    title: str
    decision_text: str | None
    decision_date: date | None
    decision_date_text: str | None
    valid_to: date | None
    valid_to_text: str | None
    rating_value: float | None
    rating_text: str | None
    capacity_text: str | None
    full_report_url: str | None
    annotation_ia_url: str | None
    annotation_pamc_url: str | None
    annotation_dl_url: str | None
    previous_accreditation_url: str | None
    notes: list[str] = field(default_factory=list)
    normalized_title: str = ""


@dataclass(slots=True)
class NeaaDataset:
    records: list[NeaaInstitutionRecord]
    fetched_at: str
    source_url: str


@dataclass(slots=True)
class NeaaInstitutionMatch:
    record: NeaaInstitutionRecord
    match_type: str
    matched_name: str
    confidence: str
    score: float
    note: str


class NeaaClient:
    """Live NEAA overlay for Bulgarian higher-education institutions."""

    def __init__(self, registry: InstitutionRegistry | None = None) -> None:
        self.settings = get_settings()
        self.base_url = self.settings.neaa_base_url.rstrip("/")
        self.source_url = urljoin(f"{self.base_url}/", self.settings.neaa_higher_institutions_path.lstrip("/"))
        self.registry = registry or InstitutionRegistry()
        self._dataset_cache: tuple[float, NeaaDataset] | None = None

    async def build_institution_status(
        self,
        institution: InstitutionOption,
        *,
        extra_names: Iterable[str] | None = None,
    ) -> ExternalSourceStatus:
        if (institution.country_code or "").upper() != "BG":
            return ExternalSourceStatus(
                source="neaa",
                status="unavailable",
                message="NEAA local context is only applicable to Bulgarian higher-education institutions.",
                institution_id=institution.id,
                metadata={
                    "applicable": False,
                    "source_url": self.source_url,
                },
            )

        try:
            dataset = await self._load_dataset()
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.warning("NEAA dataset fetch failed for %s: %s", institution.id, exc)
            return ExternalSourceStatus(
                source="neaa",
                status="unavailable",
                message="The NEAA institutional-accreditation page could not be loaded right now.",
                institution_id=institution.id,
                metadata={
                    "applicable": True,
                    "source_url": self.source_url,
                    "error": str(exc),
                    "next_step": "Retry later or review the NEAA site directly for Bulgarian institutional accreditation details.",
                },
            )

        match = self._match_institution(dataset, institution, extra_names=extra_names)
        if not match:
            return ExternalSourceStatus(
                source="neaa",
                status="ready",
                message="No confident NEAA institutional-accreditation match was found for this Bulgarian university.",
                institution_id=institution.id,
                metadata={
                    "applicable": True,
                    "source_url": dataset.source_url,
                    "dataset_updated_at": dataset.fetched_at,
                    "match_confidence": "low",
                    "next_step": "Review the NEAA list directly if you need Bulgaria-specific accreditation timing for this institution.",
                },
            )

        record = match.record
        message = record.decision_text or "NEAA lists institutional accreditation context for this university."
        if record.decision_date:
            message += f" Decision date: {record.decision_date.isoformat()}."
        if record.valid_to_text:
            message += f" Validity: {record.valid_to_text}."
        elif record.valid_to:
            message += f" Valid until: {record.valid_to.isoformat()}."

        return ExternalSourceStatus(
            source="neaa",
            status="active",
            message=message,
            institution_id=institution.id,
            metadata={
                "applicable": True,
                "source_url": dataset.source_url,
                "dataset_updated_at": dataset.fetched_at,
                "matched_institution_name": record.title,
                "match_type": match.match_type,
                "match_value": match.matched_name,
                "match_confidence": match.confidence,
                "match_score": round(match.score, 3),
                "match_note": match.note,
                "current_status": record.decision_text,
                "decision_date": record.decision_date.isoformat() if record.decision_date else None,
                "decision_date_text": record.decision_date_text,
                "valid_to": record.valid_to.isoformat() if record.valid_to else None,
                "valid_to_text": record.valid_to_text,
                "rating_value": record.rating_value,
                "rating_text": record.rating_text,
                "capacity_text": record.capacity_text,
                "full_report_url": record.full_report_url,
                "annotation_ia_url": record.annotation_ia_url,
                "annotation_pamc_url": record.annotation_pamc_url,
                "annotation_dl_url": record.annotation_dl_url,
                "previous_accreditation_url": record.previous_accreditation_url,
                "notes": record.notes,
                "neaa_id": record.neaa_id,
            },
        )

    async def _load_dataset(self) -> NeaaDataset:
        now = time.time()
        cached = self._dataset_cache
        ttl_seconds = max(int(self.settings.neaa_cache_ttl_hours), 1) * 60 * 60
        if cached and (now - cached[0]) < ttl_seconds:
            return cached[1]

        logger.info("NEAA GET %s", self.source_url)
        async with httpx.AsyncClient(timeout=self.settings.neaa_timeout_seconds, follow_redirects=True) as client:
            response = await client.get(self.source_url)
            response.raise_for_status()
            payload = response.text

        dataset = NeaaDataset(
            records=parse_neaa_dataset(payload, self.base_url),
            fetched_at=datetime.now(UTC).isoformat(),
            source_url=self.source_url,
        )
        self._dataset_cache = (now, dataset)
        return dataset

    def _match_institution(
        self,
        dataset: NeaaDataset,
        institution: InstitutionOption,
        *,
        extra_names: Iterable[str] | None = None,
    ) -> NeaaInstitutionMatch | None:
        candidate_names = candidate_institution_names(
            institution,
            registry_record=self._resolve_registry_record(institution.id),
            extra_names=extra_names,
        )
        if not candidate_names:
            return None

        exact_names = {normalize_name(name): name for name in candidate_names if normalize_name(name)}
        for record in dataset.records:
            record_names = [record.normalized_title, normalize_name(strip_location_suffix(record.title))]
            matched_name = next((exact_names.get(record_name) for record_name in record_names if record_name in exact_names), None)
            if matched_name:
                return NeaaInstitutionMatch(
                    record=record,
                    match_type="exact_name",
                    matched_name=matched_name,
                    confidence="high",
                    score=1.0,
                    note="Matched on the English institution title listed on the NEAA page.",
                )

        scored: list[tuple[float, str, NeaaInstitutionRecord]] = []
        for record in dataset.records:
            best_local_score = 0.0
            best_local_name = ""
            record_variants = [record.normalized_title, normalize_name(strip_location_suffix(record.title))]
            for candidate_name in candidate_names:
                normalized_candidate = normalize_name(candidate_name)
                if not normalized_candidate:
                    continue
                score = max(
                    SequenceMatcher(a=normalized_candidate, b=record_variant).ratio()
                    for record_variant in record_variants
                    if record_variant
                )
                if score > best_local_score:
                    best_local_score = score
                    best_local_name = candidate_name
            if best_local_score > 0:
                scored.append((best_local_score, best_local_name, record))

        if not scored:
            return None

        scored.sort(key=lambda item: item[0], reverse=True)
        best_score, best_name, best_record = scored[0]
        second_score = scored[1][0] if len(scored) > 1 else 0.0
        if best_score < TITLE_MATCH_THRESHOLD or (best_score - second_score) < TITLE_MARGIN_THRESHOLD:
            return None

        confidence = "medium" if best_score >= 0.93 else "low"
        note = (
            "Matched on a close institution-name variant against the NEAA institutional list."
            if confidence == "medium"
            else "Matched on a weaker institution-name similarity and should be reviewed carefully."
        )
        return NeaaInstitutionMatch(
            record=best_record,
            match_type="fuzzy_name",
            matched_name=best_name,
            confidence=confidence,
            score=best_score,
            note=note,
        )

    def _resolve_registry_record(self, institution_id: str) -> InstitutionRecord | None:
        try:
            return self.registry.resolve("openalex", institution_id, log_lookup=False)
        except Exception:  # noqa: BLE001
            logger.exception("Registry profile lookup failed for NEAA match openalex=%s", institution_id)
            return None


def parse_neaa_dataset(payload: str, base_url: str) -> list[NeaaInstitutionRecord]:
    records: list[NeaaInstitutionRecord] = []
    for title_html, intro_html in ENTRY_PATTERN.findall(payload):
        title = html_to_text(title_html)
        if not title:
            continue
        record = parse_neaa_entry(title, intro_html, base_url)
        if record:
            records.append(record)
    return records


def parse_neaa_entry(title: str, intro_html: str, base_url: str) -> NeaaInstitutionRecord | None:
    paragraphs_html = PARAGRAPH_PATTERN.findall(intro_html)
    if not paragraphs_html:
        return None

    decision_text = None
    decision_date = None
    decision_date_text = None
    valid_to = None
    valid_to_text = None
    rating_value = None
    rating_text = None
    capacity_text = None
    full_report_url = None
    annotation_ia_url = None
    annotation_pamc_url = None
    annotation_dl_url = None
    previous_accreditation_url = None
    notes: list[str] = []

    for paragraph_html in paragraphs_html:
        paragraph_text = html_to_text(paragraph_html)
        links = paragraph_links(paragraph_html, base_url)
        normalized = paragraph_text.casefold()
        if not paragraph_text:
            continue

        if normalized.startswith("institutional accreditation:"):
            decision_text = extract_after_colon(paragraph_text)
            parsed_date = first_date_from_text(paragraph_text)
            if parsed_date:
                decision_date = parsed_date
                decision_date_text = parsed_date.isoformat()
        elif normalized.startswith("the decision was taken on:"):
            decision_date_text = extract_after_colon(paragraph_text)
            decision_date = first_date_from_text(paragraph_text)
        elif normalized.startswith("valid until:") or normalized.startswith("validity until:"):
            valid_to_text = extract_after_colon(paragraph_text)
            valid_to = first_date_from_text(paragraph_text)
        elif normalized.startswith("rating:") or normalized.startswith("assessment:"):
            rating_text = extract_after_colon(paragraph_text)
            rating_value = parse_rating_value(rating_text)
        elif normalized.startswith("capacity of the higher school:") or normalized.startswith("capacity:"):
            capacity_text = extract_after_colon(paragraph_text)
        elif "full report on completed institutional accreditation procedure" in normalized and links:
            full_report_url = links[0][0]
        elif "annotation on ia" in normalized and links:
            annotation_ia_url = links[-1][0]
        elif "annotation pamc" in normalized and links:
            annotation_pamc_url = links[-1][0]
        elif ("annotation on dl" in normalized or "annotation on dle" in normalized) and links:
            annotation_dl_url = links[-1][0]
        elif "previous institutional accreditation" in normalized and links:
            previous_accreditation_url = links[-1][0]
        elif paragraph_text.startswith("*") or "deadline" in normalized or "within 12 months" in normalized:
            notes.append(paragraph_text)

    neaa_id = slug_or_path_id(full_report_url) or slug_or_path_id(previous_accreditation_url) or normalize_name(title)
    return NeaaInstitutionRecord(
        neaa_id=neaa_id,
        title=title,
        decision_text=decision_text,
        decision_date=decision_date,
        decision_date_text=decision_date_text,
        valid_to=valid_to,
        valid_to_text=valid_to_text,
        rating_value=rating_value,
        rating_text=rating_text,
        capacity_text=capacity_text,
        full_report_url=full_report_url,
        annotation_ia_url=annotation_ia_url,
        annotation_pamc_url=annotation_pamc_url,
        annotation_dl_url=annotation_dl_url,
        previous_accreditation_url=previous_accreditation_url,
        notes=notes,
        normalized_title=normalize_name(title),
    )


def candidate_institution_names(
    institution: InstitutionOption,
    *,
    registry_record: InstitutionRecord | None,
    extra_names: Iterable[str] | None = None,
) -> list[str]:
    seen: set[str] = set()
    names: list[str] = []
    raw_candidates = [
        institution.display_name,
        *(institution.aliases or []),
        registry_record.canonical_name if registry_record else None,
        *(extra_names or []),
    ]
    expanded_candidates: list[str | None] = []
    for candidate in raw_candidates:
        expanded_candidates.append(candidate)
        stripped_candidate = strip_location_suffix(str(candidate or "").strip())
        if stripped_candidate and stripped_candidate != str(candidate or "").strip():
            expanded_candidates.append(stripped_candidate)

    for candidate in expanded_candidates:
        text = str(candidate or "").strip()
        normalized = normalize_name(text) if text else ""
        if not text or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        names.append(text)
    return names


def paragraph_links(fragment: str, base_url: str) -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    for href, label_html in LINK_PATTERN.findall(fragment):
        label = html_to_text(label_html)
        resolved = urljoin(f"{base_url}/", html.unescape(href).strip())
        links.append((resolved, label))
    return links


def html_to_text(fragment: str) -> str:
    with_breaks = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.IGNORECASE)
    stripped = TAG_PATTERN.sub(" ", with_breaks)
    unescaped = html.unescape(stripped).replace("\xa0", " ")
    return WHITESPACE_PATTERN.sub(" ", unescaped).strip()


def extract_after_colon(value: str) -> str:
    _, _, tail = value.partition(":")
    return tail.strip() or value.strip()


def first_date_from_text(value: str) -> date | None:
    match = DATE_TOKEN_PATTERN.search(value)
    if not match:
        return None
    token = match.group(0)
    for fmt in ("%m/%d/%Y", "%d %B %Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(token, fmt).date()
        except ValueError:
            continue
    return None


def parse_rating_value(value: str | None) -> float | None:
    candidate = str(value or "").strip()
    if not candidate:
        return None
    match = RATING_NUMBER_PATTERN.search(candidate)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", "."))
    except ValueError:
        return None


def slug_or_path_id(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlparse(value)
    path = parsed.path.strip("/")
    if not path:
        return None
    return path.split("/")[-1]


def strip_location_suffix(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        return ""
    return LOCATION_SUFFIX_PATTERN.sub("", candidate).strip()
