from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date
import logging
import math

import httpx

from backend.app.clients.deqar import DeqarClient
from backend.app.clients.neaa import NeaaClient
from backend.app.clients.openalex import OpenAlexClient
from backend.app.models.schemas import ExternalSourceStatus, InstitutionOption, QualityInstitutionResponse, QualityInstitutionStatus
from backend.app.services.institution_registry import InstitutionRecord, InstitutionRegistry


logger = logging.getLogger(__name__)

DEFAULT_PEER_MODE = "regional"
TARGET_DYNAMIC_PEER_COUNT = 8
PEER_CANDIDATE_POOL_SIZE = 24
EUROPE_FILTER = "continent:europe"
PEER_MODE_LABELS = {
    "country": "Same-country dynamic cohort",
    "regional": "European similar cohort",
    "global": "Global similar cohort",
}
PEER_MODE_DESCRIPTIONS = {
    "country": "Starts with similar-size higher-education institutions in the same country, then broadens only if the local pool is too thin.",
    "regional": "Builds a similar-size higher-education cohort across Europe, then broadens globally only if needed.",
    "global": "Builds a similar-size higher-education cohort globally without country constraints.",
}
STRONG_DEQAR_MATCH_TYPES = frozenset({"deqar_id", "eter_id", "ror"})
DETERMINISTIC_DEQAR_MATCH_TYPES = frozenset({"website", "exact_name", "exact_name_and_website", "registry_crosswalk"})
FUZZY_DEQAR_MATCH_TYPES = frozenset({"website_and_fuzzy_name", "fuzzy_name"})


@dataclass(slots=True)
class RankedPeerCandidate:
    institution: InstitutionOption
    registry_record: InstitutionRecord | None
    deqar_summary: dict[str, object]
    similarity_score: float
    selection_score: float
    selection_note: str


class QualityService:
    """Coordinates institution-level quality and benchmarking source contracts."""

    def __init__(self) -> None:
        self.registry = InstitutionRegistry()
        self.deqar_client = DeqarClient(registry=self.registry)
        self.neaa_client = NeaaClient(registry=self.registry)
        self.openalex_client = OpenAlexClient(registry=self.registry)

    async def get_institution_quality(self, institution_id: str, peer_mode: str = DEFAULT_PEER_MODE) -> QualityInstitutionResponse:
        normalized_peer_mode = normalize_peer_mode(peer_mode)
        institution = await self._get_institution_option(institution_id)
        neaa: ExternalSourceStatus | None = None

        if not self.deqar_client.is_configured():
            neaa = await self.neaa_client.build_institution_status(institution)
            deqar = QualityInstitutionStatus(
                source="deqar",
                institution_id=institution_id,
                status="unavailable",
                summary="Downloaded DEQAR CSV datasets are not configured yet, so institutional QA status and reports cannot be loaded.",
                metadata={
                    "institution_name": institution.display_name,
                    "dataset_source": "csv",
                    "missing_dataset_paths": self.deqar_client.missing_dataset_paths(),
                    "next_step": "Download the DEQAR institutions and reports CSV exports, then point the backend at those files.",
                },
            )
            benchmarking = ExternalSourceStatus(
                source="benchmarking",
                status="unavailable",
                message="Peer benchmarking readiness is blocked until the DEQAR CSV snapshot is available.",
                institution_id=institution_id,
                metadata={
                    "peer_mode": normalized_peer_mode,
                    "peer_group_label": peer_group_label(normalized_peer_mode),
                    "peer_group_description": PEER_MODE_DESCRIPTIONS[normalized_peer_mode],
                    "next_step": "Load the DEQAR institutions and reports CSV files first, then benchmark readiness can be assessed.",
                },
            )
        else:
            try:
                deqar = self.deqar_client.build_quality_status(institution)
                benchmarking, neaa = await asyncio.gather(
                    self._build_benchmarking_status(institution, deqar, normalized_peer_mode),
                    self.neaa_client.build_institution_status(
                        institution,
                        extra_names=[
                            str(deqar.metadata.get("matched_institution_name") or ""),
                            str(deqar.metadata.get("registry_canonical_name") or ""),
                        ],
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("DEQAR dataset parsing failed for %s: %s", institution_id, exc)
                deqar = QualityInstitutionStatus(
                    source="deqar",
                    institution_id=institution_id,
                    status="unavailable",
                    summary="The local DEQAR dataset snapshot could not be parsed right now.",
                    metadata={
                        "institution_name": institution.display_name,
                        "dataset_source": "csv",
                        "next_step": "Check the downloaded DEQAR CSV files and retry after replacing any partial or malformed snapshot.",
                    },
                )
                benchmarking = ExternalSourceStatus(
                    source="benchmarking",
                    status="unavailable",
                    message="Peer benchmarking readiness could not be assessed because the local DEQAR snapshot failed to parse.",
                    institution_id=institution_id,
                    metadata={
                        "peer_mode": normalized_peer_mode,
                        "peer_group_label": peer_group_label(normalized_peer_mode),
                        "peer_group_description": PEER_MODE_DESCRIPTIONS[normalized_peer_mode],
                        "next_step": "Fix the DEQAR CSV snapshot first, then retry the quality page.",
                    },
                )
                neaa = await self.neaa_client.build_institution_status(institution)

        neaa = merge_neaa_comparison(neaa or await self.neaa_client.build_institution_status(institution), deqar)
        return QualityInstitutionResponse(
            institution_id=institution_id,
            deqar=deqar,
            neaa=neaa,
            benchmarking=benchmarking,
            metadata={
                "report_count": int(deqar.metadata.get("report_count") or len(deqar.reports)),
                "has_live_quality_data": deqar.status == "active" or neaa.status == "active",
            },
        )

    async def _build_benchmarking_status(
        self,
        institution: InstitutionOption,
        deqar: QualityInstitutionStatus,
        peer_mode: str,
    ) -> ExternalSourceStatus:
        peer_group = peer_group_label(peer_mode)
        if deqar.status != "active":
            return ExternalSourceStatus(
                source="benchmarking",
                status="ready",
                message="Peer benchmarking needs a DEQAR-matched primary institution before QA-context comparison can start.",
                institution_id=institution.id,
                metadata={
                    "next_step": deqar.metadata.get("next_step") or "Match the selected institution in DEQAR first.",
                    "peers": [],
                    "peer_count": 0,
                    "ready_peer_count": 0,
                    "matched_peer_count": 0,
                    "institutional_peer_count": 0,
                    "peer_mode": peer_mode,
                    "peer_group_label": peer_group,
                    "peer_group_description": PEER_MODE_DESCRIPTIONS[peer_mode],
                },
            )

        ranked_candidates, scope_trace = await self._select_dynamic_peer_candidates(institution, peer_mode)

        peer_summaries = []
        for candidate in ranked_candidates:
            summary = dict(candidate.deqar_summary)
            summary["is_primary"] = candidate.institution.id == institution.id
            summary["peer_similarity_score"] = candidate.similarity_score
            summary["peer_selection_score"] = candidate.selection_score
            summary["peer_selection_note"] = candidate.selection_note
            summary.update(registry_profile_metadata(candidate.registry_record))
            peer_summaries.append(summary)

        peer_summaries.sort(
            key=lambda item: (
                not bool(item.get("is_primary")),
                float(item.get("peer_selection_score") or 0.0),
                readiness_rank(str(item.get("readiness") or "")),
                -int(item.get("report_count") or 0),
                str(item.get("display_name") or ""),
            )
        )

        peer_count = len(peer_summaries)
        matched_peer_count = sum(1 for peer in peer_summaries if peer.get("deqar_status") == "active")
        ready_peer_count = sum(1 for peer in peer_summaries if peer.get("readiness") == "ready")
        partial_peer_count = sum(1 for peer in peer_summaries if peer.get("readiness") == "partial")
        institutional_peer_count = sum(1 for peer in peer_summaries if int(peer.get("institutional_report_count") or 0) > 0)
        expiring_12m_peer_count = sum(
            1 for peer in peer_summaries if peer.get("institutional_validity_status") == "expires_within_12_months"
        )
        expiring_24m_peer_count = sum(
            1
            for peer in peer_summaries
            if peer.get("institutional_validity_status") in {"expires_within_12_months", "expires_within_24_months"}
        )
        high_risk_peer_count = sum(1 for peer in peer_summaries if peer.get("qa_risk_level") == "high")
        open_ended_peer_count = sum(1 for peer in peer_summaries if peer.get("institutional_validity_status") == "active_open_ended")
        strong_crosswalk_peer_count = sum(
            1 for peer in peer_summaries if str(peer.get("match_type") or "") in STRONG_DEQAR_MATCH_TYPES
        )
        fuzzy_crosswalk_peer_count = sum(
            1 for peer in peer_summaries if str(peer.get("match_type") or "") in FUZZY_DEQAR_MATCH_TYPES
        )
        primary_peer = next((peer for peer in peer_summaries if peer.get("is_primary")), None)

        if primary_peer and primary_peer.get("readiness") == "ready" and ready_peer_count >= 2:
            status = "active"
            message = (
                f"DEQAR currently supports QA-context peer comparison for {ready_peer_count} of {peer_count} institutions "
                f"in the {peer_group.lower()}."
            )
        elif matched_peer_count >= 2:
            status = "ready"
            message = (
                f"DEQAR coverage exists for {matched_peer_count} of {peer_count} institutions, but some peers still lack "
                f"institutional-level review coverage inside the {peer_group.lower()}."
            )
        else:
            status = "ready"
            message = f"DEQAR has only limited coverage in the current {peer_group.lower()}, so benchmarking readiness remains partial."

        return ExternalSourceStatus(
            source="benchmarking",
            status=status,
            message=message,
            institution_id=institution.id,
            metadata={
                "peer_mode": peer_mode,
                "peer_group_label": peer_group,
                "peer_group_description": describe_scope_trace(peer_mode, scope_trace),
                "primary_institution_name": institution.display_name,
                "peer_count": peer_count,
                "matched_peer_count": matched_peer_count,
                "ready_peer_count": ready_peer_count,
                "partial_peer_count": partial_peer_count,
                "institutional_peer_count": institutional_peer_count,
                "expiring_12m_peer_count": expiring_12m_peer_count,
                "expiring_24m_peer_count": expiring_24m_peer_count,
                "high_risk_peer_count": high_risk_peer_count,
                "open_ended_peer_count": open_ended_peer_count,
                "strong_crosswalk_peer_count": strong_crosswalk_peer_count,
                "fuzzy_crosswalk_peer_count": fuzzy_crosswalk_peer_count,
                "peers": peer_summaries,
                "scope_trace": scope_trace,
                "peer_selection_note": (
                    "Peers are ranked by size and research proximity first, then adjusted using institution-registry profile fit "
                    "and DEQAR match strength. ETER or ROR-linked matches are preferred, while fuzzy-only matches are down-ranked."
                ),
                "next_step": "Use this identity-weighted cohort to frame QA context now, then deepen it later with richer EHESO benchmarking descriptors.",
            },
        )

    async def _select_dynamic_peer_candidates(
        self,
        institution: InstitutionOption,
        peer_mode: str,
    ) -> tuple[list[RankedPeerCandidate], list[str]]:
        target_peer_count = TARGET_DYNAMIC_PEER_COUNT
        candidate_pool: dict[str, InstitutionOption] = {
            institution.id: institution,
        }
        scope_trace: list[str] = []

        for scope_name in candidate_scope_order(peer_mode, institution.country_code):
            candidates = await self._load_peer_candidates_for_scope(institution, scope_name)
            if not candidates:
                continue
            scope_trace.append(scope_name)
            for candidate in candidates:
                if candidate.id == institution.id or candidate.id in candidate_pool:
                    continue
                candidate_pool[candidate.id] = candidate
            if len(candidate_pool) >= target_peer_count:
                break

        primary_record = self._resolve_registry_record(institution.id)
        primary_candidate = self._rank_peer_candidate(
            primary=institution,
            primary_record=primary_record,
            candidate=institution,
            is_primary=True,
        )

        if len(candidate_pool) == 1:
            return [primary_candidate], scope_trace

        ranked_candidates = sorted(
            [
                self._rank_peer_candidate(
                    primary=institution,
                    primary_record=primary_record,
                    candidate=candidate,
                )
                for candidate in candidate_pool.values()
                if candidate.id != institution.id
            ],
            key=lambda candidate: (
                candidate.selection_score,
                readiness_rank(str(candidate.deqar_summary.get("readiness") or "")),
                match_priority(str(candidate.deqar_summary.get("match_type") or "")),
                -int(candidate.deqar_summary.get("report_count") or 0),
                -int(candidate.institution.cited_by_count or 0),
                str(candidate.institution.display_name or ""),
            ),
        )

        selected = [primary_candidate, *ranked_candidates[: target_peer_count - 1]]
        return selected, scope_trace

    async def _load_peer_candidates_for_scope(
        self,
        institution: InstitutionOption,
        scope_name: str,
    ) -> list[InstitutionOption]:
        scoped_parts: list[str] = []
        if scope_name == "country":
            if not institution.country_code:
                return []
            scoped_parts.append(f"country_code:{institution.country_code}")
        elif scope_name == "regional":
            scoped_parts.append(EUROPE_FILTER)

        size_parts = works_count_band_filters(institution.works_count)
        filter_expressions = [
            ",".join(["type:education", *scoped_parts, *size_parts]) if size_parts else ",".join(["type:education", *scoped_parts]),
            ",".join(["type:education", *scoped_parts]),
            ",".join([*scoped_parts, *size_parts]) if size_parts else ",".join(scoped_parts),
            ",".join(scoped_parts),
        ]

        for filter_expression in dict.fromkeys(expression for expression in filter_expressions if expression):
            try:
                return await self.openalex_client.list_institutions(
                    filter_expression=filter_expression,
                    sort="works_count:desc",
                    per_page=PEER_CANDIDATE_POOL_SIZE,
                )
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                logger.warning(
                    "Peer candidate lookup failed for %s scope=%s filter=%s: %s",
                    institution.id,
                    scope_name,
                    filter_expression,
                    exc,
                )
        return []

    async def _get_institution_option(self, institution_id: str) -> InstitutionOption:
        try:
            return await self.openalex_client.get_institution_option(institution_id)
        except (httpx.HTTPStatusError, httpx.RequestError):
            return InstitutionOption(id=institution_id, display_name=institution_id)

    def _rank_peer_candidate(
        self,
        *,
        primary: InstitutionOption,
        primary_record: InstitutionRecord | None,
        candidate: InstitutionOption,
        is_primary: bool = False,
    ) -> RankedPeerCandidate:
        registry_record = self._resolve_registry_record(candidate.id)
        deqar_summary = self.deqar_client.build_benchmark_peer_summary(candidate)
        if is_primary:
            return RankedPeerCandidate(
                institution=candidate,
                registry_record=registry_record,
                deqar_summary=deqar_summary,
                similarity_score=0.0,
                selection_score=-1.0,
                selection_note="Selection anchor for the cohort.",
            )

        similarity_score = peer_similarity_score(primary, candidate)
        selection_score = similarity_score
        reasons = ["size/research similar"]

        if primary.country_code and candidate.country_code and primary.country_code == candidate.country_code:
            reasons.append("same country")

        if (
            primary_record
            and registry_record
            and primary_record.institution_type
            and registry_record.institution_type
            and primary_record.institution_type == registry_record.institution_type
        ):
            selection_score -= 0.14
            reasons.append(f"same type ({registry_record.institution_type})")
        elif primary_record and registry_record and primary_record.institution_type and registry_record.institution_type:
            selection_score += 0.04

        if (
            primary_record
            and registry_record
            and primary_record.legal_status
            and registry_record.legal_status
            and primary_record.legal_status == registry_record.legal_status
        ):
            selection_score -= 0.09
            reasons.append(f"same legal status ({registry_record.legal_status})")
        elif primary_record and registry_record and primary_record.legal_status and registry_record.legal_status:
            selection_score += 0.03

        match_type = str(deqar_summary.get("match_type") or "")
        match_label = str(deqar_summary.get("match_provenance_label") or "")
        readiness = str(deqar_summary.get("readiness") or "")

        if match_type in STRONG_DEQAR_MATCH_TYPES:
            selection_score -= 0.24
            reasons.append(match_label or "strong DEQAR link")
        elif match_type in DETERMINISTIC_DEQAR_MATCH_TYPES:
            selection_score -= 0.12
            reasons.append(match_label or "deterministic DEQAR link")
        elif match_type in FUZZY_DEQAR_MATCH_TYPES:
            selection_score += 0.22
            reasons.append("fuzzy-only DEQAR link")
        elif deqar_summary.get("deqar_status") == "ready":
            selection_score += 0.18
            reasons.append("no DEQAR reports yet")
        else:
            selection_score += 0.24
            reasons.append("no DEQAR link yet")

        if readiness == "ready":
            selection_score -= 0.08
            reasons.append("institutional QA coverage")
        elif readiness == "partial":
            selection_score += 0.04
        elif readiness == "limited":
            selection_score += 0.08

        return RankedPeerCandidate(
            institution=candidate,
            registry_record=registry_record,
            deqar_summary=deqar_summary,
            similarity_score=similarity_score,
            selection_score=selection_score,
            selection_note=build_peer_selection_note(reasons),
        )

    def _resolve_registry_record(self, institution_id: str) -> InstitutionRecord | None:
        try:
            return self.registry.resolve("openalex", institution_id, log_lookup=False)
        except Exception:  # noqa: BLE001
            logger.exception("Registry profile lookup failed for openalex=%s", institution_id)
            return None


def readiness_rank(value: str) -> int:
    order = {"ready": 0, "partial": 1, "limited": 2}
    return order.get(value, 3)


def match_priority(match_type: str) -> int:
    if match_type in STRONG_DEQAR_MATCH_TYPES:
        return 0
    if match_type in DETERMINISTIC_DEQAR_MATCH_TYPES:
        return 1
    if match_type in FUZZY_DEQAR_MATCH_TYPES:
        return 2
    return 3


def normalize_peer_mode(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if normalized in PEER_MODE_LABELS:
        return normalized
    return DEFAULT_PEER_MODE


def peer_group_label(peer_mode: str) -> str:
    return PEER_MODE_LABELS.get(peer_mode, PEER_MODE_LABELS[DEFAULT_PEER_MODE])


def candidate_scope_order(peer_mode: str, country_code: str | None) -> list[str]:
    if peer_mode == "country":
        if country_code:
            return ["country", "regional", "global"]
        return ["regional", "global"]
    if peer_mode == "regional":
        return ["regional", "global"]
    return ["global"]


def describe_scope_trace(peer_mode: str, scope_trace: list[str]) -> str:
    if not scope_trace:
        return PEER_MODE_DESCRIPTIONS[peer_mode]

    scope_labels = {
        "country": "same-country",
        "regional": "European",
        "global": "global",
    }
    if len(scope_trace) == 1:
        return {
            "country": "Similar-size higher-education institutions from the same country.",
            "regional": "Similar-size higher-education institutions from the European pool.",
            "global": "Similar-size higher-education institutions from the global pool.",
        }[scope_trace[0]]

    path = " -> ".join(scope_labels.get(scope, scope) for scope in scope_trace)
    return f"Started with {scope_labels.get(scope_trace[0], scope_trace[0])} candidates, then broadened through {path} to fill the cohort."


def works_count_band_filters(works_count: int | None) -> list[str]:
    if not works_count or works_count <= 0:
        return []

    lower = max(int(works_count / 3), 1)
    upper = max(int(works_count * 3), lower + 1)
    return [
        f"works_count:>{lower}",
        f"works_count:<{upper}",
    ]


def peer_similarity_score(primary: InstitutionOption, candidate: InstitutionOption) -> float:
    works_gap = log_gap(primary.works_count, candidate.works_count)
    citations_gap = log_gap(primary.cited_by_count, candidate.cited_by_count)
    country_bonus = -0.18 if primary.country_code and primary.country_code == candidate.country_code else 0.0
    return (works_gap * 0.65) + (citations_gap * 0.35) + country_bonus


def log_gap(primary_value: int | None, candidate_value: int | None) -> float:
    if primary_value and candidate_value and primary_value > 0 and candidate_value > 0:
        return abs(math.log10(candidate_value) - math.log10(primary_value))
    if primary_value == candidate_value:
        return 0.0
    return 1.0


def build_peer_selection_note(reasons: list[str]) -> str:
    normalized = [reason for reason in reasons if reason]
    if not normalized:
        return "Selected from the current candidate pool."
    lead = normalized[:4]
    return "Why selected: " + "; ".join(lead) + "."


def registry_profile_metadata(record: InstitutionRecord | None) -> dict[str, object]:
    if not record:
        return {
            "registry_institution_uid": None,
            "registry_canonical_name": None,
            "registry_country_code": None,
            "registry_website_host": None,
            "registry_eter_id": None,
            "registry_ror": None,
            "registry_openalex_id": None,
            "registry_institution_type": None,
            "registry_legal_status": None,
        }
    return {
        "registry_institution_uid": record.institution_uid,
        "registry_canonical_name": record.canonical_name,
        "registry_country_code": record.country_code,
        "registry_website_host": record.website_host,
        "registry_eter_id": record.eter_id,
        "registry_ror": record.identifiers.get("ror"),
        "registry_openalex_id": record.identifiers.get("openalex"),
        "registry_institution_type": record.institution_type,
        "registry_legal_status": record.legal_status,
    }


def merge_neaa_comparison(neaa: ExternalSourceStatus, deqar: QualityInstitutionStatus) -> ExternalSourceStatus:
    metadata = dict(neaa.metadata or {})
    metadata["applicable"] = bool(metadata.get("applicable"))
    if not metadata["applicable"] or neaa.status != "active":
        return ExternalSourceStatus(
            source=neaa.source,
            status=neaa.status,
            message=neaa.message,
            institution_id=neaa.institution_id,
            metadata=metadata,
        )

    comparison_summary, comparison_tone = compare_neaa_to_deqar(metadata, deqar)
    if comparison_summary:
        metadata["comparison_summary"] = comparison_summary
        metadata["comparison_tone"] = comparison_tone

    return ExternalSourceStatus(
        source=neaa.source,
        status=neaa.status,
        message=neaa.message,
        institution_id=neaa.institution_id,
        metadata=metadata,
    )


def compare_neaa_to_deqar(neaa_metadata: dict[str, object], deqar: QualityInstitutionStatus) -> tuple[str | None, str | None]:
    neaa_decision_date = parse_iso_date(str(neaa_metadata.get("decision_date") or ""))
    neaa_valid_to = parse_iso_date(str(neaa_metadata.get("valid_to") or ""))
    deqar_metadata = deqar.metadata or {}
    deqar_anchor_date = parse_iso_date(
        str(
            deqar_metadata.get("current_institutional_decision_date")
            or deqar_metadata.get("latest_institutional_decision_date")
            or deqar.decision_date
            or ""
        )
    )
    deqar_valid_to = parse_iso_date(
        str(
            deqar_metadata.get("current_institutional_valid_to")
            or deqar_metadata.get("latest_institutional_valid_to")
            or deqar_metadata.get("institutional_valid_to")
            or ""
        )
    )

    if deqar.status != "active":
        return (
            "NEAA provides Bulgaria-specific institutional accreditation context while DEQAR is unavailable for this institution.",
            "warning",
        )
    if not deqar_metadata.get("current_institutional_decision_date") and neaa_decision_date:
        return (
            "NEAA lists a local institutional-accreditation decision, while the current DEQAR snapshot does not show an institutional review for this institution.",
            "warning",
        )
    if neaa_decision_date and deqar_anchor_date and neaa_decision_date > deqar_anchor_date:
        return (
            f"NEAA shows a newer Bulgarian institutional decision ({neaa_decision_date.isoformat()}) than the current DEQAR anchor ({deqar_anchor_date.isoformat()}).",
            "warning",
        )
    if neaa_decision_date and deqar_anchor_date and neaa_decision_date < deqar_anchor_date:
        if neaa_valid_to and not deqar_valid_to:
            return (
                f"DEQAR shows a newer institutional decision ({deqar_anchor_date.isoformat()}), while NEAA still exposes a local validity window through {neaa_valid_to.isoformat()} that is not listed in the current DEQAR snapshot.",
                "neutral",
            )
        return (
            f"DEQAR shows a newer institutional decision ({deqar_anchor_date.isoformat()}) than the current NEAA overlay ({neaa_decision_date.isoformat()}).",
            "neutral",
        )
    if neaa_decision_date and deqar_anchor_date and neaa_decision_date == deqar_anchor_date:
        if neaa_valid_to and deqar_valid_to and neaa_valid_to != deqar_valid_to:
            return (
                f"NEAA and DEQAR align on the decision date ({neaa_decision_date.isoformat()}), but the listed validity windows differ ({neaa_valid_to.isoformat()} vs {deqar_valid_to.isoformat()}).",
                "neutral",
            )
        return (
            f"NEAA and DEQAR align on the institutional decision date ({neaa_decision_date.isoformat()}).",
            "neutral",
        )
    return None, None


def parse_iso_date(value: str) -> date | None:
    candidate = value.strip()
    if not candidate:
        return None
    try:
        return date.fromisoformat(candidate)
    except ValueError:
        return None
