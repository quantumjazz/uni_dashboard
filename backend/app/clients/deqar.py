from __future__ import annotations

import csv
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Iterable
from urllib.parse import urlparse

from backend.app.config import get_settings
from backend.app.models.schemas import InstitutionOption, QualityInstitutionStatus, QualityReportSummary
from backend.app.services.institution_registry import (
    IdentifierAssertion,
    InstitutionRecord,
    InstitutionRegistry,
    NameVariant,
    RegistrationRequest,
)


logger = logging.getLogger(__name__)

REPORT_FILE_URL_PATTERN = re.compile(r"\((https?://[^)]+)\)")
NAME_VARIANT_SPLIT_PATTERN = re.compile(r"\s*[;|]\s*")
IDENTIFIER_SPLIT_PATTERN = re.compile(r"\s*,\s*")

MATCH_CONFIDENCE_BY_TYPE = {
    "deqar_id": ("high", 1.0, "Matched on an explicit DEQAR institution identifier."),
    "eter_id": ("high", 0.995, "Matched on a shared ETER identifier."),
    "ror": ("high", 1.0, "Matched on a shared canonical ROR identifier."),
    "website": ("high", 0.99, "Matched on a unique institution website host."),
    "exact_name": ("high", 0.96, "Matched on a unique normalized institution name."),
    "exact_name_and_website": ("high", 1.0, "Matched on both normalized institution name and website host."),
    "website_and_fuzzy_name": ("medium", 0.9, "Matched on website host and a close institution-name variant."),
    "fuzzy_name": ("low", 0.82, "Matched on a close institution-name variant only and should be reviewed carefully."),
    "registry_crosswalk": ("high", 0.92, "Matched through the cross-source institution crosswalk."),
}

COVERAGE_NOTICE = (
    "DEQAR covers reports uploaded by EQAR-registered agencies that participate in DEQAR. "
    "Missing coverage in this snapshot does not by itself mean that the institution lacks accreditation."
)


@dataclass(slots=True)
class DeqarInstitutionRecord:
    deqar_id: str
    deqar_url: str | None
    eter_id: str | None
    ror_id: str | None
    country: str | None
    name_primary: str
    name_official: str | None
    city: str | None
    website_link: str | None
    report_count: int
    name_candidates: set[str] = field(default_factory=set)
    normalized_names: set[str] = field(default_factory=set)
    website_host: str | None = None


@dataclass(slots=True)
class DeqarReportRecord:
    report_id: str
    institution_id: str
    parent_institution_id: str | None
    programme_name: str | None
    report_url: str | None
    report_agency: str | None
    report_type: str | None
    report_activity: str | None
    report_status: str | None
    report_decision: str | None
    valid_from: date | None
    valid_to: date | None
    file_url: str | None

    @property
    def effective_url(self) -> str | None:
        return self.file_url or self.report_url


@dataclass(slots=True)
class DeqarAgencyRecord:
    agency_id: str
    acronym_primary: str | None
    name_primary: str
    is_registered: str | None
    registration_start: date | None
    registration_valid_to: date | None
    register_entry: str | None
    deqar_reports_link: str | None


@dataclass(slots=True)
class DeqarInstitutionMatch:
    institution: DeqarInstitutionRecord
    match_type: str
    matched_value: str
    registry_record: InstitutionRecord | None = None
    matched_via_registry: bool = False


@dataclass(slots=True)
class DeqarDataset:
    institutions: list[DeqarInstitutionRecord]
    institutions_by_id: dict[str, DeqarInstitutionRecord]
    institutions_by_eter_id: dict[str, list[DeqarInstitutionRecord]]
    institutions_by_ror: dict[str, list[DeqarInstitutionRecord]]
    institutions_by_host: dict[str, list[DeqarInstitutionRecord]]
    institutions_by_name: dict[str, list[DeqarInstitutionRecord]]
    reports_by_institution_id: dict[str, list[DeqarReportRecord]]
    agencies_by_name: dict[str, DeqarAgencyRecord]

    def match_institution(self, institution: InstitutionOption) -> DeqarInstitutionMatch | None:
        if looks_like_deqar_identifier(institution.id):
            direct_match = self.institutions_by_id.get(institution.id.strip())
            if direct_match:
                return DeqarInstitutionMatch(
                    institution=direct_match,
                    match_type="deqar_id",
                    matched_value=direct_match.deqar_id,
                )

        eter_id = normalize_identifier(institution.eter_id)
        if eter_id:
            eter_candidates = self.institutions_by_eter_id.get(eter_id, [])
            matched = resolve_identifier_candidates(eter_candidates, institution)
            if matched:
                return DeqarInstitutionMatch(
                    institution=matched,
                    match_type="eter_id",
                    matched_value=eter_id,
                )

        ror_id = normalize_ror(institution.ror)
        if ror_id:
            ror_candidates = self.institutions_by_ror.get(ror_id, [])
            matched = resolve_identifier_candidates(ror_candidates, institution)
            if matched:
                return DeqarInstitutionMatch(
                    institution=matched,
                    match_type="ror",
                    matched_value=ror_id,
                )

        host = normalize_host(institution.homepage_url)
        if host:
            host_candidates = self.institutions_by_host.get(host, [])
            if len(host_candidates) == 1:
                return DeqarInstitutionMatch(
                    institution=host_candidates[0],
                    match_type="website",
                    matched_value=host,
                )

        if looks_like_identifier(institution.display_name):
            return None
        normalized_names = institution_normalized_names(institution)
        if not normalized_names:
            return None

        for normalized_name in normalized_names:
            exact_candidates = self.institutions_by_name.get(normalized_name, [])
            if len(exact_candidates) == 1:
                return DeqarInstitutionMatch(
                    institution=exact_candidates[0],
                    match_type="exact_name",
                    matched_value=normalized_name,
                )
            if len(exact_candidates) > 1 and host:
                host_filtered = [candidate for candidate in exact_candidates if candidate.website_host == host]
                if len(host_filtered) == 1:
                    return DeqarInstitutionMatch(
                        institution=host_filtered[0],
                        match_type="exact_name_and_website",
                        matched_value=normalized_name,
                    )

        if host:
            host_candidates = self.institutions_by_host.get(host, [])
            if host_candidates:
                best_host_match = best_name_match(normalized_names, host_candidates)
                if best_host_match:
                    return DeqarInstitutionMatch(
                        institution=best_host_match,
                        match_type="website_and_fuzzy_name",
                        matched_value=host,
                    )

        best_global_match = best_name_match(normalized_names, self.institutions)
        if best_global_match:
            return DeqarInstitutionMatch(
                institution=best_global_match,
                match_type="fuzzy_name",
                matched_value=institution.display_name,
            )

        return None

    def reports_for_institution(self, deqar_id: str) -> list[DeqarReportRecord]:
        return list(self.reports_by_institution_id.get(deqar_id, []))

    def resolve_agency_name(self, raw_value: str | None) -> str | None:
        agency = self.resolve_agency(raw_value)
        if agency:
            return agency.name_primary
        return raw_value

    def resolve_agency(self, raw_value: str | None) -> DeqarAgencyRecord | None:
        if not raw_value:
            return None
        return self.agencies_by_name.get(normalize_text(raw_value))


class DeqarClient:
    """Loads DEQAR CSV exports from disk and maps them to dashboard quality responses."""

    def __init__(self, registry: InstitutionRegistry | None = None) -> None:
        self.settings = get_settings()
        self.base_url = self.settings.deqar_base_url.rstrip("/")
        self.api_key = self.settings.deqar_api_key
        self.reports_csv_path = self.settings.deqar_reports_csv_path
        self.institutions_csv_path = self.settings.deqar_institutions_csv_path
        self.agencies_csv_path = self.settings.deqar_agencies_csv_path
        self.report_limit = self.settings.deqar_report_limit
        self.registry = registry or InstitutionRegistry()
        self._dataset_cache: tuple[tuple[tuple[str, int, int], ...], DeqarDataset] | None = None
        self._registry_populated_signature: tuple[tuple[str, int, int], ...] | None = None

    def is_configured(self) -> bool:
        return self.reports_csv_path.is_file() and self.institutions_csv_path.is_file()

    def missing_dataset_paths(self) -> list[str]:
        missing: list[str] = []
        for path in (self.institutions_csv_path, self.reports_csv_path):
            if not path.is_file():
                missing.append(str(path))
        return missing

    def build_quality_status(self, institution: InstitutionOption) -> QualityInstitutionStatus:
        dataset = self._load_dataset()
        dataset_metadata = self._dataset_metadata()
        match, reports = self._match_reports(dataset, institution)
        if not match:
            return QualityInstitutionStatus(
                source="deqar",
                institution_id=institution.id,
                status="ready",
                summary=(
                    f"DEQAR datasets are loaded, but no institution match was found yet for "
                    f"{institution.display_name}. Treat this as missing DEQAR coverage only, "
                    "not as evidence that accreditation is absent."
                ),
                metadata={
                    "institution_name": institution.display_name,
                    "dataset_source": "csv",
                    **dataset_metadata,
                    "coverage_notice": COVERAGE_NOTICE,
                    "match_confidence": "none",
                    "match_confidence_score": 0.0,
                    "match_confidence_note": "No DEQAR institution match was found in the local snapshot.",
                    "openalex_ror": normalize_ror(institution.ror),
                    "openalex_eter_id": normalize_identifier(institution.eter_id),
                    "next_step": "Check the OpenAlex ROR, homepage, or name metadata and add a stronger matching key if this university should exist in DEQAR.",
                },
            )

        total_reports = len(reports)
        match_metadata = match_confidence_metadata(match.match_type)
        crosswalk_metadata = build_match_identity_metadata(institution, match)
        report_coverage = summarize_report_coverage(reports)
        date_metadata = summarize_report_dates(reports)
        decision_analytics = summarize_decision_analytics(reports)
        institutional_risk = summarize_institutional_risk(reports)
        qa_risk = summarize_quality_risk(institutional_risk, decision_analytics)
        current_institutional = next(
            (report for report in reports if classify_report_scope(report) == "institutional" and is_current_report(report)),
            None,
        )
        if not reports:
            return QualityInstitutionStatus(
                source="deqar",
                institution_id=institution.id,
                status="ready",
                summary=(
                    f"Matched {match.institution.name_primary} in DEQAR, but the downloaded dataset has no linked reports for it. "
                    "This should still be treated as a DEQAR coverage gap, not proof that accreditation is absent."
                ),
                metadata={
                    "institution_name": institution.display_name,
                    "dataset_source": "csv",
                    **dataset_metadata,
                    "coverage_notice": COVERAGE_NOTICE,
                    "deqar_id": match.institution.deqar_id,
                    "deqar_url": match.institution.deqar_url,
                    "matched_institution_name": match.institution.name_primary,
                    "match_type": match.match_type,
                    "match_value": match.matched_value,
                    **match_metadata,
                    **crosswalk_metadata,
                    **report_coverage,
                    **date_metadata,
                    **decision_analytics,
                    **institutional_risk,
                    **qa_risk,
                    "next_step": "Refresh the local DEQAR CSV snapshot if this institution should already have published reports.",
                    "report_count": 0,
                },
            )

        lead_report = self._select_lead_report(reports)
        lead_agency = dataset.resolve_agency(lead_report.report_agency)
        displayed_reports = reports if self.report_limit <= 0 else reports[: self.report_limit]
        report_summaries = [self._to_report_summary(report, dataset) for report in displayed_reports]
        current_status = lead_report.report_decision or lead_report.report_status or lead_report.report_activity or lead_report.report_type

        return QualityInstitutionStatus(
            source="deqar",
            institution_id=institution.id,
            status="active",
            current_status=current_status,
            agency=dataset.resolve_agency_name(lead_report.report_agency),
            decision_date=lead_report.valid_from.isoformat() if lead_report.valid_from else None,
            summary=self._build_active_summary(match, reports, dataset),
            reports=report_summaries,
            metadata={
                "institution_name": institution.display_name,
                "dataset_source": "csv",
                **dataset_metadata,
                "coverage_notice": COVERAGE_NOTICE,
                "deqar_id": match.institution.deqar_id,
                "deqar_url": match.institution.deqar_url,
                "matched_institution_name": match.institution.name_primary,
                "match_type": match.match_type,
                "match_value": match.matched_value,
                **match_metadata,
                **crosswalk_metadata,
                "report_count": total_reports,
                "displayed_report_count": len(report_summaries),
                **report_coverage,
                **date_metadata,
                **decision_analytics,
                **institutional_risk,
                **qa_risk,
                **build_agency_register_metadata(lead_agency),
                "current_institutional_decision_date": current_institutional.valid_from.isoformat()
                if current_institutional and current_institutional.valid_from
                else None,
                "current_institutional_valid_to": current_institutional.valid_to.isoformat()
                if current_institutional and current_institutional.valid_to
                else None,
                "next_step": "Refresh the local DEQAR CSV files whenever you want a newer snapshot of decisions and reports.",
            },
        )

    def build_benchmark_peer_summary(self, institution: InstitutionOption) -> dict[str, object]:
        dataset = self._load_dataset()
        match, reports = self._match_reports(dataset, institution)
        if not match:
            return {
                "institution_id": institution.id,
                "display_name": institution.display_name,
                "country_code": institution.country_code,
                "readiness": "limited",
                "deqar_status": "ready",
                "current_status": None,
                "agency": None,
                "decision_date": None,
                "report_count": 0,
                "institutional_report_count": 0,
                "deqar_id": None,
                "deqar_url": None,
                "matched_institution_name": None,
                "match_type": None,
                "match_value": None,
            }

        institutional_report_count = sum(1 for report in reports if classify_report_scope(report) == "institutional")
        institutional_reviews = [
            {
                "report_id": report.report_id,
                "report_type": titleize(report.report_activity or report.report_type or "Institutional review"),
                "decision": report.report_decision or report.report_status,
                "agency": dataset.resolve_agency_name(report.report_agency),
                "decision_date": report.valid_from.isoformat() if report.valid_from else None,
                "valid_to": report.valid_to.isoformat() if report.valid_to else None,
                "report_url": report.effective_url,
                "is_current": is_current_report(report),
            }
            for report in reports
            if classify_report_scope(report) == "institutional"
        ]
        lead_report = self._select_lead_report(reports) if reports else None
        institutional_risk = summarize_institutional_risk(reports)
        decision_analytics = summarize_decision_analytics(reports)
        qa_risk = summarize_quality_risk(institutional_risk, decision_analytics)
        readiness = "ready" if institutional_report_count > 0 else ("partial" if reports else "limited")

        return {
            "institution_id": institution.id,
            "display_name": institution.display_name,
            "country_code": institution.country_code,
            "readiness": readiness,
            "deqar_status": "active" if reports else "ready",
            "current_status": (
                lead_report.report_decision or lead_report.report_status or lead_report.report_activity or lead_report.report_type
            ) if lead_report else None,
            "agency": dataset.resolve_agency_name(lead_report.report_agency) if lead_report else None,
            "decision_date": lead_report.valid_from.isoformat() if lead_report and lead_report.valid_from else None,
            "report_count": len(reports),
            "institutional_report_count": institutional_report_count,
            "institutional_reviews": institutional_reviews,
            "deqar_id": match.institution.deqar_id,
            "deqar_url": match.institution.deqar_url,
            "matched_institution_name": match.institution.name_primary,
            "match_type": match.match_type,
            "match_value": match.matched_value,
            **match_confidence_metadata(match.match_type),
            **build_match_identity_metadata(institution, match),
            "anchor_decision_tone": decision_analytics["anchor_decision_tone"],
            "institutional_validity_status": institutional_risk["institutional_validity_status"],
            "institutional_validity_label": institutional_risk["institutional_validity_label"],
            "institutional_days_remaining": institutional_risk["institutional_days_remaining"],
            "institutional_review_age_days": institutional_risk["institutional_review_age_days"],
            "institutional_valid_to": institutional_risk["institutional_valid_to"],
            "qa_risk_level": qa_risk["qa_risk_level"],
            "qa_risk_summary": qa_risk["qa_risk_summary"],
        }

    def _load_dataset(self) -> DeqarDataset:
        if not self.is_configured():
            missing = ", ".join(self.missing_dataset_paths())
            raise FileNotFoundError(f"Required DEQAR CSV datasets are missing: {missing}")

        signature = self._dataset_signature()
        cached = self._dataset_cache
        if cached and cached[0] == signature:
            return cached[1]

        logger.info(
            "Loading DEQAR datasets from %s and %s",
            self.institutions_csv_path,
            self.reports_csv_path,
        )
        dataset = self._build_dataset()
        self._dataset_cache = (signature, dataset)
        self._populate_registry(dataset, signature)
        return dataset

    def _populate_registry(
        self,
        dataset: DeqarDataset,
        signature: tuple[tuple[str, int, int], ...],
    ) -> None:
        """Register every DEQAR institution into the crosswalk on CSV (re)load.

        ID-based merging only — a DEQAR row sharing an ETER or ROR identifier with an
        already-registered OpenAlex institution will land on the same ``institution_uid``.
        Name-cascade matching (fuzzy / website+name) still runs at request time in
        ``DeqarDataset.match_institution`` until the OpenAlex side also registers.
        """
        if self._registry_populated_signature == signature:
            return

        requests: list[RegistrationRequest] = []
        skipped_no_country = 0
        for institution in dataset.institutions:
            if not institution.country:
                skipped_no_country += 1
                continue

            identifiers: list[IdentifierAssertion] = [
                IdentifierAssertion(
                    scheme="deqar",
                    value=institution.deqar_id,
                    source="deqar",
                    confidence=1.0,
                )
            ]
            if institution.eter_id:
                identifiers.append(
                    IdentifierAssertion(
                        scheme="eter",
                        value=institution.eter_id,
                        source="deqar",
                        confidence=MATCH_CONFIDENCE_BY_TYPE["eter_id"][1],
                    )
                )
            if institution.ror_id:
                identifiers.append(
                    IdentifierAssertion(
                        scheme="ror",
                        value=institution.ror_id,
                        source="deqar",
                        confidence=MATCH_CONFIDENCE_BY_TYPE["ror"][1],
                    )
                )

            name_variants: list[NameVariant] = [
                NameVariant(variant=institution.name_primary, source="deqar")
            ]
            if institution.name_official and institution.name_official != institution.name_primary:
                name_variants.append(
                    NameVariant(variant=institution.name_official, source="deqar")
                )
            for candidate in institution.name_candidates:
                if candidate and candidate not in (institution.name_primary, institution.name_official):
                    name_variants.append(NameVariant(variant=candidate, source="deqar"))

            requests.append(
                RegistrationRequest(
                    canonical_name=institution.name_primary,
                    country_code=institution.country,
                    identifiers=identifiers,
                    name_variants=name_variants,
                    website_host=institution.website_host,
                    source="deqar",
                )
            )

        mapping = self.registry.bulk_register(requests, skip_cascade=True)
        logger.info(
            "DEQAR registry population: %d institutions registered, %d skipped (no country)",
            len(mapping),
            skipped_no_country,
        )
        self._registry_populated_signature = signature

    def _dataset_signature(self) -> tuple[tuple[str, int, int], ...]:
        paths = [self.institutions_csv_path, self.reports_csv_path]
        if self.agencies_csv_path.is_file():
            paths.append(self.agencies_csv_path)
        return tuple((str(path.resolve()), path.stat().st_mtime_ns, path.stat().st_size) for path in paths)

    def _build_dataset(self) -> DeqarDataset:
        institutions = self._load_institutions()
        institutions_by_id = {institution.deqar_id: institution for institution in institutions}
        institutions_by_eter_id: dict[str, list[DeqarInstitutionRecord]] = {}
        institutions_by_ror: dict[str, list[DeqarInstitutionRecord]] = {}
        institutions_by_host: dict[str, list[DeqarInstitutionRecord]] = {}
        institutions_by_name: dict[str, list[DeqarInstitutionRecord]] = {}

        for institution in institutions:
            if institution.eter_id:
                institutions_by_eter_id.setdefault(institution.eter_id, []).append(institution)
            if institution.ror_id:
                institutions_by_ror.setdefault(institution.ror_id, []).append(institution)
            if institution.website_host:
                institutions_by_host.setdefault(institution.website_host, []).append(institution)
            for normalized_name in institution.normalized_names:
                institutions_by_name.setdefault(normalized_name, []).append(institution)

        reports_by_institution_id = self._load_reports()
        agencies_by_name = self._load_agencies()
        for report_list in reports_by_institution_id.values():
            report_list.sort(key=report_sort_key, reverse=True)

        return DeqarDataset(
            institutions=institutions,
            institutions_by_id=institutions_by_id,
            institutions_by_eter_id=institutions_by_eter_id,
            institutions_by_ror=institutions_by_ror,
            institutions_by_host=institutions_by_host,
            institutions_by_name=institutions_by_name,
            reports_by_institution_id=reports_by_institution_id,
            agencies_by_name=agencies_by_name,
        )

    def _match_reports(
        self,
        dataset: DeqarDataset,
        institution: InstitutionOption,
    ) -> tuple[DeqarInstitutionMatch | None, list[DeqarReportRecord]]:
        match = self._match_via_registry(dataset, institution) or dataset.match_institution(institution)
        if not match:
            return None, []
        return match, dataset.reports_for_institution(match.institution.deqar_id)

    def _match_via_registry(
        self,
        dataset: DeqarDataset,
        institution: InstitutionOption,
    ) -> DeqarInstitutionMatch | None:
        """Registry fast path: skip the in-memory cascade when the crosswalk already
        knows which DEQAR record the incoming institution maps to.
        """
        lookup_scheme = "deqar" if looks_like_deqar_identifier(institution.id) else "openalex"
        try:
            record = self.registry.resolve(lookup_scheme, institution.id)
        except Exception:  # noqa: BLE001
            logger.exception("Registry lookup failed for %s=%s", lookup_scheme, institution.id)
            return None
        if record is None:
            return None
        deqar_id = record.identifiers.get("deqar")
        if not deqar_id:
            return None
        deqar_record = dataset.institutions_by_id.get(deqar_id)
        if not deqar_record:
            return None
        match_type = self.registry.last_merge_match_type(record.institution_uid) or "registry_crosswalk"
        if match_type not in MATCH_CONFIDENCE_BY_TYPE:
            match_type = "registry_crosswalk"
        return DeqarInstitutionMatch(
            institution=deqar_record,
            match_type=match_type,
            matched_value=registry_matched_value(match_type, record, deqar_record),
            registry_record=record,
            matched_via_registry=True,
        )

    def _load_institutions(self) -> list[DeqarInstitutionRecord]:
        institutions: list[DeqarInstitutionRecord] = []
        with self.institutions_csv_path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                deqar_id = (row.get("deqar_id") or "").strip()
                if not deqar_id:
                    continue

                name_primary = (row.get("name_primary") or "").strip() or "Unknown institution"
                name_candidates = {
                    candidate
                    for candidate in iter_name_candidates(
                        name_primary,
                        row.get("name_official"),
                        row.get("name_versions"),
                    )
                }
                normalized_names = {normalize_text(candidate) for candidate in name_candidates if normalize_text(candidate)}
                website_link = (row.get("website_link") or "").strip() or None

                institutions.append(
                    DeqarInstitutionRecord(
                        deqar_id=deqar_id,
                        deqar_url=(row.get("deqar_url") or "").strip() or None,
                        eter_id=normalize_identifier(row.get("eter_id")),
                        ror_id=normalize_ror(first_identifier_value(row.get("identifiers_all"), "ROR")),
                        country=(row.get("country") or "").strip() or None,
                        name_primary=name_primary,
                        name_official=(row.get("name_official") or "").strip() or None,
                        city=(row.get("city") or "").strip() or None,
                        website_link=website_link,
                        report_count=parse_int(row.get("report_count")),
                        name_candidates=name_candidates,
                        normalized_names=normalized_names,
                        website_host=normalize_host(website_link),
                    )
                )
        return institutions

    def _load_reports(self) -> dict[str, list[DeqarReportRecord]]:
        reports_by_institution_id: dict[str, list[DeqarReportRecord]] = {}
        with self.reports_csv_path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                institution_id = (row.get("hei_deqar_id") or "").strip()
                if not institution_id:
                    continue

                report = DeqarReportRecord(
                    report_id=(row.get("report_id") or "").strip(),
                    institution_id=institution_id,
                    parent_institution_id=(row.get("parent_hei_deqar_id") or "").strip() or None,
                    programme_name=(row.get("programme_name") or "").strip() or None,
                    report_url=(row.get("report_url") or "").strip() or None,
                    report_agency=(row.get("report_agency") or "").strip() or None,
                    report_type=(row.get("report_type") or "").strip() or None,
                    report_activity=(row.get("report_esg_activity_short") or "").strip() or None,
                    report_status=(row.get("report_status") or "").strip() or None,
                    report_decision=(row.get("report_decision") or "").strip() or None,
                    valid_from=parse_date(row.get("report_valid_from")),
                    valid_to=parse_date(row.get("report_valid_to")),
                    file_url=extract_first_url(row.get("report_files")),
                )
                attach_report(reports_by_institution_id, institution_id, report)
                parent_id = report.parent_institution_id
                if parent_id and parent_id != institution_id:
                    attach_report(reports_by_institution_id, parent_id, report)

        return reports_by_institution_id

    def _load_agencies(self) -> dict[str, DeqarAgencyRecord]:
        if not self.agencies_csv_path.is_file():
            return {}

        agencies_by_name: dict[str, DeqarAgencyRecord] = {}
        with self.agencies_csv_path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                name_primary = (row.get("name_primary") or "").strip()
                acronym_primary = (row.get("acronym_primary") or "").strip()
                if not name_primary and not acronym_primary:
                    continue

                agency = DeqarAgencyRecord(
                    agency_id=(row.get("agency_id") or "").strip(),
                    acronym_primary=acronym_primary or None,
                    name_primary=name_primary or acronym_primary or "Unknown agency",
                    is_registered=(row.get("is_registered") or "").strip() or None,
                    registration_start=parse_date(row.get("registration_start")),
                    registration_valid_to=parse_date(row.get("registration_valid_to")),
                    register_entry=(row.get("register_entry") or "").strip() or None,
                    deqar_reports_link=(row.get("deqar_reports_link") or "").strip() or None,
                )

                for raw_name in (name_primary, acronym_primary):
                    normalized = normalize_text(raw_name)
                    if normalized:
                        agencies_by_name[normalized] = agency
        return agencies_by_name

    def _select_lead_report(self, reports: list[DeqarReportRecord]) -> DeqarReportRecord:
        current_institutional = [
            report
            for report in reports
            if classify_report_scope(report) == "institutional" and is_current_report(report)
        ]
        if current_institutional:
            return current_institutional[0]

        institutional_reports = [report for report in reports if classify_report_scope(report) == "institutional"]
        if institutional_reports:
            return institutional_reports[0]

        current_reports = [report for report in reports if is_current_report(report)]
        if current_reports:
            return current_reports[0]

        return reports[0]

    def _to_report_summary(self, report: DeqarReportRecord, dataset: DeqarDataset) -> QualityReportSummary:
        report_type = titleize(report.report_activity or report.report_type or "Report")
        if report.programme_name and report.report_type != "institutional":
            report_type = f"{report_type}: {report.programme_name}"

        agency = dataset.resolve_agency(report.report_agency)
        agency_metadata = build_agency_register_metadata(agency)

        return QualityReportSummary(
            report_id=report.report_id,
            report_type=report_type,
            scope=classify_report_scope(report),
            decision=report.report_decision or report.report_status,
            agency=dataset.resolve_agency_name(report.report_agency),
            agency_listing_status=agency_metadata.get("agency_register_status"),
            agency_listing_note=agency_metadata.get("agency_register_note"),
            agency_listing_valid_to=agency_metadata.get("agency_register_valid_to"),
            agency_register_url=agency_metadata.get("agency_register_url"),
            agency_reports_url=agency_metadata.get("agency_reports_url"),
            decision_date=report.valid_from.isoformat() if report.valid_from else None,
            valid_to=report.valid_to.isoformat() if report.valid_to else None,
            report_url=report.effective_url,
        )

    def _build_active_summary(
        self,
        match: DeqarInstitutionMatch,
        reports: list[DeqarReportRecord],
        dataset: DeqarDataset,
    ) -> str:
        lead_report = self._select_lead_report(reports)
        agency = dataset.resolve_agency_name(lead_report.report_agency) or "an EQAR-listed agency"
        decision = lead_report.report_decision or lead_report.report_status or "a recorded decision"
        decision_date = lead_report.valid_from.isoformat() if lead_report.valid_from else "an unknown date"
        return (
            f"Matched {match.institution.name_primary} in the downloaded DEQAR datasets via {match_summary_phrase(match)}, "
            f"with {len(reports)} linked reports. "
            f"The latest anchor decision is {decision} by {agency} on {decision_date}."
        )

    def _dataset_metadata(self) -> dict[str, object]:
        file_metadata = self._dataset_file_metadata()
        if not file_metadata:
            return {
                "dataset_updated_at": None,
                "dataset_age_days": None,
                "dataset_files": [],
            }

        latest_timestamp = max(item["modified_at_timestamp"] for item in file_metadata)
        latest_datetime = datetime.fromtimestamp(latest_timestamp, tz=timezone.utc)
        dataset_files = [
            {
                "name": str(item["name"]),
                "modified_at": item["modified_at"],
                "size_bytes": item["size_bytes"],
            }
            for item in file_metadata
        ]

        return {
            "dataset_updated_at": latest_datetime.isoformat().replace("+00:00", "Z"),
            "dataset_age_days": max(0, (datetime.now(timezone.utc).date() - latest_datetime.date()).days),
            "dataset_files": dataset_files,
        }

    def _dataset_file_metadata(self) -> list[dict[str, object]]:
        files = [self.institutions_csv_path, self.reports_csv_path]
        if self.agencies_csv_path.is_file():
            files.append(self.agencies_csv_path)

        metadata: list[dict[str, object]] = []
        for path in files:
            stat = path.stat()
            modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            metadata.append(
                {
                    "name": path.name,
                    "modified_at": modified_at.isoformat().replace("+00:00", "Z"),
                    "modified_at_timestamp": stat.st_mtime,
                    "size_bytes": stat.st_size,
                }
            )
        return metadata


def iter_name_candidates(*values: str | None) -> Iterable[str]:
    seen: set[str] = set()
    for raw_value in values:
        if not raw_value:
            continue
        for candidate in NAME_VARIANT_SPLIT_PATTERN.split(raw_value):
            cleaned = candidate.strip().strip('"')
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                yield cleaned


def institution_normalized_names(institution: InstitutionOption) -> list[str]:
    normalized_names: list[str] = []
    seen: set[str] = set()
    for raw_value in [institution.display_name, *(institution.aliases or [])]:
        normalized = normalize_text(raw_value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_names.append(normalized)
    return normalized_names


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value.casefold())
    ascii_text = "".join(char for char in normalized if not unicodedata.combining(char))
    ascii_text = ascii_text.replace("&", " and ")
    ascii_text = ascii_text.replace("st.", "st ")
    ascii_text = ascii_text.replace("saint ", "st ")
    collapsed = re.sub(r"[^a-z0-9]+", " ", ascii_text)
    return " ".join(collapsed.split())


def looks_like_identifier(value: str | None) -> bool:
    if not value:
        return False
    return bool(re.fullmatch(r"[A-Za-z]\d{5,}", value.strip()))


def looks_like_deqar_identifier(value: str | None) -> bool:
    if not value:
        return False
    return bool(re.fullmatch(r"DEQARINST\d+", value.strip()))


def normalize_identifier(value: str | None) -> str | None:
    candidate = (value or "").strip()
    return candidate or None


def normalize_ror(value: str | None) -> str | None:
    candidate = (value or "").strip()
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


def normalize_host(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
    host = parsed.netloc or parsed.path
    host = host.casefold().strip("/")
    if host.startswith("www."):
        host = host[4:]
    return host or None


def match_confidence_metadata(match_type: str) -> dict[str, object]:
    confidence, score, note = MATCH_CONFIDENCE_BY_TYPE.get(
        match_type,
        ("low", 0.75, "Matched using a fallback institution-identity heuristic."),
    )
    return {
        "match_confidence": confidence,
        "match_confidence_score": score,
        "match_confidence_note": note,
    }


def build_match_identity_metadata(
    institution: InstitutionOption,
    match: DeqarInstitutionMatch,
) -> dict[str, object]:
    registry_record = match.registry_record
    registry_identifiers = registry_record.identifiers if registry_record else {}
    if match.match_type == "ror":
        crosswalk_scheme = "ROR"
        crosswalk_note = "OpenAlex and DEQAR share the same ROR identifier for this institution."
    elif match.match_type == "eter_id":
        crosswalk_scheme = "ETER"
        crosswalk_note = "Source data and DEQAR share the same ETER identifier for this institution."
    elif match.match_type == "deqar_id":
        crosswalk_scheme = "DEQAR"
        crosswalk_note = "The institution was requested by an explicit DEQAR identifier."
    elif match.matched_via_registry:
        crosswalk_scheme = "Registry"
        crosswalk_note = "The cross-source institution registry had already linked this institution to DEQAR before fallback matching was needed."
    else:
        crosswalk_scheme = None
        crosswalk_note = None

    return {
        "openalex_ror": normalize_ror(institution.ror),
        "openalex_eter_id": normalize_identifier(institution.eter_id),
        "matched_ror": match.institution.ror_id,
        "matched_eter_id": match.institution.eter_id,
        "registry_institution_uid": registry_record.institution_uid if registry_record else None,
        "registry_canonical_name": registry_record.canonical_name if registry_record else None,
        "registry_country_code": registry_record.country_code if registry_record else None,
        "registry_website_host": registry_record.website_host if registry_record else None,
        "registry_eter_id": registry_record.eter_id if registry_record else None,
        "registry_ror": registry_identifiers.get("ror"),
        "registry_openalex_id": registry_identifiers.get("openalex"),
        "registry_institution_type": registry_record.institution_type if registry_record else None,
        "registry_legal_status": registry_record.legal_status if registry_record else None,
        "crosswalk_scheme": crosswalk_scheme,
        "crosswalk_value": match.matched_value,
        "crosswalk_note": crosswalk_note,
        "matched_via_registry": match.matched_via_registry,
        "match_lookup_path": match_lookup_path(match),
        "match_provenance_label": match_provenance_label(match),
        "match_provenance_note": match_provenance_note(match),
    }


def registry_matched_value(
    match_type: str,
    record: InstitutionRecord,
    deqar_record: DeqarInstitutionRecord,
) -> str:
    if match_type == "eter_id":
        return normalize_identifier(record.identifiers.get("eter") or record.eter_id or deqar_record.eter_id) or deqar_record.deqar_id
    if match_type == "ror":
        return normalize_ror(record.identifiers.get("ror") or deqar_record.ror_id) or deqar_record.deqar_id
    if match_type == "deqar_id":
        return deqar_record.deqar_id
    if match_type in {"website", "exact_name_and_website", "website_and_fuzzy_name"}:
        return record.website_host or deqar_record.website_host or deqar_record.deqar_id
    if match_type in {"exact_name", "fuzzy_name"}:
        return record.canonical_name or deqar_record.name_primary
    return deqar_record.deqar_id


def match_lookup_path(match: DeqarInstitutionMatch) -> str:
    if match.matched_via_registry:
        return "registry"
    if match.match_type == "deqar_id":
        return "explicit_id"
    return "deqar_dataset"


def match_provenance_label(match: DeqarInstitutionMatch) -> str:
    if match.matched_via_registry:
        if match.match_type == "eter_id":
            return "Registry-linked via ETER"
        if match.match_type == "ror":
            return "Registry-linked via ROR"
        if match.match_type == "website":
            return "Registry-linked via website"
        if match.match_type == "exact_name":
            return "Registry-linked via exact name"
        if match.match_type == "exact_name_and_website":
            return "Registry-linked via name + website"
        if match.match_type == "website_and_fuzzy_name":
            return "Registry-linked via website + fuzzy name"
        if match.match_type == "fuzzy_name":
            return "Registry-linked via fuzzy name"
        return "Registry-linked crosswalk"
    if match.match_type == "deqar_id":
        return "Explicit DEQAR ID"
    if match.match_type == "eter_id":
        return "Shared ETER identifier"
    if match.match_type == "ror":
        return "Shared ROR identifier"
    if match.match_type == "website":
        return "Website host"
    if match.match_type == "exact_name":
        return "Exact institution name"
    if match.match_type == "exact_name_and_website":
        return "Name + website"
    if match.match_type == "website_and_fuzzy_name":
        return "Website + fuzzy name"
    if match.match_type == "fuzzy_name":
        return "Fuzzy institution name"
    return "Fallback institution matching"


def match_provenance_note(match: DeqarInstitutionMatch) -> str:
    if match.matched_via_registry:
        return (
            "The cross-source institution registry had already linked this university to the DEQAR provider using "
            f"{match_summary_phrase(match)} before the DEQAR fallback cascade ran."
        )
    if match.match_type == "deqar_id":
        return "The selected institution already carried an explicit DEQAR identifier."
    return (
        "No prior registry link was available, so the DEQAR lookup used "
        f"{match_summary_phrase(match)}."
    )


def build_agency_register_metadata(agency: DeqarAgencyRecord | None) -> dict[str, object]:
    if not agency:
        return {
            "agency_register_status": None,
            "agency_register_note": None,
            "agency_register_valid_to": None,
            "agency_register_url": None,
            "agency_reports_url": None,
        }

    today = date.today()
    register_flag = normalize_text(agency.is_registered)
    valid_to = agency.registration_valid_to

    if "registered" in register_flag and valid_to and valid_to >= today:
        status = "EQAR listed"
        note = f"Current EQAR listing is shown through {valid_to.isoformat()} in the local agencies snapshot."
    elif "registered" in register_flag and valid_to:
        status = "Listing expired"
        note = f"The last listed EQAR validity ended on {valid_to.isoformat()} in the local agencies snapshot."
    elif "registered" in register_flag:
        status = "EQAR listed"
        note = "Agency is marked as registered in the local agencies snapshot."
    elif agency.is_registered:
        status = agency.is_registered
        note = "Agency register status comes from the local agencies snapshot."
    else:
        status = "Register status unavailable"
        note = "No explicit EQAR register status was available for this agency in the local agencies snapshot."

    return {
        "agency_register_status": status,
        "agency_register_note": note,
        "agency_register_valid_to": valid_to.isoformat() if valid_to else None,
        "agency_register_url": agency.register_entry,
        "agency_reports_url": agency.deqar_reports_link,
    }


def summarize_report_coverage(reports: list[DeqarReportRecord]) -> dict[str, object]:
    counts = {"institutional": 0, "programme": 0, "monitoring": 0, "other": 0}
    for report in reports:
        counts[classify_report_scope(report)] += 1

    if not reports:
        coverage_scope = "none"
        coverage_summary = "No linked reports are visible in this DEQAR snapshot."
    elif counts["institutional"] and any(classify_report_scope(report) == "institutional" and is_current_report(report) for report in reports):
        coverage_scope = "institutional_and_programme"
        coverage_summary = "Institutional-level coverage is present in this DEQAR snapshot."
    elif counts["institutional"]:
        coverage_scope = "historical_institutional"
        coverage_summary = "Only historical institutional-review coverage is visible in this DEQAR snapshot."
    elif counts["programme"] or counts["monitoring"]:
        coverage_scope = "programme_only"
        coverage_summary = "Coverage is limited to programme-level or monitoring records in this DEQAR snapshot."
    else:
        coverage_scope = "other_only"
        coverage_summary = "Only non-standard report types are visible in this DEQAR snapshot."

    return {
        "coverage_scope": coverage_scope,
        "coverage_summary": coverage_summary,
        "institutional_report_count": counts["institutional"],
        "programme_report_count": counts["programme"],
        "monitoring_report_count": counts["monitoring"],
        "other_report_count": counts["other"],
    }


def summarize_report_dates(reports: list[DeqarReportRecord]) -> dict[str, object]:
    valid_from_dates = [report.valid_from for report in reports if report.valid_from]
    institutional_reports = [report for report in reports if classify_report_scope(report) == "institutional"]
    latest_institutional = institutional_reports[0] if institutional_reports else None

    return {
        "first_decision_date": min(valid_from_dates).isoformat() if valid_from_dates else None,
        "latest_decision_date": max(valid_from_dates).isoformat() if valid_from_dates else None,
        "latest_institutional_decision_date": latest_institutional.valid_from.isoformat()
        if latest_institutional and latest_institutional.valid_from
        else None,
        "latest_institutional_valid_to": latest_institutional.valid_to.isoformat()
        if latest_institutional and latest_institutional.valid_to
        else None,
    }


def summarize_decision_analytics(reports: list[DeqarReportRecord]) -> dict[str, object]:
    today = date.today()
    recent_cutoff = today - timedelta(days=365 * 5)
    counts = {"positive": 0, "conditional": 0, "negative": 0, "neutral": 0}
    recent_counts = {"positive": 0, "conditional": 0, "negative": 0, "neutral": 0}
    report_years: dict[int, dict[str, int]] = {}

    for report in reports:
        tone = decision_tone(report.report_decision or report.report_status)
        counts[tone] += 1

        if report.valid_from and report.valid_from >= recent_cutoff:
            recent_counts[tone] += 1

        if not report.valid_from:
            continue

        year_bucket = report_years.setdefault(
            report.valid_from.year,
            {
                "year": report.valid_from.year,
                "total": 0,
                "institutional": 0,
                "programme": 0,
                "monitoring": 0,
                "other": 0,
                "conditional": 0,
                "negative": 0,
            },
        )
        scope = classify_report_scope(report)
        year_bucket["total"] += 1
        year_bucket[scope] += 1
        if tone == "conditional":
            year_bucket["conditional"] += 1
        if tone == "negative":
            year_bucket["negative"] += 1

    report_year_rows = [report_years[year] for year in sorted(report_years, reverse=True)]
    lead_report = next(
        (
            report
            for report in reports
            if classify_report_scope(report) == "institutional" and is_current_report(report)
        ),
        None,
    )
    if not lead_report:
        lead_report = next((report for report in reports if classify_report_scope(report) == "institutional"), None)
    if not lead_report:
        lead_report = next((report for report in reports if is_current_report(report)), None)
    if not lead_report:
        lead_report = reports[0] if reports else None

    return {
        "anchor_decision_tone": decision_tone(
            (lead_report.report_decision or lead_report.report_status) if lead_report else None
        ),
        "positive_decision_count": counts["positive"],
        "conditional_decision_count": counts["conditional"],
        "negative_decision_count": counts["negative"],
        "recent_window_years": 5,
        "recent_conditional_decision_count": recent_counts["conditional"],
        "recent_negative_decision_count": recent_counts["negative"],
        "report_years": report_year_rows,
    }


def summarize_institutional_risk(reports: list[DeqarReportRecord]) -> dict[str, object]:
    today = date.today()
    institutional_reports = [report for report in reports if classify_report_scope(report) == "institutional"]
    dated_institutional_reports = [report for report in institutional_reports if report.valid_from]

    current_institutional = next((report for report in institutional_reports if is_current_report(report)), None)
    anchor_report = current_institutional or (institutional_reports[0] if institutional_reports else None)
    anchor_dated_report = current_institutional or (dated_institutional_reports[0] if dated_institutional_reports else None)

    age_days = (
        max(0, (today - anchor_dated_report.valid_from).days)
        if anchor_dated_report and anchor_dated_report.valid_from
        else None
    )

    dated_review_dates = [report.valid_from for report in dated_institutional_reports if report.valid_from]
    intervals = [
        (earlier - later).days
        for earlier, later in zip(dated_review_dates, dated_review_dates[1:], strict=False)
        if earlier and later
    ]
    average_interval_days = round(sum(intervals) / len(intervals)) if intervals else None

    listed_valid_to = current_institutional.valid_to if current_institutional and current_institutional.valid_to else (
        anchor_report.valid_to if anchor_report else None
    )
    listed_days_remaining = (listed_valid_to - today).days if listed_valid_to else None

    if not anchor_report:
        validity_status = "no_institutional_review"
        validity_label = "No institutional review is visible in this snapshot."
    elif current_institutional and current_institutional.valid_to:
        if listed_days_remaining is not None and listed_days_remaining <= 365:
            validity_status = "expires_within_12_months"
            validity_label = "Current institutional validity is listed to end within 12 months."
        elif listed_days_remaining is not None and listed_days_remaining <= 730:
            validity_status = "expires_within_24_months"
            validity_label = "Current institutional validity is listed to end within 24 months."
        else:
            validity_status = "active"
            validity_label = "Current institutional validity is not near its listed end date."
    elif current_institutional:
        validity_status = "active_open_ended"
        validity_label = "A current institutional review exists, but no validity end date is listed."
    elif anchor_report.valid_to and anchor_report.valid_to < today:
        validity_status = "expired"
        validity_label = "The latest listed institutional validity window has already ended."
    else:
        validity_status = "historical_only"
        validity_label = "Only historical institutional review coverage is visible in this snapshot."

    return {
        "anchor_institutional_decision_date": anchor_dated_report.valid_from.isoformat()
        if anchor_dated_report and anchor_dated_report.valid_from
        else None,
        "institutional_valid_to": listed_valid_to.isoformat() if listed_valid_to else None,
        "institutional_review_age_days": age_days,
        "institutional_review_avg_interval_days": average_interval_days,
        "institutional_validity_status": validity_status,
        "institutional_validity_label": validity_label,
        "institutional_days_remaining": listed_days_remaining,
    }


def summarize_quality_risk(
    institutional_risk: dict[str, object],
    decision_analytics: dict[str, object],
) -> dict[str, object]:
    validity_status = str(institutional_risk.get("institutional_validity_status") or "")
    recent_negative_count = int(decision_analytics.get("recent_negative_decision_count") or 0)
    recent_conditional_count = int(decision_analytics.get("recent_conditional_decision_count") or 0)

    if recent_negative_count > 0 or validity_status in {"expired", "expires_within_12_months", "no_institutional_review"}:
        level = "high"
        if recent_negative_count > 0:
            summary = "Recent negative decisions are visible in the DEQAR history."
        elif validity_status == "no_institutional_review":
            summary = "No institutional review is visible in this snapshot, which weakens risk interpretation."
        elif validity_status == "expired":
            summary = "The latest listed institutional validity window has already ended."
        else:
            summary = "The listed institutional validity window ends within the next 12 months."
    elif recent_conditional_count > 0 or validity_status in {"expires_within_24_months", "historical_only"}:
        level = "medium"
        if recent_conditional_count > 0:
            summary = "Conditional decisions appear in the last five years of the DEQAR record."
        elif validity_status == "historical_only":
            summary = "Only historical institutional review coverage is visible in this snapshot."
        else:
            summary = "The listed institutional validity window ends within the next 24 months."
    else:
        level = "low"
        if validity_status == "active_open_ended":
            summary = "A current institutional review exists, but the snapshot lists no end date."
        else:
            summary = "No immediate institutional-expiry or negative-decision signal is visible in this snapshot."

    return {
        "qa_risk_level": level,
        "qa_risk_summary": summary,
    }


def classify_report_scope(report: DeqarReportRecord) -> str:
    activity = normalize_text(report.report_activity)
    report_type = normalize_text(report.report_type)

    if "monitor" in activity or "post accreditation" in activity or "pamc" in activity:
        return "monitoring"
    if report_type == "institutional" or "institutional" in activity:
        return "institutional"
    if report_type == "programme" or "programme" in activity or report.programme_name:
        return "programme"
    return "other"


def decision_tone(value: str | None) -> str:
    normalized = normalize_text(value)
    if not normalized:
        return "neutral"
    if any(token in normalized for token in ("negative", "withdrawn", "refused", "revoked", "denied")):
        return "negative"
    if any(token in normalized for token in ("condition", "restriction", "partially", "follow up")):
        return "conditional"
    if "positive" in normalized:
        return "positive"
    return "neutral"


def best_name_match(
    normalized_names: str | Iterable[str],
    candidates: Iterable[DeqarInstitutionRecord],
) -> DeqarInstitutionRecord | None:
    if isinstance(normalized_names, str):
        name_candidates = [normalized_names]
    else:
        name_candidates = [name for name in normalized_names if name]
    best_candidate: DeqarInstitutionRecord | None = None
    best_score = 0.0
    for normalized_name in name_candidates:
        for candidate in candidates:
            for normalized_candidate_name in candidate.normalized_names:
                if normalized_name == normalized_candidate_name:
                    return candidate
                if normalized_name in normalized_candidate_name or normalized_candidate_name in normalized_name:
                    score = 0.95
                else:
                    score = SequenceMatcher(a=normalized_name, b=normalized_candidate_name).ratio()
                if score > best_score:
                    best_score = score
                    best_candidate = candidate
    if best_score >= 0.92:
        return best_candidate
    return None


def resolve_identifier_candidates(
    candidates: list[DeqarInstitutionRecord],
    institution: InstitutionOption,
) -> DeqarInstitutionRecord | None:
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        return None

    host = normalize_host(institution.homepage_url)
    if host:
        host_matches = [candidate for candidate in candidates if candidate.website_host == host]
        if len(host_matches) == 1:
            return host_matches[0]

    return best_name_match(institution_normalized_names(institution), candidates)


def first_identifier_value(raw_identifiers: str | None, scheme: str) -> str | None:
    wanted_scheme = scheme.casefold()
    for raw_part in IDENTIFIER_SPLIT_PATTERN.split(raw_identifiers or ""):
        if ":" not in raw_part:
            continue
        raw_scheme, raw_value = raw_part.split(":", 1)
        if raw_scheme.casefold().strip() != wanted_scheme:
            continue
        cleaned = raw_value.strip()
        if cleaned:
            return cleaned
    return None


def match_summary_phrase(match: DeqarInstitutionMatch) -> str:
    if match.match_type == "ror":
        return f"the shared ROR identifier {match.matched_value}"
    if match.match_type == "eter_id":
        return f"the shared ETER identifier {match.matched_value}"
    if match.match_type == "deqar_id":
        return f"the explicit DEQAR identifier {match.matched_value}"
    if match.match_type == "website":
        return f"the website host {match.matched_value}"
    if match.match_type == "exact_name":
        return "an exact normalized institution name"
    if match.match_type == "exact_name_and_website":
        return "an exact institution name plus website host"
    if match.match_type == "website_and_fuzzy_name":
        return f"the website host {match.matched_value} plus a close institution-name variant"
    if match.match_type == "fuzzy_name":
        return "a close institution-name variant"
    return "a fallback institution-identity heuristic"


def attach_report(target: dict[str, list[DeqarReportRecord]], institution_id: str, report: DeqarReportRecord) -> None:
    reports = target.setdefault(institution_id, [])
    if any(existing.report_id == report.report_id for existing in reports):
        return
    reports.append(report)


def parse_int(value: str | None) -> int:
    try:
        return int((value or "").strip())
    except ValueError:
        return 0


def parse_date(value: str | None) -> date | None:
    raw_value = (value or "").strip()
    if not raw_value:
        return None
    try:
        return date.fromisoformat(raw_value)
    except ValueError:
        logger.debug("Could not parse DEQAR date %r", raw_value)
        return None


def extract_first_url(value: str | None) -> str | None:
    if not value:
        return None
    match = REPORT_FILE_URL_PATTERN.search(value)
    return match.group(1) if match else None


def report_sort_key(report: DeqarReportRecord) -> tuple[date, date, str]:
    return (
        report.valid_from or date.min,
        report.valid_to or date.max,
        report.report_id,
    )


def is_current_report(report: DeqarReportRecord) -> bool:
    today = date.today()
    if report.valid_from and report.valid_from > today:
        return False
    return report.valid_to is None or report.valid_to >= today


def titleize(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").strip().title()
