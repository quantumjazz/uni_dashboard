from __future__ import annotations

import logging
from datetime import date
from typing import Any
from urllib.parse import urlparse

import httpx

from backend.app.config import get_settings
from backend.app.models.schemas import InstitutionOption, ResearchInstitutionSummary, ResearchTrendPoint
from backend.app.services.institution_registry import (
    IdentifierAssertion,
    InstitutionRegistry,
    NameVariant,
)

logger = logging.getLogger(__name__)


class OpenAlexClient:
    """Thin async client for OpenAlex institution search and summaries."""

    def __init__(self, registry: InstitutionRegistry | None = None) -> None:
        self.settings = get_settings()
        self.base_url = self.settings.openalex_base_url.rstrip("/")
        self.registry = registry or InstitutionRegistry()

    async def search_institutions(self, query: str, per_page: int = 12) -> list[InstitutionOption]:
        return await self._list_institutions(
            params={
                "search": query.strip(),
                "per-page": per_page,
            },
        )

    async def browse_institutions(self, per_page: int = 12) -> list[InstitutionOption]:
        results = await self.list_institutions(
            per_page=min(max(per_page * 4, per_page), 48),
            sort="cited_by_count:desc",
        )
        if len(results) <= per_page:
            return results

        diversified: list[InstitutionOption] = []
        deferred: list[InstitutionOption] = []
        country_counts: dict[str, int] = {}

        for institution in results:
            country_code = institution.country_code or ""
            if country_code and country_counts.get(country_code, 0) >= 1:
                deferred.append(institution)
                continue
            diversified.append(institution)
            if country_code:
                country_counts[country_code] = country_counts.get(country_code, 0) + 1
            if len(diversified) >= per_page:
                return diversified[:per_page]

        for institution in deferred:
            country_code = institution.country_code or ""
            if country_code and country_counts.get(country_code, 0) >= 2:
                continue
            diversified.append(institution)
            if country_code:
                country_counts[country_code] = country_counts.get(country_code, 0) + 1
            if len(diversified) >= per_page:
                break

        return diversified[:per_page]

    async def list_institutions(
        self,
        *,
        filter_expression: str | None = None,
        sort: str | None = None,
        per_page: int = 25,
    ) -> list[InstitutionOption]:
        params: dict[str, Any] = {
            "per-page": per_page,
        }
        if filter_expression:
            params["filter"] = filter_expression
        if sort:
            params["sort"] = sort
        return await self._list_institutions(params=params)

    async def get_institution_option(self, institution_id: str) -> InstitutionOption:
        normalized_id = self._normalize_id(institution_id)
        payload = await self._get(f"/institutions/{normalized_id}")
        institution = self._parse_institution(payload)
        self._register_institution(institution, payload)
        return institution

    async def _list_institutions(self, params: dict[str, Any]) -> list[InstitutionOption]:
        payload = await self._get("/institutions", params=params)
        results: list[InstitutionOption] = []
        for item in payload.get("results", []):
            institution = self._parse_institution(item)
            self._register_institution(institution, item)
            results.append(institution)
        return results

    async def get_institution_summary(self, institution_id: str) -> ResearchInstitutionSummary:
        normalized_id = self._normalize_id(institution_id)
        payload = await self._get(f"/institutions/{normalized_id}")
        institution = self._parse_institution(payload)
        self._register_institution(institution, payload)
        counts_by_year = [self._parse_counts_point(item) for item in payload.get("counts_by_year", [])]
        counts_by_year.sort(key=lambda item: item.year)

        latest_complete_year = None
        current_year = date.today().year
        complete_years = [point.year for point in counts_by_year if point.year < current_year]
        if complete_years:
            latest_complete_year = max(complete_years)

        return ResearchInstitutionSummary(
            source="openalex",
            institution=institution,
            works_count=payload.get("works_count") or 0,
            cited_by_count=payload.get("cited_by_count") or 0,
            summary_stats=payload.get("summary_stats") or {},
            counts_by_year=counts_by_year,
            metadata={
                "latest_year": max((point.year for point in counts_by_year), default=None),
                "latest_complete_year": latest_complete_year,
                "updated_date": payload.get("updated_date"),
                "works_api_url": payload.get("works_api_url"),
            },
        )

    def _register_institution(
        self,
        institution: InstitutionOption,
        payload: dict[str, Any],
    ) -> None:
        """Register an OpenAlex institution into the crosswalk.

        Merges with any prior entry (from DEQAR, a future ETER seed, etc.) that shares
        a ROR identifier. OpenAlex doesn't carry ETER IDs, so ROR is the main bridge
        into DEQAR-registered rows; name/host cascades still live in DeqarDataset until
        the registry grows its own fuzzy cascade.
        """
        if not institution.country_code:
            return

        identifiers: list[IdentifierAssertion] = [
            IdentifierAssertion(
                scheme="openalex",
                value=institution.id,
                source="openalex",
                confidence=1.0,
            )
        ]
        normalized_ror = _normalize_ror(institution.ror)
        if normalized_ror:
            identifiers.append(
                IdentifierAssertion(
                    scheme="ror",
                    value=normalized_ror,
                    source="openalex",
                    confidence=1.0,
                )
            )
        ids_block = payload.get("ids") or {}
        wikidata_id = ids_block.get("wikidata")
        if wikidata_id:
            normalized_wikidata = wikidata_id.rstrip("/").split("/")[-1]
            if normalized_wikidata:
                identifiers.append(
                    IdentifierAssertion(
                        scheme="wikidata",
                        value=normalized_wikidata,
                        source="openalex",
                        confidence=0.99,
                    )
                )

        name_variants: list[NameVariant] = [
            NameVariant(variant=institution.display_name, source="openalex")
        ]
        for alias in institution.aliases:
            name_variants.append(NameVariant(variant=alias, source="openalex"))

        try:
            self.registry.register(
                canonical_name=institution.display_name,
                country_code=institution.country_code.upper(),
                identifiers=identifiers,
                name_variants=name_variants,
                website_host=_homepage_host(institution.homepage_url),
                institution_type=_clean_optional_text(payload.get("type")),
                source="openalex",
            )
        except Exception:  # noqa: BLE001
            logger.exception("Failed to register OpenAlex institution %s into crosswalk", institution.id)

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request_params = dict(params or {})
        if self.settings.openalex_contact_email:
            request_params["mailto"] = self.settings.openalex_contact_email

        url = f"{self.base_url}{path}"
        logger.info("OpenAlex GET %s params=%s", url, request_params)
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=request_params)
            response.raise_for_status()
            return response.json()

    @staticmethod
    def _normalize_id(institution_id: str) -> str:
        return institution_id.rstrip("/").split("/")[-1]

    @classmethod
    def _parse_institution(cls, payload: dict[str, Any]) -> InstitutionOption:
        ids = payload.get("ids") or {}
        display_name = payload.get("display_name") or "Unknown institution"
        return InstitutionOption(
            id=cls._normalize_id(payload.get("id", "")),
            display_name=display_name,
            country_code=payload.get("country_code"),
            works_count=payload.get("works_count"),
            cited_by_count=payload.get("cited_by_count"),
            homepage_url=payload.get("homepage_url"),
            ror=payload.get("ror") or ids.get("ror"),
            aliases=dedupe_names(
                [
                    *(payload.get("display_name_alternatives") or []),
                    *(payload.get("display_name_acronyms") or []),
                ],
                primary_name=display_name,
            ),
        )

    @staticmethod
    def _parse_counts_point(payload: dict[str, Any]) -> ResearchTrendPoint:
        works_count = int(payload.get("works_count") or 0)
        oa_works_count = int(payload.get("oa_works_count") or 0)
        return ResearchTrendPoint(
            year=int(payload.get("year")),
            works_count=works_count,
            cited_by_count=int(payload.get("cited_by_count") or 0),
            oa_works_count=oa_works_count,
            open_access_share=(oa_works_count / works_count * 100) if works_count > 0 else None,
        )


def _normalize_ror(value: str | None) -> str | None:
    candidate = (value or "").strip()
    if not candidate:
        return None
    if candidate.upper().startswith("ROR:"):
        candidate = candidate.split(":", 1)[1].strip()
    if "ror.org/" in candidate.casefold():
        parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
        path_parts = [part for part in parsed.path.split("/") if part]
        candidate = path_parts[-1] if path_parts else candidate
    return candidate.casefold().strip().strip("/") or None


def _homepage_host(homepage_url: str | None) -> str | None:
    if not homepage_url:
        return None
    candidate = homepage_url.strip()
    if not candidate:
        return None
    parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
    host = (parsed.netloc or parsed.path).casefold().strip("/")
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _clean_optional_text(value: Any) -> str | None:
    candidate = str(value or "").strip()
    return candidate or None


def dedupe_names(values: list[str], primary_name: str) -> list[str]:
    seen = {primary_name.casefold().strip()}
    deduped: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        cleaned = value.strip()
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
    return deduped
