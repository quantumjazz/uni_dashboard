import csv
import json
import logging
from io import StringIO
from typing import Literal

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse

from backend.app.models.schemas import CountryOption, QualityReportAnalysisRequest, QualityReportThemeSummaryRequest
from backend.app.services.data_service import DataService
from backend.app.services.indicator_registry import IndicatorRegistry
from backend.app.services.page_registry import PageRegistry
from backend.app.services.quality_service import QualityService
from backend.app.services.quality_report_analysis import QualityReportAnalysisService
from backend.app.services.research_service import ResearchService

logger = logging.getLogger(__name__)


router = APIRouter()
data_service = DataService()
registry = IndicatorRegistry()
page_registry = PageRegistry()
research_service = ResearchService()
quality_service = QualityService()
quality_report_analysis_service = QualityReportAnalysisService()

COUNTRIES = [
    CountryOption(code="BG", label="Bulgaria"),
    CountryOption(code="EU27_2020", label="EU benchmark"),
    CountryOption(code="AT", label="Austria"),
    CountryOption(code="BE", label="Belgium"),
    CountryOption(code="CZ", label="Czechia"),
    CountryOption(code="DK", label="Denmark"),
    CountryOption(code="EE", label="Estonia"),
    CountryOption(code="FI", label="Finland"),
    CountryOption(code="FR", label="France"),
    CountryOption(code="DE", label="Germany"),
    CountryOption(code="EL", label="Greece"),
    CountryOption(code="HU", label="Hungary"),
    CountryOption(code="IE", label="Ireland"),
    CountryOption(code="IT", label="Italy"),
    CountryOption(code="LV", label="Latvia"),
    CountryOption(code="LT", label="Lithuania"),
    CountryOption(code="NL", label="Netherlands"),
    CountryOption(code="PL", label="Poland"),
    CountryOption(code="PT", label="Portugal"),
    CountryOption(code="RO", label="Romania"),
    CountryOption(code="SK", label="Slovakia"),
    CountryOption(code="SI", label="Slovenia"),
    CountryOption(code="ES", label="Spain"),
    CountryOption(code="SE", label="Sweden"),
    CountryOption(code="UK", label="United Kingdom"),
]


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/api/indicators")
async def list_indicators() -> list[dict]:
    return [indicator.model_dump() for indicator in registry.list()]


@router.get("/api/countries")
async def list_countries() -> list[dict]:
    return [country.model_dump() for country in COUNTRIES]


@router.get("/api/pages")
async def list_pages() -> list[dict]:
    return [page.model_dump() for page in page_registry.list()]


@router.get("/api/metadata")
async def metadata() -> dict:
    return registry.metadata()


@router.get("/api/institutions/search")
async def search_institutions(
    query: str = Query("", description="Institution search query"),
    mode: Literal["browse", "featured", "search"] = Query("browse", description="Institution search mode"),
) -> list[dict]:
    logger.info("GET /api/institutions/search query=%s mode=%s", query, mode)
    try:
        results = await research_service.search_institutions(query, mode=mode)
    except (httpx.HTTPStatusError, httpx.RequestError) as exc:
        logger.warning("Institution search failed: %s", exc)
        raise HTTPException(status_code=502, detail="Institution search is unavailable right now.") from exc
    return [item.model_dump() for item in results]


@router.get("/api/research/institutions/{institution_id}/summary")
async def get_research_summary(institution_id: str) -> dict:
    logger.info("GET /api/research/institutions/%s/summary", institution_id)
    try:
        result = await research_service.get_institution_summary(institution_id)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Unknown institution: {institution_id}") from exc
        logger.warning("Research summary lookup failed for %s: %s", institution_id, exc)
        raise HTTPException(status_code=502, detail="Research summary is unavailable right now.") from exc
    except httpx.RequestError as exc:
        logger.warning("Research summary request failed for %s: %s", institution_id, exc)
        raise HTTPException(status_code=502, detail="Research summary is unavailable right now.") from exc
    return result.model_dump()


@router.get("/api/projects/institutions/{institution_id}")
async def get_projects_status(institution_id: str) -> dict:
    logger.info("GET /api/projects/institutions/%s", institution_id)
    result = await research_service.get_projects_status(institution_id)
    return result.model_dump()


@router.post("/api/projects/institutions/{institution_id}/extract")
async def create_projects_extraction(institution_id: str) -> dict:
    logger.info("POST /api/projects/institutions/%s/extract", institution_id)
    result = await research_service.create_projects_extraction(institution_id)
    return result.model_dump()


@router.get("/api/quality/institutions/{institution_id}")
async def get_quality_status(
    institution_id: str,
    peer_mode: Literal["country", "regional", "global"] = Query("regional", description="Benchmark peer cohort mode"),
) -> dict:
    logger.info("GET /api/quality/institutions/%s peer_mode=%s", institution_id, peer_mode)
    result = await quality_service.get_institution_quality(institution_id, peer_mode=peer_mode)
    return result.model_dump()


@router.post("/api/quality/reports/analyze")
async def analyze_quality_report(request: QualityReportAnalysisRequest) -> dict:
    logger.info("POST /api/quality/reports/analyze report_id=%s", request.report_id)
    result = await quality_report_analysis_service.analyze_report(request)
    return result.model_dump()


@router.post("/api/quality/reports/theme-summary")
async def summarize_quality_report_themes(request: QualityReportThemeSummaryRequest) -> dict:
    logger.info(
        "POST /api/quality/reports/theme-summary institution_id=%s reports=%s peer_reports=%s",
        request.institution_id,
        len(request.reports),
        len(request.peer_reports),
    )
    result = await quality_report_analysis_service.summarize_themes(request)
    return result.model_dump()


@router.get("/api/data/batch")
async def get_data_batch(
    indicators: str = Query(..., description="Comma-separated indicator identifiers"),
    countries: str = Query("", description="Comma-separated country codes"),
    year_from: int | None = Query(None),
    year_to: int | None = Query(None),
) -> dict:
    indicator_ids = [indicator.strip() for indicator in indicators.split(",") if indicator.strip()]
    if not indicator_ids:
        raise HTTPException(status_code=400, detail="At least one indicator must be provided.")

    target_countries = [country.strip() for country in countries.split(",") if country.strip()]
    logger.info("GET /api/data/batch indicators=%s countries=%s", indicator_ids, target_countries)
    result = await data_service.get_many_indicator_data(indicator_ids, target_countries, year_from, year_to)
    return result.model_dump()


@router.get("/api/data")
async def get_data(
    indicator: str = Query(..., description="Indicator identifier"),
    countries: str = Query("", description="Comma-separated country codes"),
    year_from: int | None = Query(None),
    year_to: int | None = Query(None),
    format: str = Query("json", pattern="^(json|csv)$"),
):
    try:
        target_countries = [country.strip() for country in countries.split(",") if country.strip()]
        logger.info("GET /api/data indicator=%s countries=%s format=%s", indicator, target_countries, format)
        result = await data_service.get_indicator_data(indicator, target_countries, year_from, year_to)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown indicator: {indicator}") from exc

    if format == "csv":
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            ["source", "dataset", "indicator", "country", "year", "series_key", "series_label", "value", "unit", "note", "dimensions_json"]
        )
        for row in result.rows:
            writer.writerow(
                [
                    row.source,
                    row.dataset,
                    row.indicator,
                    row.country,
                    row.year,
                    row.series_key or "",
                    row.series_label or "",
                    row.value,
                    row.unit or "",
                    row.note or "",
                    json.dumps(row.dimensions),
                ]
            )
        return PlainTextResponse(buffer.getvalue(), media_type="text/csv")

    return result.model_dump()
