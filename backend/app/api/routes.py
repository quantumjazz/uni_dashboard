import csv
import logging
from io import StringIO

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse

from backend.app.models.schemas import CountryOption
from backend.app.services.data_service import DataService
from backend.app.services.indicator_registry import IndicatorRegistry

logger = logging.getLogger(__name__)


router = APIRouter()
data_service = DataService()
registry = IndicatorRegistry()

COUNTRIES = [
    CountryOption(code="BG", label="Bulgaria"),
    CountryOption(code="EU27_2020", label="EU average"),
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


@router.get("/api/metadata")
async def metadata() -> dict:
    return registry.metadata()


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
        writer.writerow(["source", "dataset", "indicator", "country", "year", "value", "unit", "note"])
        for row in result.rows:
            writer.writerow([row.source, row.dataset, row.indicator, row.country, row.year, row.value, row.unit or "", row.note or ""])
        return PlainTextResponse(buffer.getvalue(), media_type="text/csv")

    return result.model_dump()
