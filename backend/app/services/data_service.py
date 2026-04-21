from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from backend.app.cache.repository import CacheRepository
from backend.app.clients.eurostat import EurostatClient
from backend.app.models.schemas import BatchDataError, BatchDataResponse, DataPoint, DataResponse, IndicatorDefinition
from backend.app.services.indicator_registry import IndicatorRegistry

logger = logging.getLogger(__name__)


class DataService:
    """Coordinates indicator lookup, cache usage, source fetches, and lightweight narratives."""

    def __init__(self) -> None:
        self.registry = IndicatorRegistry()
        self.cache = CacheRepository()
        self.eurostat_client = EurostatClient()

    async def get_indicator_data(
        self,
        indicator_id: str,
        countries: list[str],
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> DataResponse:
        indicator = self.registry.get(indicator_id)
        target_countries = countries or indicator.default_countries
        cached_rows = self.cache.get_indicator_rows(indicator_id, target_countries, year_from, year_to)
        cached_countries = {row.country for row in cached_rows}
        all_countries_present = cached_rows and all(c in cached_countries for c in target_countries)
        metadata: dict[str, Any] = {"cache": "hit" if all_countries_present else "miss"}

        rows = cached_rows if all_countries_present else []
        if not rows:
            try:
                rows = await self._refresh_indicator(indicator, target_countries)
                if year_from is not None or year_to is not None:
                    rows = self._filter_years(rows, year_from, year_to)
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                logger.warning("Eurostat fetch failed for %s: %s — trying stale cache", indicator_id, exc)
                rows = self.cache.get_indicator_rows(indicator_id, target_countries, year_from, year_to, allow_stale=True)
                if rows:
                    metadata["cache"] = "stale"
                    logger.info("Serving %d stale rows for %s", len(rows), indicator_id)
                else:
                    raise

        summary = self._build_summary(indicator, rows, target_countries)
        metadata.update(self._build_metadata(rows))
        return DataResponse(indicator=indicator, rows=rows, summary=summary, metadata=metadata)

    async def get_many_indicator_data(
        self,
        indicator_ids: list[str],
        countries: list[str],
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> BatchDataResponse:
        unique_indicator_ids = list(dict.fromkeys(indicator_ids))
        tasks = [
            self.get_indicator_data(indicator_id, countries, year_from, year_to)
            for indicator_id in unique_indicator_ids
        ]
        settled = await asyncio.gather(*tasks, return_exceptions=True)

        results: dict[str, DataResponse] = {}
        errors: dict[str, BatchDataError] = {}
        for indicator_id, result in zip(unique_indicator_ids, settled, strict=False):
            if isinstance(result, Exception):
                if isinstance(result, KeyError):
                    errors[indicator_id] = BatchDataError(message=f"Unknown indicator: {indicator_id}")
                else:
                    errors[indicator_id] = BatchDataError(message=str(result))
                continue
            results[indicator_id] = result

        return BatchDataResponse(
            results=results,
            errors=errors,
            metadata={
                "requested": len(unique_indicator_ids),
                "returned": len(results),
                "failed": len(errors),
            },
        )

    async def _refresh_indicator(self, indicator: IndicatorDefinition, countries: list[str]) -> list[DataPoint]:
        if indicator.source != "eurostat":
            raise NotImplementedError(f"Source {indicator.source} is not implemented yet.")
        rows, payload = await self.eurostat_client.fetch_indicator(indicator, countries)
        self.cache.replace_indicator_rows(indicator.id, rows)
        self.cache.set_cached_payload("api_cache", indicator.id, payload)
        return rows

    @staticmethod
    def _filter_years(rows: list[DataPoint], year_from: int | None, year_to: int | None) -> list[DataPoint]:
        filtered = rows
        if year_from is not None:
            filtered = [row for row in filtered if row.year >= year_from]
        if year_to is not None:
            filtered = [row for row in filtered if row.year <= year_to]
        return filtered

    @staticmethod
    def _build_metadata(rows: list[DataPoint]) -> dict[str, Any]:
        if not rows:
            return {"latest_year": None, "series_count": 0}
        series_keys = {row.series_key or "__base__" for row in rows}
        return {"latest_year": max(row.year for row in rows), "series_count": len(series_keys)}

    @staticmethod
    def _build_summary(indicator: IndicatorDefinition, rows: list[DataPoint], countries: list[str]) -> str | None:
        if not rows:
            return f"No data returned for {indicator.title}."

        if indicator.breakdown_dimension:
            focus_country = countries[0] if countries else "BG"
            focus_rows = [row for row in rows if row.country == focus_country]
            target_rows = focus_rows or rows
            focus_country = focus_country if focus_rows else target_rows[0].country
            latest_year = max(row.year for row in target_rows)
            latest_rows = [row for row in target_rows if row.year == latest_year]
            series_count = len({row.series_key or "__base__" for row in latest_rows})
            return f"{series_count} {indicator.breakdown_dimension} series available for {focus_country} in {latest_year}."

        latest_by_country: dict[str, DataPoint] = {}
        for row in rows:
            current = latest_by_country.get(row.country)
            if current is None or row.year > current.year:
                latest_by_country[row.country] = row

        focus_country = countries[0] if countries else "BG"
        focus = latest_by_country.get(focus_country)
        eu = latest_by_country.get("EU27_2020")
        if focus and indicator.unit == "persons" and len(latest_by_country) > 1:
            return f"Latest value for {focus.country} is {focus.value:.1f} in {focus.year}. Use absolute counts as system-size context."
        if focus and eu and focus.country != eu.country:
            delta = focus.value - eu.value
            direction = "above" if delta >= 0 else "below"
            return f"{focus.country} is {abs(delta):.1f} {indicator.unit or ''} {direction} the EU benchmark in {focus.year}."
        if focus:
            return f"Latest value for {focus.country} is {focus.value:.1f} in {focus.year}."
        latest = max(rows, key=lambda row: row.year)
        return f"Latest observed value is {latest.value:.1f} for {latest.country} in {latest.year}."
