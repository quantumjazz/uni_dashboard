from __future__ import annotations

import logging
from typing import Any

import httpx

from backend.app.cache.repository import CacheRepository
from backend.app.clients.eurostat import EurostatClient
from backend.app.models.schemas import DataPoint, DataResponse, IndicatorDefinition
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
        return DataResponse(indicator=indicator, rows=rows, summary=summary, metadata=metadata)

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
    def _build_summary(indicator: IndicatorDefinition, rows: list[DataPoint], countries: list[str]) -> str | None:
        if not rows:
            return f"No data returned for {indicator.title}."

        latest_by_country: dict[str, DataPoint] = {}
        for row in rows:
            current = latest_by_country.get(row.country)
            if current is None or row.year > current.year:
                latest_by_country[row.country] = row

        focus_country = countries[0] if countries else "BG"
        focus = latest_by_country.get(focus_country)
        eu = latest_by_country.get("EU27_2020")
        if focus and eu and focus.country != eu.country:
            delta = focus.value - eu.value
            direction = "above" if delta >= 0 else "below"
            return f"{focus.country} is {abs(delta):.1f} {indicator.unit or ''} {direction} the EU benchmark in {focus.year}."
        if focus:
            return f"Latest value for {focus.country} is {focus.value:.1f} in {focus.year}."
        latest = max(rows, key=lambda row: row.year)
        return f"Latest observed value is {latest.value:.1f} for {latest.country} in {latest.year}."
