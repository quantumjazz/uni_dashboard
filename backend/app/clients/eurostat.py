from __future__ import annotations

import logging
from collections import defaultdict
from itertools import product
from typing import Any

import httpx

from backend.app.config import get_settings
from backend.app.models.schemas import DataPoint, IndicatorDefinition

logger = logging.getLogger(__name__)


class EurostatClient:
    """Thin async client for Eurostat's JSON-stat API."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.base_url = self.settings.eurostat_base_url.rstrip("/")

    async def fetch_indicator(self, indicator: IndicatorDefinition, countries: list[str]) -> tuple[list[DataPoint], dict[str, Any]]:
        params = self._build_query_params(indicator, countries)
        url = f"{self.base_url}/{indicator.dataset}"
        logger.info("Fetching %s for countries=%s from %s", indicator.id, countries, url)
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
        rows = self._parse_dataset(payload, indicator)
        if indicator.aggregate_dimension:
            rows = self._aggregate_rows(rows, indicator.aggregate_dimension)
        logger.info("Parsed %d rows for %s", len(rows), indicator.id)
        return rows, payload

    def _build_query_params(self, indicator: IndicatorDefinition, countries: list[str]) -> dict[str, str | list[str]]:
        params: dict[str, str | list[str]] = {"format": "JSON"}
        for key, value in indicator.dimensions.items():
            params[key] = value
        params["geo"] = countries
        return params

    def _parse_dataset(self, payload: dict[str, Any], indicator: IndicatorDefinition) -> list[DataPoint]:
        ids = payload.get("id", [])
        sizes = payload.get("size", [])
        values = payload.get("value", {})
        dimensions = payload.get("dimension", {})

        categories = [dimensions[dimension_id]["category"]["index"] for dimension_id in ids]
        reverse_maps = []
        for category_index in categories:
            reverse_maps.append({position: label for label, position in category_index.items()})

        rows: list[DataPoint] = []
        for index_tuple in product(*[range(size) for size in sizes]):
            flat_index = self._flatten_index(index_tuple, sizes)
            value = values.get(str(flat_index))
            if value is None:
                continue

            dims = {ids[i]: reverse_maps[i][index_tuple[i]] for i in range(len(ids))}
            time_value = dims.get("time") or dims.get("TIME_PERIOD")
            country = dims.get("geo")
            if country is None or time_value is None:
                continue

            rows.append(
                DataPoint(
                    source=indicator.source,
                    dataset=indicator.dataset,
                    indicator=indicator.id,
                    country=country,
                    year=int(str(time_value)[:4]),
                    value=float(value),
                    unit=dims.get("unit", indicator.unit),
                    note=indicator.notes,
                )
            )
        return rows

    @staticmethod
    def _aggregate_rows(rows: list[DataPoint], dimension: str) -> list[DataPoint]:
        """Sum values across a dimension (e.g. individual ages) per country+year."""
        sums: dict[tuple[str, int], float] = defaultdict(float)
        template: dict[tuple[str, int], DataPoint] = {}
        for row in rows:
            key = (row.country, row.year)
            sums[key] += row.value
            if key not in template:
                template[key] = row
        return [
            template[key].model_copy(update={"value": sums[key]})
            for key in sorted(sums)
        ]

    @staticmethod
    def _flatten_index(index_tuple: tuple[int, ...], sizes: list[int]) -> int:
        flat_index = 0
        multiplier = 1
        for dimension_index in range(len(sizes) - 1, -1, -1):
            flat_index += index_tuple[dimension_index] * multiplier
            multiplier *= sizes[dimension_index]
        return flat_index
