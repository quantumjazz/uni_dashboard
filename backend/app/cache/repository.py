import json
import logging
from datetime import UTC, datetime, timedelta

from backend.app.cache.database import get_connection
from backend.app.config import get_settings
from backend.app.models.schemas import DataPoint

logger = logging.getLogger(__name__)


class CacheRepository:
    """SQLite-backed storage for normalized observations and raw API payloads."""

    def __init__(self) -> None:
        self.settings = get_settings()

    def _is_fresh(self, fetched_at: str) -> bool:
        ttl = timedelta(hours=self.settings.cache_ttl_hours)
        return datetime.fromisoformat(fetched_at) >= datetime.now(UTC) - ttl

    def get_cached_payload(self, table: str, cache_key: str) -> dict | list | None:
        with get_connection() as conn:
            row = conn.execute(
                f"SELECT payload, fetched_at FROM {table} WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
        if not row or not self._is_fresh(row["fetched_at"]):
            return None
        return json.loads(row["payload"])

    def set_cached_payload(self, table: str, cache_key: str, payload: dict | list) -> None:
        with get_connection() as conn:
            conn.execute(
                f"""
                INSERT INTO {table} (cache_key, payload, fetched_at)
                VALUES (?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    payload = excluded.payload,
                    fetched_at = excluded.fetched_at
                """,
                (cache_key, json.dumps(payload), datetime.now(UTC).isoformat()),
            )
            conn.commit()

    def get_indicator_rows(
        self,
        indicator_id: str,
        countries: list[str],
        year_from: int | None,
        year_to: int | None,
        allow_stale: bool = False,
    ) -> list[DataPoint]:
        clauses = ["indicator = ?"]
        params: list[object] = [indicator_id]
        if countries:
            placeholders = ",".join("?" for _ in countries)
            clauses.append(f"country IN ({placeholders})")
            params.extend(countries)
        if year_from is not None:
            clauses.append("year >= ?")
            params.append(year_from)
        if year_to is not None:
            clauses.append("year <= ?")
            params.append(year_to)

        query = f"""
            SELECT
                source,
                dataset,
                indicator,
                country,
                year,
                series_key,
                series_label,
                value,
                unit,
                note,
                dimensions_json,
                fetched_at
            FROM data_points
            WHERE {' AND '.join(clauses)}
            ORDER BY year, country, series_key
        """
        with get_connection() as conn:
            rows = conn.execute(query, params).fetchall()

        if not rows:
            logger.debug("Cache miss for %s", indicator_id)
            return []
        if not allow_stale and not self._is_fresh(rows[0]["fetched_at"]):
            logger.debug("Cache stale for %s (fetched_at=%s)", indicator_id, rows[0]["fetched_at"])
            return []
        logger.debug("Cache hit for %s: %d rows (allow_stale=%s)", indicator_id, len(rows), allow_stale)
        return [
            DataPoint(
                source=row["source"],
                dataset=row["dataset"],
                indicator=row["indicator"],
                country=row["country"],
                year=row["year"],
                series_key=row["series_key"] or None,
                series_label=row["series_label"],
                value=row["value"],
                unit=row["unit"],
                note=row["note"],
                dimensions=json.loads(row["dimensions_json"] or "{}"),
            )
            for row in rows
        ]

    def replace_indicator_rows(self, indicator_id: str, rows: list[DataPoint]) -> None:
        now = datetime.now(UTC).isoformat()
        with get_connection() as conn:
            conn.execute("DELETE FROM data_points WHERE indicator = ?", (indicator_id,))
            conn.executemany(
                """
                INSERT INTO data_points (
                    source,
                    dataset,
                    indicator,
                    country,
                    year,
                    series_key,
                    series_label,
                    value,
                    unit,
                    note,
                    dimensions_json,
                    fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.source,
                        row.dataset,
                        row.indicator,
                        row.country,
                        row.year,
                        row.series_key or "",
                        row.series_label,
                        row.value,
                        row.unit,
                        row.note,
                        json.dumps(row.dimensions),
                        now,
                    )
                    for row in rows
                ],
            )
            conn.commit()
