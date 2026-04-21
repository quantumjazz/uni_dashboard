from __future__ import annotations

import io
import json
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Any

import httpx

from backend.app.cache.repository import CacheRepository
from backend.app.config import get_settings


CORDIS_XML_NS = {"cordis": "http://cordis.europa.eu"}


class CordisClient:
    """Read-only client for completed CORDIS Data Extraction Tool tasks."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.base_url = self.settings.cordis_base_url.rstrip("/")
        self.api_key = self.settings.cordis_api_key
        self.cache = CacheRepository()

    def is_configured(self) -> bool:
        return bool(self.api_key)

    @staticmethod
    def institution_query(institution_name: str) -> str:
        cleaned_name = " ".join(institution_name.split())
        return f'"{cleaned_name}"'

    async def list_extractions(self) -> list[dict[str, Any]]:
        payload = await self._get_json("/listExtractions", {"key": self._require_key()})
        results = payload.get("payload", {}).get("result", [])
        if not isinstance(results, list):
            return []

        return results

    async def latest_extraction_for_query(self, query: str) -> dict[str, Any] | None:
        extractions = await self.list_query_extractions(query)
        if not extractions:
            return None

        return sorted(extractions, key=self._task_sort_key, reverse=True)[0]

    async def list_query_extractions(self, query: str) -> list[dict[str, Any]]:
        extractions = [
            extraction
            for extraction in await self.list_extractions()
            if self._normalize_query(str(extraction.get("query") or "")) == self._normalize_query(query)
        ]
        return sorted(extractions, key=self._task_sort_key, reverse=True)

    async def any_running_extraction(self) -> dict[str, Any] | None:
        extractions = await self.list_extractions()
        for extraction in sorted(extractions, key=self._task_sort_key, reverse=True):
            progress = str(extraction.get("progress") or "").lower()
            if progress and progress != "finished":
                return extraction
        return None

    async def create_extraction(self, query: str, output_format: str = "json", archived: bool = False) -> dict[str, Any]:
        payload = await self._get_json(
            "/getExtraction",
            {
                "query": query,
                "key": self._require_key(),
                "outputFormat": output_format,
                "archived": "true" if archived else "false",
            },
        )
        return payload.get("payload", {})

    async def latest_extraction_for_query_with_member(self, query: str, member_name: str) -> dict[str, Any] | None:
        for extraction in await self.list_query_extractions(query):
            progress = str(extraction.get("progress") or "").lower()
            if progress != "finished" or not extraction.get("destinationFileUri"):
                continue
            try:
                members = await self.get_outer_members(extraction)
            except Exception:
                continue
            if member_name in members:
                return extraction
        return None

    async def parse_summary(self, extraction: dict[str, Any]) -> dict[str, Any]:
        task_id = str(extraction.get("taskId") or extraction.get("taskID") or "")
        if not task_id:
            return {"content_type_counts": {}, "total_hits": self._to_int(extraction.get("numberOfRecords"))}

        cache_key = f"cordis:summary:{task_id}"
        cached = self.cache.get_cached_payload("api_cache", cache_key)
        if isinstance(cached, dict):
            return cached

        destination_uri = str(extraction.get("destinationFileUri") or "")
        if not destination_uri:
            return {"content_type_counts": {}, "total_hits": self._to_int(extraction.get("numberOfRecords"))}

        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            response = await client.get(destination_uri)
            response.raise_for_status()

        summary = self._parse_summary_archive(response.content)
        self.cache.set_cached_payload("api_cache", cache_key, summary)
        return summary

    async def parse_json_export(self, extraction: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        task_id = str(extraction.get("taskId") or extraction.get("taskID") or "")
        if not task_id:
            return {}

        cache_key = f"cordis:json_export:{task_id}"
        cached = self.cache.get_cached_payload("api_cache", cache_key)
        if isinstance(cached, dict):
            return cached

        archive_bytes = await self.download_extraction_archive(extraction)
        parsed = self._parse_json_archive(archive_bytes)
        self.cache.set_cached_payload("api_cache", cache_key, parsed)
        return parsed

    async def get_outer_members(self, extraction: dict[str, Any]) -> list[str]:
        task_id = str(extraction.get("taskId") or extraction.get("taskID") or "")
        if not task_id:
            return []

        cache_key = f"cordis:outer_members:{task_id}"
        cached = self.cache.get_cached_payload("api_cache", cache_key)
        if isinstance(cached, list):
            return cached

        archive_bytes = await self.download_extraction_archive(extraction)
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
            members = archive.namelist()
        self.cache.set_cached_payload("api_cache", cache_key, members)
        return members

    async def download_extraction_archive(self, extraction: dict[str, Any]) -> bytes:
        destination_uri = str(extraction.get("destinationFileUri") or "")
        if not destination_uri:
            raise ValueError("Extraction does not have a download URL yet.")

        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            response = await client.get(destination_uri)
            response.raise_for_status()
            return response.content

    async def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(f"{self.base_url}{path}", params=params)
            response.raise_for_status()
            return response.json()

    def _require_key(self) -> str:
        if not self.api_key:
            raise RuntimeError("CORDIS API key is not configured")
        return self.api_key

    @classmethod
    def _parse_summary_archive(cls, content: bytes) -> dict[str, Any]:
        with zipfile.ZipFile(io.BytesIO(content)) as outer:
            outer_members = outer.namelist()
            summary_archive_member = cls._find_archive_member(outer_members, "summary.zip")
            if summary_archive_member:
                with zipfile.ZipFile(io.BytesIO(outer.read(summary_archive_member))) as summary_zip:
                    return cls._parse_summary_members(summary_zip)

            try:
                return cls._parse_summary_members(outer)
            except ValueError as exc:
                raise KeyError("Extraction does not contain a supported summary export.") from exc

    @classmethod
    def _parse_summary_members(cls, archive: zipfile.ZipFile) -> dict[str, Any]:
        members = [member for member in archive.namelist() if not member.endswith("/")]

        xml_candidates = [member for member in members if member.lower().endswith(".xml")]
        for member in cls._sort_summary_members(xml_candidates):
            try:
                return cls._parse_summary_xml(archive.read(member))
            except (ET.ParseError, KeyError, ValueError):
                continue

        json_candidates = [member for member in members if member.lower().endswith(".json")]
        for member in cls._sort_summary_members(json_candidates):
            try:
                return cls._parse_summary_json(archive.read(member))
            except (json.JSONDecodeError, TypeError, ValueError):
                continue

        raise ValueError("No supported summary member found.")

    @classmethod
    def _parse_summary_xml(cls, content: bytes) -> dict[str, Any]:
        root = ET.fromstring(content)
        total_hits = cls._to_int(root.findtext(".//cordis:totalHits", namespaces=CORDIS_XML_NS))
        num_hits = cls._to_int(root.findtext(".//cordis:numHits", namespaces=CORDIS_XML_NS))
        counts: Counter[str] = Counter()
        examples: list[dict[str, str | None]] = []

        for hit in root.findall(".//cordis:hit", CORDIS_XML_NS):
            for child in list(hit):
                content_type = child.tag.split("}", 1)[-1]
                record_id = child.findtext("cordis:rcn", namespaces=CORDIS_XML_NS)
                counts[content_type] += 1
                if len(examples) < 8:
                    examples.append({"content_type": content_type, "rcn": record_id})
                break

        if total_hits is None and num_hits is None and not counts:
            raise ValueError("XML payload does not look like a summary export.")

        return {
            "total_hits": total_hits,
            "num_hits": num_hits,
            "content_type_counts": dict(counts),
            "example_records": examples,
        }

    @classmethod
    def _parse_summary_json(cls, content: bytes) -> dict[str, Any]:
        payload = json.loads(content)
        if isinstance(payload, list):
            entries = payload
        elif isinstance(payload, dict):
            entries = []
            for key in ("result", "results", "payload", "items", "data", "summary"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    entries = candidate
                    break
            if not entries and cls._is_summary_entry(payload):
                entries = [payload]
        else:
            entries = []

        summary_entries = [entry for entry in entries if isinstance(entry, dict) and cls._is_summary_entry(entry)]
        if not summary_entries:
            raise ValueError("JSON payload does not look like a summary export.")

        counts: Counter[str] = Counter()
        examples: list[dict[str, str | None]] = []
        for entry in summary_entries:
            content_type = str(entry.get("collection") or entry.get("contenttype") or entry.get("type") or "").strip()
            if content_type:
                counts[content_type] += 1
            if len(examples) < 8:
                record_id = str(entry.get("rcn") or "") or None
                examples.append({"content_type": content_type or None, "rcn": record_id})

        total_hits = cls._to_int(len(summary_entries))
        return {
            "total_hits": total_hits,
            "num_hits": total_hits,
            "content_type_counts": dict(counts),
            "example_records": examples,
        }

    @classmethod
    def _parse_json_archive(cls, content: bytes) -> dict[str, list[dict[str, Any]]]:
        with zipfile.ZipFile(io.BytesIO(content)) as outer:
            archive_members = outer.namelist()
            json_archive_member = cls._find_archive_member(archive_members, "json.zip")
            if json_archive_member:
                with zipfile.ZipFile(io.BytesIO(outer.read(json_archive_member))) as json_zip:
                    return cls._parse_json_members(json_zip)
            return cls._parse_json_members(outer)

    @classmethod
    def _parse_json_members(cls, archive: zipfile.ZipFile) -> dict[str, list[dict[str, Any]]]:
        parsed: dict[str, list[dict[str, Any]]] = {}
        record_files: list[dict[str, Any]] = []
        saw_record_export = False

        for member in archive.namelist():
            if member.endswith("/") or not member.lower().endswith(".json"):
                continue

            payload = json.loads(archive.read(member))
            key = cls._member_key(member)
            if cls._looks_like_legacy_collection(key, payload):
                parsed[key] = cls._coerce_to_list(payload)
                saw_record_export = True
                continue

            if cls._looks_like_summary_payload(payload):
                continue

            file_records = cls._coerce_record_items(payload, member)
            if file_records:
                saw_record_export = True
                record_files.extend(file_records)

        if record_files:
            parsed["_records"] = record_files
        if not saw_record_export:
            raise KeyError("Extraction does not contain a JSON record export.")
        return parsed

    @staticmethod
    def _coerce_to_list(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            return [payload]
        return []

    @classmethod
    def _coerce_record_items(cls, payload: Any, member_name: str) -> list[dict[str, Any]]:
        items = cls._coerce_to_list(payload)
        records: list[dict[str, Any]] = []
        for item in items:
            if cls._is_summary_entry(item):
                continue
            record = dict(item)
            record.setdefault("_member_name", member_name)
            records.append(record)
        return records

    @staticmethod
    def _find_archive_member(members: list[str], target_name: str) -> str | None:
        lowered_target = target_name.lower()
        for member in members:
            if member.lower() == lowered_target or member.lower().endswith(f"/{lowered_target}"):
                return member
        return None

    @staticmethod
    def _member_key(member: str) -> str:
        leaf = member.rsplit("/", 1)[-1]
        return leaf.rsplit(".", 1)[0].lower()

    @staticmethod
    def _sort_summary_members(members: list[str]) -> list[str]:
        return sorted(members, key=lambda member: ("summary" not in member.lower(), member.lower()))

    @staticmethod
    def _looks_like_legacy_collection(key: str, payload: Any) -> bool:
        legacy_keys = {
            "project",
            "organization",
            "topics",
            "topic",
            "programme",
            "program",
            "call",
            "result",
            "publication",
            "deliverable",
            "report",
            "patent",
        }
        return key in legacy_keys and isinstance(payload, (dict, list))

    @classmethod
    def _looks_like_summary_payload(cls, payload: Any) -> bool:
        if isinstance(payload, dict):
            if cls._is_summary_entry(payload):
                return True
            for key in ("result", "results", "payload", "items", "data", "summary"):
                candidate = payload.get(key)
                if isinstance(candidate, list) and candidate and all(cls._is_summary_entry(item) for item in candidate if isinstance(item, dict)):
                    return True
            return False

        if isinstance(payload, list):
            dict_items = [item for item in payload if isinstance(item, dict)]
            return bool(dict_items) and all(cls._is_summary_entry(item) for item in dict_items)

        return False

    @staticmethod
    def _is_summary_entry(payload: dict[str, Any]) -> bool:
        if not isinstance(payload, dict) or "rcn" not in payload:
            return False
        has_summary_shape = any(key in payload for key in ("collection", "language"))
        has_record_shape = any(key in payload for key in ("contenttype", "id", "title", "relations"))
        return has_summary_shape and not has_record_shape

    @staticmethod
    def _normalize_query(query: str) -> str:
        return " ".join(query.strip().split()).lower()

    @staticmethod
    def _task_sort_key(extraction: dict[str, Any]) -> int:
        return CordisClient._to_int(extraction.get("taskId") or extraction.get("taskID")) or 0

    @staticmethod
    def _to_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
