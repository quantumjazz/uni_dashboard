from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import hashlib
import importlib
import ipaddress
import logging
import re
import sys
import time
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

import httpx

from backend.app.models.schemas import (
    QualityReportAnalysisRequest,
    QualityReportAnalysisResponse,
    QualityReportFinding,
    QualityReportThemeSummaryRequest,
    QualityReportThemeSummaryResponse,
    QualityThemeRecurringItem,
    QualityThemeReportTarget,
    QualityThemeSummaryItem,
)


logger = logging.getLogger(__name__)

PDF_ACCEPT_HEADER = "application/pdf, text/html;q=0.9, */*;q=0.2"
PDF_MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024
PDF_ANALYSIS_CACHE_TTL_SECONDS = 6 * 60 * 60
PDF_ANALYSIS_TIMEOUT_SECONDS = 45.0
PDF_ANALYSIS_MAX_PAGES = 80
PDF_ANALYSIS_MAX_TEXT_CHARS = 350_000
THEME_SUMMARY_MAX_PRIMARY_REPORTS = 12
THEME_SUMMARY_MAX_PEER_REPORTS = 6
THEME_SUMMARY_CONCURRENCY = 4
HTML_PDF_LINK_PATTERN = re.compile(r"""href=["']([^"']+\.pdf(?:\?[^"']*)?)["']""", flags=re.IGNORECASE)
WHITESPACE_PATTERN = re.compile(r"\s+")
SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[.!?])\s+")
SECTION_SPLIT_PATTERN = re.compile(r"\n\s*\n+")

RECOMMENDATION_SIGNALS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("recommendation", re.compile(r"\brecommend(?:ation|ed|s)?\b", flags=re.IGNORECASE)),
    ("improvement", re.compile(r"\bimprov(?:e|ement|ing)\b", flags=re.IGNORECASE)),
    ("should", re.compile(r"\bshould\b", flags=re.IGNORECASE)),
    ("needs", re.compile(r"\bneeds?\s+to\b", flags=re.IGNORECASE)),
    ("ensure", re.compile(r"\bensure\b", flags=re.IGNORECASE)),
    ("strengthen", re.compile(r"\bstrengthen\b", flags=re.IGNORECASE)),
    ("enhance", re.compile(r"\benhance\b", flags=re.IGNORECASE)),
    ("develop", re.compile(r"\bdevelop\b", flags=re.IGNORECASE)),
)
CONDITION_SIGNALS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("condition", re.compile(r"\bcondition(?:al|s)?\b", flags=re.IGNORECASE)),
    ("restriction", re.compile(r"\brestriction(?:s)?\b", flags=re.IGNORECASE)),
    ("requirement", re.compile(r"\brequire(?:d|ment|ments)?\b", flags=re.IGNORECASE)),
    ("must", re.compile(r"\bmust\b", flags=re.IGNORECASE)),
    ("follow_up", re.compile(r"\bfollow[\s-]?up\b", flags=re.IGNORECASE)),
    ("subject_to", re.compile(r"\bsubject to\b", flags=re.IGNORECASE)),
)
SECTION_HINTS = {
    "recommendations",
    "recommendation",
    "conditions",
    "condition",
    "follow-up",
    "follow up",
    "areas for improvement",
    "recommendations for improvement",
}
THEME_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "id": "governance",
        "label": "Governance",
        "description": "Leadership, strategy, oversight, and institutional decision-making.",
        "patterns": (
            re.compile(r"\bgovernance\b", flags=re.IGNORECASE),
            re.compile(r"\bleadership\b", flags=re.IGNORECASE),
            re.compile(r"\bstrateg(?:y|ic)\b", flags=re.IGNORECASE),
            re.compile(r"\bmanagement\b", flags=re.IGNORECASE),
            re.compile(r"\bboard\b", flags=re.IGNORECASE),
            re.compile(r"\bcommittee\b", flags=re.IGNORECASE),
            re.compile(r"\bsenate\b", flags=re.IGNORECASE),
            re.compile(r"\bmission\b", flags=re.IGNORECASE),
        ),
    },
    {
        "id": "internal_qa",
        "label": "Internal QA",
        "description": "Internal quality assurance, evaluation, monitoring, and feedback loops.",
        "patterns": (
            re.compile(r"\bquality assurance\b", flags=re.IGNORECASE),
            re.compile(r"\binternal quality\b", flags=re.IGNORECASE),
            re.compile(r"\bquality culture\b", flags=re.IGNORECASE),
            re.compile(r"\bself[- ]evaluation\b", flags=re.IGNORECASE),
            re.compile(r"\bfeedback\b", flags=re.IGNORECASE),
            re.compile(r"\bmonitor(?:ing)?\b", flags=re.IGNORECASE),
            re.compile(r"\bevaluat(?:e|ion)\b", flags=re.IGNORECASE),
            re.compile(r"\bimprovement cycle\b", flags=re.IGNORECASE),
        ),
    },
    {
        "id": "staffing",
        "label": "Staffing",
        "description": "Academic staffing, workload, recruitment, and staff development.",
        "patterns": (
            re.compile(r"\bstaff(?:ing)?\b", flags=re.IGNORECASE),
            re.compile(r"\bacademic staff\b", flags=re.IGNORECASE),
            re.compile(r"\bfacult(?:y|ies)\b", flags=re.IGNORECASE),
            re.compile(r"\bteacher(?:s)?\b", flags=re.IGNORECASE),
            re.compile(r"\brecruit(?:ment)?\b", flags=re.IGNORECASE),
            re.compile(r"\bprofessional development\b", flags=re.IGNORECASE),
            re.compile(r"\bworkload\b", flags=re.IGNORECASE),
            re.compile(r"\bstaff development\b", flags=re.IGNORECASE),
        ),
    },
    {
        "id": "curriculum",
        "label": "Curriculum",
        "description": "Programme design, learning outcomes, assessment, and course structure.",
        "patterns": (
            re.compile(r"\bcurricul(?:um|a)\b", flags=re.IGNORECASE),
            re.compile(r"\bprogramme design\b", flags=re.IGNORECASE),
            re.compile(r"\blearning outcomes?\b", flags=re.IGNORECASE),
            re.compile(r"\bassessment\b", flags=re.IGNORECASE),
            re.compile(r"\bects\b", flags=re.IGNORECASE),
            re.compile(r"\bsyllab(?:us|i)\b", flags=re.IGNORECASE),
            re.compile(r"\bcourse(?:s)?\b", flags=re.IGNORECASE),
            re.compile(r"\bcompetenc(?:e|ies)\b", flags=re.IGNORECASE),
        ),
    },
    {
        "id": "student_support",
        "label": "Student support",
        "description": "Advising, wellbeing, inclusion, and support services for students.",
        "patterns": (
            re.compile(r"\bstudent support\b", flags=re.IGNORECASE),
            re.compile(r"\badvis(?:e|ing)\b", flags=re.IGNORECASE),
            re.compile(r"\bcounsell?ing\b", flags=re.IGNORECASE),
            re.compile(r"\bmentoring\b", flags=re.IGNORECASE),
            re.compile(r"\bwellbeing\b", flags=re.IGNORECASE),
            re.compile(r"\bcareer services?\b", flags=re.IGNORECASE),
            re.compile(r"\binclusion\b", flags=re.IGNORECASE),
            re.compile(r"\bstudent services?\b", flags=re.IGNORECASE),
        ),
    },
    {
        "id": "internationalization",
        "label": "Internationalization",
        "description": "Mobility, partnerships, and international student or staff experience.",
        "patterns": (
            re.compile(r"\binternationali[sz]ation\b", flags=re.IGNORECASE),
            re.compile(r"\binternational\b", flags=re.IGNORECASE),
            re.compile(r"\bmobility\b", flags=re.IGNORECASE),
            re.compile(r"\berasmus\b", flags=re.IGNORECASE),
            re.compile(r"\bexchange\b", flags=re.IGNORECASE),
            re.compile(r"\bforeign students?\b", flags=re.IGNORECASE),
            re.compile(r"\bglobal\b", flags=re.IGNORECASE),
        ),
    },
    {
        "id": "resources",
        "label": "Resources",
        "description": "Facilities, digital systems, libraries, and operational resources.",
        "patterns": (
            re.compile(r"\binfrastructure\b", flags=re.IGNORECASE),
            re.compile(r"\bfacilities\b", flags=re.IGNORECASE),
            re.compile(r"\blibrar(?:y|ies)\b", flags=re.IGNORECASE),
            re.compile(r"\bdigital\b", flags=re.IGNORECASE),
            re.compile(r"\bequipment\b", flags=re.IGNORECASE),
            re.compile(r"\blearning resources?\b", flags=re.IGNORECASE),
            re.compile(r"\bfinancial resources?\b", flags=re.IGNORECASE),
            re.compile(r"\bcampus\b", flags=re.IGNORECASE),
        ),
    },
    {
        "id": "data_systems",
        "label": "Data and evidence",
        "description": "Indicators, data quality, information systems, and evidence use.",
        "patterns": (
            re.compile(r"\binformation system\b", flags=re.IGNORECASE),
            re.compile(r"\bmanagement information\b", flags=re.IGNORECASE),
            re.compile(r"\bindicators?\b", flags=re.IGNORECASE),
            re.compile(r"\bevidence\b", flags=re.IGNORECASE),
            re.compile(r"\bdata collection\b", flags=re.IGNORECASE),
            re.compile(r"\bdata quality\b", flags=re.IGNORECASE),
            re.compile(r"\bperformance data\b", flags=re.IGNORECASE),
            re.compile(r"\bbenchmark(?:ing)?\b", flags=re.IGNORECASE),
        ),
    },
)
THEME_BY_ID = {definition["id"]: definition for definition in THEME_DEFINITIONS}
OTHER_THEME_ID = "other"
OTHER_THEME_DEFINITION = {
    "id": OTHER_THEME_ID,
    "label": "Other recommendations",
    "description": "Excerpts that do not map cleanly to the current theme taxonomy.",
}


@dataclass(slots=True)
class ThemeFindingRecord:
    theme_id: str
    finding_type: str
    report_id: str
    institution_id: str
    institution_name: str | None
    page_number: int | None
    signal: str | None
    excerpt: str


@dataclass(slots=True)
class ThemeBucketAccumulator:
    theme_id: str
    label: str
    description: str | None
    total_count: int = 0
    recommendation_count: int = 0
    condition_count: int = 0
    report_ids: set[str] = field(default_factory=set)
    institution_ids: set[str] = field(default_factory=set)
    sample: ThemeFindingRecord | None = None


class QualityReportAnalysisService:
    """Downloads a report PDF on demand and extracts recommendation-like snippets."""

    def __init__(self) -> None:
        self._cache: dict[str, tuple[float, QualityReportAnalysisResponse]] = {}

    async def analyze_report(
        self,
        request: QualityReportAnalysisRequest,
    ) -> QualityReportAnalysisResponse:
        cache_key = self._cache_key(request)
        cached = self._cache.get(cache_key)
        if cached and (time.time() - cached[0]) < PDF_ANALYSIS_CACHE_TTL_SECONDS:
            return cached[1]

        result = await self._analyze_uncached(request)
        self._cache[cache_key] = (time.time(), result)
        return result

    async def summarize_themes(
        self,
        request: QualityReportThemeSummaryRequest,
    ) -> QualityReportThemeSummaryResponse:
        requested_report_count = len(request.reports)
        requested_peer_report_count = len(request.peer_reports)
        primary_targets, truncated_primary_count = prepare_theme_targets(request.reports, THEME_SUMMARY_MAX_PRIMARY_REPORTS)
        peer_targets, truncated_peer_count = prepare_theme_targets(request.peer_reports, THEME_SUMMARY_MAX_PEER_REPORTS)

        if not primary_targets:
            return QualityReportThemeSummaryResponse(
                status="unavailable",
                message="No linked PDF files are available in the current filtered report set.",
                requested_report_count=requested_report_count,
                requested_peer_report_count=requested_peer_report_count,
                metadata={
                    "institution_id": request.institution_id,
                    "institution_name": request.institution_name,
                    "peer_mode": request.peer_mode,
                    "filters": request.filters,
                    "truncated_report_count": truncated_primary_count,
                    "truncated_peer_report_count": truncated_peer_count,
                    "peer_comparison_available": False,
                },
            )

        analyzed_primary, analyzed_peer = await asyncio.gather(
            self._analyze_theme_targets(primary_targets),
            self._analyze_theme_targets(peer_targets),
        )

        return build_theme_summary_response(
            request=request,
            analyzed_primary=analyzed_primary,
            analyzed_peer=analyzed_peer,
            requested_report_count=requested_report_count,
            requested_peer_report_count=requested_peer_report_count,
            truncated_primary_count=truncated_primary_count,
            truncated_peer_count=truncated_peer_count,
        )

    async def _analyze_theme_targets(
        self,
        targets: list[QualityThemeReportTarget],
    ) -> list[tuple[QualityThemeReportTarget, QualityReportAnalysisResponse]]:
        semaphore = asyncio.Semaphore(THEME_SUMMARY_CONCURRENCY)

        async def analyze_target(target: QualityThemeReportTarget) -> tuple[QualityThemeReportTarget, QualityReportAnalysisResponse]:
            async with semaphore:
                analysis = await self.analyze_report(
                    QualityReportAnalysisRequest(
                        report_id=target.report_id,
                        report_url=target.report_url,
                        report_type=target.report_type,
                        scope=target.scope,
                        decision=target.decision,
                        agency=target.agency,
                    )
                )
                return target, analysis

        return await asyncio.gather(*(analyze_target(target) for target in targets))

    async def _analyze_uncached(
        self,
        request: QualityReportAnalysisRequest,
    ) -> QualityReportAnalysisResponse:
        source_url = (request.report_url or "").strip()
        if not source_url:
            return QualityReportAnalysisResponse(
                report_id=request.report_id,
                status="unavailable",
                message="No linked report file is available for this DEQAR record.",
                source_url=None,
            )

        validated_url = validate_remote_report_url(source_url)
        if not validated_url:
            return QualityReportAnalysisResponse(
                report_id=request.report_id,
                status="unavailable",
                message="The linked report URL is not suitable for PDF extraction.",
                source_url=source_url,
            )

        PdfReader = load_pypdf_reader()
        if PdfReader is None:
            return QualityReportAnalysisResponse(
                report_id=request.report_id,
                status="unavailable",
                message="PDF parsing is not available in the current runtime.",
                source_url=validated_url,
                metadata={"next_step": "Install pypdf in the app runtime or use the bundled desktop runtime."},
            )

        try:
            download = await download_report_pdf(validated_url)
        except httpx.HTTPError as exc:
            logger.warning("PDF download failed for %s: %s", validated_url, exc)
            return QualityReportAnalysisResponse(
                report_id=request.report_id,
                status="error",
                message="The linked report PDF could not be downloaded right now.",
                source_url=validated_url,
                metadata={"error": str(exc)},
            )

        if not download.get("pdf_bytes"):
            return QualityReportAnalysisResponse(
                report_id=request.report_id,
                status="unavailable",
                message=str(download.get("message") or "The linked report did not resolve to a PDF file."),
                source_url=validated_url,
                resolved_pdf_url=str(download.get("resolved_pdf_url") or validated_url),
                metadata={
                    "content_type": download.get("content_type"),
                    "html_fallback_used": bool(download.get("html_fallback_used")),
                },
            )

        try:
            page_texts, parser_metadata = extract_pdf_page_texts(download["pdf_bytes"], PdfReader)
        except Exception as exc:  # noqa: BLE001
            logger.warning("PDF text extraction failed for %s: %s", validated_url, exc)
            return QualityReportAnalysisResponse(
                report_id=request.report_id,
                status="error",
                message="The linked PDF was downloaded, but its text could not be extracted reliably.",
                source_url=validated_url,
                resolved_pdf_url=str(download.get("resolved_pdf_url") or validated_url),
                metadata={"error": str(exc)},
            )

        findings = analyze_report_page_texts(page_texts)
        recommendation_count = len(findings["recommendations"])
        condition_count = len(findings["conditions"])
        status = "active" if recommendation_count or condition_count else "ready"
        message = (
            f"Found {recommendation_count} recommendation-like excerpts and {condition_count} condition-like excerpts."
            if recommendation_count or condition_count
            else "PDF text was extracted, but no recommendation-style excerpts were detected with the current heuristics."
        )

        return QualityReportAnalysisResponse(
            report_id=request.report_id,
            status=status,
            message=message,
            source_url=validated_url,
            resolved_pdf_url=str(download.get("resolved_pdf_url") or validated_url),
            page_count=parser_metadata["page_count"],
            extracted_page_count=parser_metadata["extracted_page_count"],
            recommendation_count=recommendation_count,
            condition_count=condition_count,
            recommendations=findings["recommendations"],
            conditions=findings["conditions"],
            metadata={
                "content_type": download.get("content_type"),
                "download_size_bytes": download.get("download_size_bytes"),
                "html_fallback_used": bool(download.get("html_fallback_used")),
                "parser": parser_metadata["parser"],
                "pages_with_text": parser_metadata["pages_with_text"],
                "total_characters": parser_metadata["total_characters"],
            },
        )

    @staticmethod
    def _cache_key(request: QualityReportAnalysisRequest) -> str:
        digest = hashlib.sha1(f"{request.report_id}|{request.report_url}".encode("utf-8")).hexdigest()
        return digest


async def download_report_pdf(source_url: str) -> dict[str, Any]:
    async with httpx.AsyncClient(
        timeout=PDF_ANALYSIS_TIMEOUT_SECONDS,
        follow_redirects=True,
        headers={"Accept": PDF_ACCEPT_HEADER},
    ) as client:
        response = await stream_download(client, source_url)
        content_type = str(response["headers"].get("content-type") or "").split(";", 1)[0].strip().lower()
        payload = response["payload"]

        if is_pdf_payload(payload, content_type, source_url):
            return {
                "pdf_bytes": payload,
                "resolved_pdf_url": source_url,
                "content_type": content_type or "application/pdf",
                "download_size_bytes": len(payload),
                "html_fallback_used": False,
            }

        if "html" not in content_type:
            return {
                "pdf_bytes": None,
                "resolved_pdf_url": source_url,
                "content_type": content_type or None,
                "message": "The linked file is not a PDF document.",
                "html_fallback_used": False,
            }

        html = payload.decode("utf-8", errors="ignore")
        pdf_link = resolve_pdf_link_from_html(source_url, html)
        if not pdf_link:
            return {
                "pdf_bytes": None,
                "resolved_pdf_url": source_url,
                "content_type": content_type,
                "message": "The report page did not expose a direct PDF download link.",
                "html_fallback_used": True,
            }

        pdf_response = await stream_download(client, pdf_link)
        pdf_content_type = str(pdf_response["headers"].get("content-type") or "").split(";", 1)[0].strip().lower()
        pdf_payload = pdf_response["payload"]
        if not is_pdf_payload(pdf_payload, pdf_content_type, pdf_link):
            return {
                "pdf_bytes": None,
                "resolved_pdf_url": pdf_link,
                "content_type": pdf_content_type,
                "message": "The linked report page resolved to a non-PDF file.",
                "html_fallback_used": True,
            }

        return {
            "pdf_bytes": pdf_payload,
            "resolved_pdf_url": pdf_link,
            "content_type": pdf_content_type or "application/pdf",
            "download_size_bytes": len(pdf_payload),
            "html_fallback_used": True,
        }


async def stream_download(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    async with client.stream("GET", url) as response:
        response.raise_for_status()
        chunks: list[bytes] = []
        total = 0
        async for chunk in response.aiter_bytes():
            if not chunk:
                continue
            total += len(chunk)
            if total > PDF_MAX_DOWNLOAD_BYTES:
                raise httpx.HTTPError("Report PDF exceeds the maximum download size.")
            chunks.append(chunk)
        return {
            "payload": b"".join(chunks),
            "headers": response.headers,
        }


def extract_pdf_page_texts(pdf_bytes: bytes, PdfReader: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    reader = PdfReader(BytesIO(pdf_bytes))
    page_count = len(reader.pages)
    pages_with_text = 0
    total_characters = 0
    page_texts: list[dict[str, Any]] = []

    for index, page in enumerate(reader.pages[:PDF_ANALYSIS_MAX_PAGES], start=1):
        text = page.extract_text() or ""
        text = text.replace("\x00", " ")
        text = normalize_pdf_text(text)
        if not text:
            continue
        pages_with_text += 1
        total_characters += len(text)
        page_texts.append({"page_number": index, "text": text})
        if total_characters >= PDF_ANALYSIS_MAX_TEXT_CHARS:
            break

    return page_texts, {
        "parser": "pypdf",
        "page_count": page_count,
        "extracted_page_count": min(page_count, PDF_ANALYSIS_MAX_PAGES),
        "pages_with_text": pages_with_text,
        "total_characters": total_characters,
    }


def analyze_report_page_texts(page_texts: list[dict[str, Any]]) -> dict[str, list[QualityReportFinding]]:
    recommendation_hits: list[dict[str, Any]] = []
    condition_hits: list[dict[str, Any]] = []

    for page in page_texts:
        page_number = int(page["page_number"])
        text = str(page["text"] or "")
        blocks = iter_candidate_blocks(text)
        for block in blocks:
            recommendation_score, recommendation_signal = block_score(block, RECOMMENDATION_SIGNALS)
            condition_score, condition_signal = block_score(block, CONDITION_SIGNALS)
            heading_bonus = 1 if any(hint in block.casefold()[:140] for hint in SECTION_HINTS) else 0
            bullet_bonus = 1 if block.lstrip().startswith(("-", "*", "\u2022")) or re.match(r"^\(?\d+[\).]", block) else 0

            if recommendation_score > 0:
                recommendation_hits.append(
                    {
                        "excerpt": block,
                        "page_number": page_number,
                        "signal": recommendation_signal,
                        "score": recommendation_score + heading_bonus + bullet_bonus,
                    }
                )
            if condition_score > 0:
                condition_hits.append(
                    {
                        "excerpt": block,
                        "page_number": page_number,
                        "signal": condition_signal,
                        "score": condition_score + heading_bonus + bullet_bonus,
                    }
                )

    recommendations = build_finding_list(recommendation_hits, limit=6)
    conditions = build_finding_list(condition_hits, limit=5)
    return {
        "recommendations": recommendations,
        "conditions": conditions,
    }


def build_finding_list(raw_hits: list[dict[str, Any]], limit: int) -> list[QualityReportFinding]:
    deduped: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for hit in sorted(raw_hits, key=lambda item: (-int(item["score"]), int(item["page_number"]), len(str(item["excerpt"])))):
        excerpt = str(hit["excerpt"]).strip()
        key = normalized_excerpt_key(excerpt)
        if not excerpt or key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(hit)
        if len(deduped) >= limit:
            break

    return [
        QualityReportFinding(
            excerpt=truncate_excerpt(str(item["excerpt"])),
            page_number=int(item["page_number"]) if item.get("page_number") else None,
            signal=str(item["signal"]) if item.get("signal") else None,
        )
        for item in deduped
    ]


def prepare_theme_targets(
    targets: list[QualityThemeReportTarget],
    limit: int,
) -> tuple[list[QualityThemeReportTarget], int]:
    valid: list[QualityThemeReportTarget] = []
    seen_keys: set[str] = set()

    for target in sorted(targets, key=theme_target_sort_key, reverse=True):
        key = target.report_id or target.report_url or ""
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        if not target.report_url:
            continue
        valid.append(target)

    truncated_count = max(len(valid) - limit, 0)
    return valid[:limit], truncated_count


def theme_target_sort_key(target: QualityThemeReportTarget) -> tuple[str, str]:
    return (
        target.decision_date or "",
        target.report_id or target.report_url or "",
    )


def build_theme_summary_response(
    *,
    request: QualityReportThemeSummaryRequest,
    analyzed_primary: list[tuple[QualityThemeReportTarget, QualityReportAnalysisResponse]],
    analyzed_peer: list[tuple[QualityThemeReportTarget, QualityReportAnalysisResponse]],
    requested_report_count: int,
    requested_peer_report_count: int,
    truncated_primary_count: int,
    truncated_peer_count: int,
) -> QualityReportThemeSummaryResponse:
    primary_stats = collect_theme_statistics(analyzed_primary)
    peer_stats = collect_theme_statistics(analyzed_peer)
    peer_comparison_available = peer_stats["institution_count"] > 0

    theme_items = [
        build_theme_summary_item(
            bucket=bucket,
            analyzed_report_count=primary_stats["analyzed_report_count"],
            peer_bucket=peer_stats["theme_buckets"].get(bucket.theme_id),
            peer_institution_count=peer_stats["institution_count"],
        )
        for bucket in sorted_theme_buckets(primary_stats["theme_buckets"].values())
    ]
    recurring_recommendations = [
        build_theme_recurring_item(
            bucket=bucket,
            finding_type="recommendation",
            analyzed_report_count=primary_stats["analyzed_report_count"],
            peer_bucket=peer_stats["theme_buckets"].get(bucket.theme_id),
            peer_institution_count=peer_stats["institution_count"],
        )
        for bucket in sorted_theme_buckets(
            [bucket for bucket in primary_stats["theme_buckets"].values() if bucket.recommendation_count > 0],
            finding_type="recommendation",
        )[:5]
    ]
    recurring_conditions = [
        build_theme_recurring_item(
            bucket=bucket,
            finding_type="condition",
            analyzed_report_count=primary_stats["analyzed_report_count"],
            peer_bucket=peer_stats["theme_buckets"].get(bucket.theme_id),
            peer_institution_count=peer_stats["institution_count"],
        )
        for bucket in sorted_theme_buckets(
            [bucket for bucket in primary_stats["theme_buckets"].values() if bucket.condition_count > 0],
            finding_type="condition",
        )[:5]
    ]

    if primary_stats["analyzed_report_count"] == 0:
        status = "ready"
        message = "The filtered reports were queued for analysis, but none yielded extractable PDF text yet."
    elif primary_stats["reports_with_findings"] == 0:
        status = "ready"
        message = "The filtered reports were parsed, but the current heuristics did not detect recurring recommendation or condition themes."
    else:
        status = "active"
        message = (
            f"Analyzed {primary_stats['analyzed_report_count']} filtered reports and found {primary_stats['reports_with_findings']} "
            f"with recommendation or condition signals."
        )
        if peer_stats["institution_count"] > 0:
            message += f" Peer context is based on {peer_stats['institution_count']} analyzed institutional peer reviews."

    return QualityReportThemeSummaryResponse(
        status=status,
        message=message,
        requested_report_count=requested_report_count,
        analyzed_report_count=primary_stats["analyzed_report_count"],
        reports_with_findings=primary_stats["reports_with_findings"],
        requested_peer_report_count=requested_peer_report_count,
        analyzed_peer_report_count=peer_stats["analyzed_report_count"],
        peer_institutions_analyzed=peer_stats["institution_count"],
        recommendation_count=primary_stats["recommendation_count"],
        condition_count=primary_stats["condition_count"],
        themes=theme_items,
        recurring_recommendations=recurring_recommendations,
        recurring_conditions=recurring_conditions,
        metadata={
            "institution_id": request.institution_id,
            "institution_name": request.institution_name,
            "peer_mode": request.peer_mode,
            "filters": request.filters,
            "truncated_report_count": truncated_primary_count,
            "truncated_peer_report_count": truncated_peer_count,
            "peer_comparison_available": peer_comparison_available,
            "available_theme_count": len(theme_items),
            "unavailable_report_count": primary_stats["unavailable_report_count"],
            "error_report_count": primary_stats["error_report_count"],
            "peer_unavailable_report_count": peer_stats["unavailable_report_count"],
            "peer_error_report_count": peer_stats["error_report_count"],
        },
    )


def collect_theme_statistics(
    analyzed_reports: list[tuple[QualityThemeReportTarget, QualityReportAnalysisResponse]],
) -> dict[str, Any]:
    theme_buckets: dict[str, ThemeBucketAccumulator] = {}
    analyzed_report_ids: set[str] = set()
    institutions_analyzed: set[str] = set()
    reports_with_findings: set[str] = set()
    recommendation_count = 0
    condition_count = 0
    unavailable_report_count = 0
    error_report_count = 0

    for target, analysis in analyzed_reports:
        if analysis.status in {"unavailable"}:
            unavailable_report_count += 1
            continue
        if analysis.status in {"error"}:
            error_report_count += 1
            continue
        if analysis.status not in {"active", "ready"}:
            continue

        report_id = target.report_id or analysis.report_id
        if not report_id:
            continue

        institution_id = target.institution_id or report_id
        institution_name = target.institution_name
        analyzed_report_ids.add(report_id)
        institutions_analyzed.add(institution_id)

        report_had_findings = False
        for finding in analysis.recommendations:
            report_had_findings = True
            recommendation_count += 1
            record_theme_finding(
                theme_buckets=theme_buckets,
                finding_type="recommendation",
                report_id=report_id,
                institution_id=institution_id,
                institution_name=institution_name,
                finding=finding,
            )
        for finding in analysis.conditions:
            report_had_findings = True
            condition_count += 1
            record_theme_finding(
                theme_buckets=theme_buckets,
                finding_type="condition",
                report_id=report_id,
                institution_id=institution_id,
                institution_name=institution_name,
                finding=finding,
            )

        if report_had_findings:
            reports_with_findings.add(report_id)

    return {
        "theme_buckets": theme_buckets,
        "analyzed_report_count": len(analyzed_report_ids),
        "institution_count": len(institutions_analyzed),
        "reports_with_findings": len(reports_with_findings),
        "recommendation_count": recommendation_count,
        "condition_count": condition_count,
        "unavailable_report_count": unavailable_report_count,
        "error_report_count": error_report_count,
    }


def record_theme_finding(
    *,
    theme_buckets: dict[str, ThemeBucketAccumulator],
    finding_type: str,
    report_id: str,
    institution_id: str,
    institution_name: str | None,
    finding: QualityReportFinding,
) -> None:
    excerpt = str(finding.excerpt or "").strip()
    if not excerpt:
        return

    for theme_id in theme_ids_for_excerpt(excerpt):
        definition = THEME_BY_ID.get(theme_id, OTHER_THEME_DEFINITION)
        bucket = theme_buckets.setdefault(
            theme_id,
            ThemeBucketAccumulator(
                theme_id=theme_id,
                label=str(definition["label"]),
                description=str(definition.get("description") or ""),
            ),
        )
        bucket.total_count += 1
        if finding_type == "recommendation":
            bucket.recommendation_count += 1
        else:
            bucket.condition_count += 1
        bucket.report_ids.add(report_id)
        bucket.institution_ids.add(institution_id)
        if bucket.sample is None:
            bucket.sample = ThemeFindingRecord(
                theme_id=theme_id,
                finding_type=finding_type,
                report_id=report_id,
                institution_id=institution_id,
                institution_name=institution_name,
                page_number=finding.page_number,
                signal=finding.signal,
                excerpt=excerpt,
            )


def theme_ids_for_excerpt(excerpt: str) -> list[str]:
    matches: list[str] = []
    for definition in THEME_DEFINITIONS:
        if any(pattern.search(excerpt) for pattern in definition["patterns"]):
            matches.append(str(definition["id"]))
    return matches or [OTHER_THEME_ID]


def sorted_theme_buckets(
    buckets: Iterable[ThemeBucketAccumulator],
    finding_type: str | None = None,
) -> list[ThemeBucketAccumulator]:
    def bucket_count(bucket: ThemeBucketAccumulator) -> int:
        if finding_type == "recommendation":
            return bucket.recommendation_count
        if finding_type == "condition":
            return bucket.condition_count
        return bucket.total_count

    return sorted(
        list(buckets),
        key=lambda bucket: (
            -bucket_count(bucket),
            -len(bucket.report_ids),
            bucket.label,
        ),
    )


def build_theme_summary_item(
    *,
    bucket: ThemeBucketAccumulator,
    analyzed_report_count: int,
    peer_bucket: ThemeBucketAccumulator | None,
    peer_institution_count: int,
) -> QualityThemeSummaryItem:
    report_share = ratio(len(bucket.report_ids), analyzed_report_count)
    peer_share = ratio(len(peer_bucket.institution_ids), peer_institution_count) if peer_bucket else ratio(0, peer_institution_count)
    comparison_label, comparison_note = theme_comparison_note(report_share, peer_share, peer_institution_count > 0)
    sample = bucket.sample

    return QualityThemeSummaryItem(
        theme_id=bucket.theme_id,
        label=bucket.label,
        description=bucket.description,
        total_count=bucket.total_count,
        recommendation_count=bucket.recommendation_count,
        condition_count=bucket.condition_count,
        report_count=len(bucket.report_ids),
        institution_count=len(bucket.institution_ids),
        report_share=report_share,
        comparison_label=comparison_label,
        comparison_note=comparison_note,
        peer_institution_count=len(peer_bucket.institution_ids) if peer_bucket else 0,
        peer_institution_share=peer_share if peer_institution_count > 0 else None,
        sample_excerpt=sample.excerpt if sample else None,
        sample_page_number=sample.page_number if sample else None,
        sample_signal=sample.signal if sample else None,
        sample_report_id=sample.report_id if sample else None,
        sample_institution_name=sample.institution_name if sample else None,
    )


def build_theme_recurring_item(
    *,
    bucket: ThemeBucketAccumulator,
    finding_type: str,
    analyzed_report_count: int,
    peer_bucket: ThemeBucketAccumulator | None,
    peer_institution_count: int,
) -> QualityThemeRecurringItem:
    report_share = ratio(len(bucket.report_ids), analyzed_report_count)
    peer_share = ratio(len(peer_bucket.institution_ids), peer_institution_count) if peer_bucket else ratio(0, peer_institution_count)
    comparison_label, comparison_note = theme_comparison_note(report_share, peer_share, peer_institution_count > 0)
    sample = bucket.sample
    count = bucket.recommendation_count if finding_type == "recommendation" else bucket.condition_count

    return QualityThemeRecurringItem(
        theme_id=bucket.theme_id,
        label=bucket.label,
        finding_type=finding_type,
        count=count,
        report_count=len(bucket.report_ids),
        report_share=report_share,
        comparison_label=comparison_label,
        comparison_note=comparison_note,
        sample_excerpt=sample.excerpt if sample else None,
        sample_page_number=sample.page_number if sample else None,
        sample_signal=sample.signal if sample else None,
        sample_report_id=sample.report_id if sample else None,
        sample_institution_name=sample.institution_name if sample else None,
    )


def theme_comparison_note(
    primary_report_share: float | None,
    peer_institution_share: float | None,
    peer_comparison_available: bool,
) -> tuple[str | None, str | None]:
    if not peer_comparison_available or primary_report_share is None or peer_institution_share is None:
        return None, None

    primary = primary_report_share
    peer = peer_institution_share
    if primary > 0 and peer == 0:
        return (
            "Not detected in peers",
            f"Seen in {format_share(primary)} of analyzed reports here and not detected in the analyzed peer institutional reviews.",
        )

    gap = primary - peer
    if gap >= 0.34:
        return (
            "More visible than peers",
            f"Seen in {format_share(primary)} of analyzed reports here versus {format_share(peer)} of analyzed peer institutional reviews.",
        )
    if gap <= -0.34:
        return (
            "More common in peers",
            f"Seen in {format_share(primary)} of analyzed reports here versus {format_share(peer)} of analyzed peer institutional reviews.",
        )
    if primary > 0 and peer > 0:
        return (
            "Common across cohort",
            f"Seen in {format_share(primary)} of analyzed reports here and {format_share(peer)} of analyzed peer institutional reviews.",
        )
    return None, None


def ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def format_share(value: float) -> str:
    return f"{round(value * 100):.0f}%"


def iter_candidate_blocks(text: str) -> list[str]:
    candidates: list[str] = []
    paragraphs = [part.strip() for part in SECTION_SPLIT_PATTERN.split(text) if part.strip()]
    for paragraph in paragraphs:
        cleaned = normalize_pdf_text(paragraph)
        if 50 <= len(cleaned) <= 900:
            candidates.append(cleaned)
        elif len(cleaned) > 900:
            candidates.extend(
                sentence.strip()
                for sentence in SENTENCE_SPLIT_PATTERN.split(cleaned)
                if 50 <= len(sentence.strip()) <= 420
            )
    return candidates


def block_score(block: str, patterns: tuple[tuple[str, re.Pattern[str]], ...]) -> tuple[int, str | None]:
    score = 0
    signal = None
    for label, pattern in patterns:
        if pattern.search(block):
            score += 1
            signal = signal or label
    return score, signal


def normalize_pdf_text(text: str) -> str:
    normalized = text.replace("\r", "\n").replace("\t", " ")
    normalized = re.sub(r"[ ]{2,}", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def normalized_excerpt_key(text: str) -> str:
    lowered = text.casefold()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    return WHITESPACE_PATTERN.sub(" ", lowered).strip()


def truncate_excerpt(text: str, max_length: int = 360) -> str:
    cleaned = WHITESPACE_PATTERN.sub(" ", text).strip()
    if len(cleaned) <= max_length:
        return cleaned
    return cleaned[: max_length - 1].rstrip() + "…"


def validate_remote_report_url(value: str) -> str | None:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        return None
    host = (parsed.hostname or "").strip()
    if not host or host in {"localhost"} or host.endswith(".local"):
        return None
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return value
    if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
        return None
    return value


def resolve_pdf_link_from_html(base_url: str, html: str) -> str | None:
    match = HTML_PDF_LINK_PATTERN.search(html)
    if not match:
        return None
    candidate = urljoin(base_url, match.group(1))
    return validate_remote_report_url(candidate)


def is_pdf_payload(payload: bytes, content_type: str, url: str) -> bool:
    if payload[:4] == b"%PDF":
        return True
    if "pdf" in content_type:
        return True
    return urlparse(url).path.casefold().endswith(".pdf")


def load_pypdf_reader() -> Any | None:
    try:
        return importlib.import_module("pypdf").PdfReader
    except ModuleNotFoundError:
        pass

    for candidate in bundled_site_packages_candidates():
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
        try:
            return importlib.import_module("pypdf").PdfReader
        except ModuleNotFoundError:
            continue
    return None


def bundled_site_packages_candidates() -> list[Path]:
    root = Path.home() / ".cache" / "codex-runtimes" / "codex-primary-runtime" / "dependencies" / "python"
    if not root.exists():
        return []
    return sorted(root.glob("lib/python*/site-packages"))
