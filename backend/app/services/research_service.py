from __future__ import annotations

import asyncio
import logging
from collections import Counter
from datetime import date

import httpx

from backend.app.clients.cordis import CordisClient
from backend.app.clients.openalex import OpenAlexClient
from backend.app.config import get_settings
from backend.app.models.schemas import CordisProjectRecord, CordisProjectsResponse, InstitutionOption, ResearchInstitutionSummary


logger = logging.getLogger(__name__)


class ResearchService:
    """Coordinates institution-level research sources and gated project status."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.openalex_client = OpenAlexClient()
        self.cordis_client = CordisClient()

    async def search_institutions(self, query: str | None = None, mode: str = "browse") -> list[InstitutionOption]:
        normalized_query = (query or "").strip()
        if normalized_query:
            return await self.openalex_client.search_institutions(query=normalized_query)
        if mode == "featured":
            return await self.get_featured_institutions()
        return await self.openalex_client.browse_institutions()

    async def get_featured_institutions(self) -> list[InstitutionOption]:
        featured_ids = self.settings.research_featured_institution_ids
        settled = await asyncio.gather(
            *(self.openalex_client.get_institution_option(institution_id) for institution_id in featured_ids),
            return_exceptions=True,
        )

        options = [
            result
            for result in settled
            if isinstance(result, InstitutionOption)
        ]
        order = {institution_id: index for index, institution_id in enumerate(featured_ids)}
        options.sort(key=lambda option: order.get(option.id, len(order)))

        default_id = self.settings.research_default_institution_id
        return sorted(options, key=lambda option: option.id != default_id)

    async def get_institution_summary(self, institution_id: str) -> ResearchInstitutionSummary:
        return await self.openalex_client.get_institution_summary(institution_id)

    async def get_projects_status(self, institution_id: str) -> CordisProjectsResponse:
        institution = await self._get_institution_option(institution_id)
        query = self.cordis_client.institution_query(institution.display_name)

        if not self.cordis_client.is_configured():
            return CordisProjectsResponse(
                source="cordis",
                status="blocked_by_credentials",
                message="CORDIS API key is not configured yet, so EU project participation is blocked for now.",
                institution_id=institution_id,
                projects=[],
                metadata={
                    "institution_name": institution.display_name,
                    "query": query,
                    "next_step": "Add CORDIS credentials before enabling project counts, partners, and EU contribution.",
                },
            )

        try:
            query_extractions = await self.cordis_client.list_query_extractions(query)
        except (httpx.HTTPError, RuntimeError) as exc:
            logger.warning("CORDIS extraction list failed for %s: %s", institution_id, exc)
            return CordisProjectsResponse(
                source="cordis",
                status="unavailable",
                message="CORDIS credentials are configured, but the extraction list could not be loaded right now.",
                institution_id=institution_id,
                projects=[],
                metadata={
                    "institution_name": institution.display_name,
                    "query": query,
                    "next_step": "Retry after CORDIS is reachable; no dashboard refresh will create duplicate extraction tasks.",
                },
            )

        latest_extraction = query_extractions[0] if query_extractions else None
        if latest_extraction:
            progress = str(latest_extraction.get("progress") or "Unknown")
            if progress.lower() != "finished" or not latest_extraction.get("destinationFileUri"):
                task_id = str(latest_extraction.get("taskId") or latest_extraction.get("taskID") or "")
                return CordisProjectsResponse(
                    source="cordis",
                    status="processing",
                    message=f"CORDIS extraction for {institution.display_name} is still processing.",
                    institution_id=institution_id,
                    projects=[],
                    metadata={
                        "institution_name": institution.display_name,
                        "query": query,
                        "task_id": task_id,
                        "progress": progress,
                        "record_count": CordisClient._to_int(latest_extraction.get("numberOfRecords")),
                        "estimated_record_count": CordisClient._to_int(latest_extraction.get("numberOfRecordsEstimated")),
                        "processed_record_count": CordisClient._to_int(latest_extraction.get("numberOfProcessedRecords")),
                        "next_step": "Refresh after the extraction finishes; the dashboard does not create duplicate extraction tasks automatically.",
                    },
                )

        if not latest_extraction:
            return CordisProjectsResponse(
                source="cordis",
                status="ready",
                message=(
                    f"CORDIS credentials are configured, but no completed extraction is available yet "
                    f"for {institution.display_name}."
                ),
                institution_id=institution_id,
                projects=[],
                metadata={
                    "institution_name": institution.display_name,
                    "query": query,
                    "next_step": "Start a CORDIS JSON extraction for this institution query, then refresh the dashboard.",
                },
            )

        try:
            json_export = await self.cordis_client.parse_json_export(latest_extraction)
        except KeyError:
            summary_metadata: dict[str, object] = {}
            try:
                summary = await self.cordis_client.parse_summary(latest_extraction)
                type_counts = summary.get("content_type_counts", {})
                summary_metadata = {
                    "record_count": summary.get("total_hits"),
                    "project_record_count": int(type_counts.get("project") or 0) + int(type_counts.get("archive_project") or 0),
                    "content_type_counts": type_counts,
                }
            except Exception as exc:  # noqa: BLE001
                logger.warning("CORDIS summary fallback parsing failed for %s: %s", institution_id, exc)

            return CordisProjectsResponse(
                source="cordis",
                status="ready",
                message=(
                    f"CORDIS has a completed extraction for {institution.display_name}, but project-level detail is not "
                    f"available until a JSON export is prepared."
                ),
                institution_id=institution_id,
                projects=[],
                metadata={
                    "institution_name": institution.display_name,
                    "query": query,
                    "next_step": "Start a CORDIS JSON extraction for this institution query to load project-level detail.",
                    **summary_metadata,
                },
            )
        except Exception as exc:  # noqa: BLE001
            task_id = str(latest_extraction.get("taskId") or latest_extraction.get("taskID") or "")
            logger.warning("CORDIS JSON parsing failed for task %s: %s", task_id, exc)
            return CordisProjectsResponse(
                source="cordis",
                status="unavailable",
                message="CORDIS JSON extraction is finished, but its generated export could not be parsed right now.",
                institution_id=institution_id,
                projects=[],
                metadata={
                    "institution_name": institution.display_name,
                    "query": query,
                    "task_id": task_id,
                    "progress": str(latest_extraction.get("progress") or "Unknown"),
                    "next_step": "Retry after CORDIS refreshes the extraction download link.",
                },
            )

        return self._build_projects_response(
            institution=institution,
            query=query,
            extraction=latest_extraction,
            json_export=json_export,
        )

    async def create_projects_extraction(self, institution_id: str) -> CordisProjectsResponse:
        institution = await self._get_institution_option(institution_id)
        query = self.cordis_client.institution_query(institution.display_name)

        if not self.cordis_client.is_configured():
            return await self.get_projects_status(institution_id)

        try:
            current = await self.get_projects_status(institution_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("CORDIS preflight status failed for %s: %s", institution_id, exc)
            current = None

        if current and current.status in {"active", "processing"}:
            return current

        running = await self.cordis_client.any_running_extraction()
        if running:
            running_query = str(running.get("query") or "").strip() or "another query"
            return CordisProjectsResponse(
                source="cordis",
                status="processing",
                message="CORDIS already has another extraction in progress, so the project-detail export was not started yet.",
                institution_id=institution_id,
                projects=[],
                metadata={
                    "institution_name": institution.display_name,
                    "query": query,
                    "task_id": str(running.get("taskId") or running.get("taskID") or ""),
                    "progress": str(running.get("progress") or "Unknown"),
                    "running_query": running_query,
                    "next_step": "Wait for the current CORDIS extraction to finish, then try again.",
                },
            )

        try:
            payload = await self.cordis_client.create_extraction(query=query, output_format="json", archived=False)
        except (httpx.HTTPError, RuntimeError) as exc:
            logger.warning("CORDIS extraction start failed for %s: %s", institution_id, exc)
            return CordisProjectsResponse(
                source="cordis",
                status="unavailable",
                message="CORDIS could not start the JSON extraction right now.",
                institution_id=institution_id,
                projects=[],
                metadata={
                    "institution_name": institution.display_name,
                    "query": query,
                    "next_step": "Retry the extraction start after CORDIS becomes reachable again.",
                },
            )

        task_id = str(payload.get("taskId") or payload.get("taskID") or "")
        return CordisProjectsResponse(
            source="cordis",
            status="processing",
            message=(
                f"Started a CORDIS JSON extraction for {institution.display_name}. "
                f"Refresh after the task finishes to load project-level detail."
            ),
            institution_id=institution_id,
            projects=[],
            metadata={
                "institution_name": institution.display_name,
                "query": query,
                "task_id": task_id,
                "next_step": "Refresh after the extraction finishes; the dashboard does not create duplicate extraction tasks automatically.",
            },
        )

    async def _get_institution_option(self, institution_id: str) -> InstitutionOption:
        try:
            return await self.openalex_client.get_institution_option(institution_id)
        except (httpx.HTTPStatusError, httpx.RequestError):
            return InstitutionOption(id=institution_id, display_name=institution_id)

    def _build_projects_response(
        self,
        institution: InstitutionOption,
        query: str,
        extraction: dict[str, object],
        json_export: dict[str, list[dict[str, object]]],
    ) -> CordisProjectsResponse:
        if json_export.get("_records"):
            return self._build_projects_response_from_records(
                institution=institution,
                query=query,
                extraction=extraction,
                records=json_export["_records"],
            )
        return self._build_projects_response_from_legacy(
            institution=institution,
            query=query,
            extraction=extraction,
            json_export=json_export,
        )

    def _build_projects_response_from_legacy(
        self,
        institution: InstitutionOption,
        query: str,
        extraction: dict[str, object],
        json_export: dict[str, list[dict[str, object]]],
    ) -> CordisProjectsResponse:
        task_id = str(extraction.get("taskId") or extraction.get("taskID") or "")
        all_projects = json_export.get("project", [])
        all_orgs = json_export.get("organization", [])
        all_topics = json_export.get("topics", [])

        institution_name = institution.display_name
        matched_orgs = [
            row for row in all_orgs if self._organization_matches_institution(row, institution_name)
        ]
        matched_project_ids = {self._project_key(row.get("projectID")) for row in matched_orgs if self._project_key(row.get("projectID"))}
        topics_by_project = self._index_by_project_id(all_topics)
        orgs_by_project = self._index_by_project_id(matched_orgs)

        projects: list[CordisProjectRecord] = []
        role_counts: Counter[str] = Counter()
        framework_counts: Counter[str] = Counter()
        partner_countries: set[str] = set()

        for project in all_projects:
            project_id = self._project_key(project.get("id") or project.get("projectID"))
            if not project_id or project_id not in matched_project_ids:
                continue

            org_rows = orgs_by_project.get(project_id, [])
            institution_row = org_rows[0] if org_rows else {}
            related_topics = topics_by_project.get(project_id, [])
            first_topic = related_topics[0] if related_topics else {}

            role = str(institution_row.get("role") or "") or None
            if role:
                role_counts[role] += 1

            framework = str(project.get("frameworkProgramme") or "") or None
            if framework:
                framework_counts[framework] += 1

            for org in all_orgs:
                if self._project_key(org.get("projectID")) != project_id:
                    continue
                country = str(org.get("country") or "").strip()
                if country:
                    partner_countries.add(country)

            projects.append(
                CordisProjectRecord(
                    project_id=project_id,
                    rcn=CordisClient._to_int(project.get("rcn")),
                    acronym=str(project.get("acronym") or "") or None,
                    title=str(project.get("title") or "Untitled project"),
                    framework_programme=framework,
                    funding_scheme=str(project.get("fundingScheme") or "") or None,
                    topic=str(project.get("topics") or "") or str(first_topic.get("topic") or "") or None,
                    topic_title=str(first_topic.get("title") or "") or None,
                    start_date=str(project.get("startDate") or "") or None,
                    end_date=str(project.get("endDate") or "") or None,
                    project_status=str(project.get("status") or "") or None,
                    institution_role=role,
                    institution_name=str(institution_row.get("name") or "") or None,
                    institution_ec_contribution=self._to_float(institution_row.get("ecContribution")),
                    project_ec_max_contribution=self._to_float(project.get("ecMaxContribution")),
                    project_total_cost=self._to_float(project.get("totalCost")),
                    keyword_summary=str(project.get("keywords") or "") or None,
                    objective_excerpt=self._excerpt(project.get("objective")),
                    cordis_url=f"https://cordis.europa.eu/project/id/{project_id}",
                )
            )

        projects.sort(key=lambda item: (item.start_date or "", item.project_id), reverse=True)

        direct_matches = len(projects)
        broad_hits = len(all_projects)
        coordinators = role_counts.get("coordinator", 0)

        return CordisProjectsResponse(
            source="cordis",
            status="active",
            message=(
                f"Latest completed CORDIS JSON extraction contains {broad_hits} project hits for {institution_name}. "
                f"{direct_matches} of them match the institution directly in the CORDIS organization table."
            ),
            institution_id=institution.id,
            projects=projects,
            metadata={
                "institution_name": institution_name,
                "query": query,
                "task_id": task_id,
                "progress": str(extraction.get("progress") or "Finished"),
                "record_count": broad_hits,
                "project_record_count": broad_hits,
                "direct_match_project_count": direct_matches,
                "coordinator_project_count": coordinators,
                "participant_project_count": role_counts.get("participant", 0),
                "role_counts": dict(role_counts),
                "framework_counts": dict(framework_counts),
                "partner_country_count": len(partner_countries),
                "latest_start_year": max((self._year_from_date(project.start_date) for project in projects), default=None),
                "next_step": "This table is limited to direct organization matches in CORDIS. The next refinement would be partner-network summaries and project output metrics.",
            },
        )

    def _build_projects_response_from_records(
        self,
        institution: InstitutionOption,
        query: str,
        extraction: dict[str, object],
        records: list[dict[str, object]],
    ) -> CordisProjectsResponse:
        task_id = str(extraction.get("taskId") or extraction.get("taskID") or "")
        institution_name = institution.display_name
        project_records = [record for record in records if self._is_project_record(record)]

        projects: list[CordisProjectRecord] = []
        role_counts: Counter[str] = Counter()
        framework_counts: Counter[str] = Counter()
        partner_countries: set[str] = set()

        for project in project_records:
            related_organizations = self._project_related_organizations(project)
            for organization in related_organizations:
                country = self._organization_country(organization)
                if country:
                    partner_countries.add(country)

            matched_organizations = [
                organization
                for organization in related_organizations
                if self._organization_record_matches_institution(organization, institution_name)
            ]
            if not matched_organizations:
                continue

            matched_organizations.sort(key=self._organization_sort_key)
            institution_record = matched_organizations[0]
            role = self._organization_role(institution_record)
            if role:
                role_counts[role] += 1

            framework = self._project_framework(project)
            if framework:
                framework_counts[framework] += 1

            topic_code, topic_title = self._project_topic(project)
            project_id = self._project_key(project.get("id"))
            if not project_id:
                continue

            projects.append(
                CordisProjectRecord(
                    project_id=project_id,
                    rcn=CordisClient._to_int(project.get("rcn")),
                    acronym=str(project.get("acronym") or "") or None,
                    title=str(project.get("title") or "Untitled project"),
                    framework_programme=framework,
                    funding_scheme=self._project_funding_scheme(project),
                    topic=topic_code,
                    topic_title=topic_title,
                    start_date=str(project.get("startDate") or "") or None,
                    end_date=str(project.get("endDate") or "") or None,
                    project_status=str(project.get("status") or "") or None,
                    institution_role=role,
                    institution_name=self._organization_name(institution_record),
                    institution_ec_contribution=self._organization_ec_contribution(institution_record),
                    project_ec_max_contribution=self._to_float(project.get("ecMaxContribution")),
                    project_total_cost=self._to_float(project.get("totalCost")),
                    keyword_summary=self._project_keywords(project),
                    objective_excerpt=self._excerpt(project.get("objective") or self._project_content_body(project)),
                    cordis_url=f"https://cordis.europa.eu/project/id/{project_id}",
                )
            )

        projects.sort(key=lambda item: (item.start_date or "", item.project_id), reverse=True)
        broad_hits = len(project_records)
        direct_matches = len(projects)
        coordinators = role_counts.get("coordinator", 0)

        return CordisProjectsResponse(
            source="cordis",
            status="active",
            message=(
                f"Latest completed CORDIS JSON extraction contains {broad_hits} project hits for {institution_name}. "
                f"{direct_matches} of them match the institution directly in the project associations."
            ),
            institution_id=institution.id,
            projects=projects,
            metadata={
                "institution_name": institution_name,
                "query": query,
                "task_id": task_id,
                "progress": str(extraction.get("progress") or "Finished"),
                "record_count": broad_hits,
                "project_record_count": broad_hits,
                "direct_match_project_count": direct_matches,
                "coordinator_project_count": coordinators,
                "participant_project_count": role_counts.get("participant", 0),
                "role_counts": dict(role_counts),
                "framework_counts": dict(framework_counts),
                "partner_country_count": len(partner_countries),
                "latest_start_year": max((self._year_from_date(project.start_date) for project in projects), default=None),
                "next_step": "This table is limited to direct organization matches in CORDIS. The next refinement would be partner-network summaries and project output metrics.",
            },
        )

    @staticmethod
    def _index_by_project_id(rows: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
        indexed: dict[str, list[dict[str, object]]] = {}
        for row in rows:
            project_id = ResearchService._project_key(row.get("projectID") or row.get("id"))
            if not project_id:
                continue
            indexed.setdefault(project_id, []).append(row)
        return indexed

    @staticmethod
    def _project_key(value: object) -> str:
        return str(value).strip() if value not in (None, "") else ""

    @staticmethod
    def _normalize_name(value: object) -> str:
        return "".join(character.lower() for character in str(value or "") if character.isalnum())

    @classmethod
    def _organization_matches_institution(cls, organization_row: dict[str, object], institution_name: str) -> bool:
        candidate = cls._normalize_name(organization_row.get("name"))
        target = cls._normalize_name(institution_name)
        if not candidate or not target:
            return False
        return candidate == target or candidate.startswith(target) or target.startswith(candidate)

    @classmethod
    def _organization_record_matches_institution(cls, organization_row: dict[str, object], institution_name: str) -> bool:
        target = cls._normalize_name(institution_name)
        if not target:
            return False

        candidates = [
            organization_row.get("legalName"),
            organization_row.get("name"),
            organization_row.get("title"),
            organization_row.get("shortName"),
        ]
        for candidate in candidates:
            normalized_candidate = cls._normalize_name(candidate)
            if not normalized_candidate:
                continue
            if normalized_candidate == target or normalized_candidate.startswith(target) or target.startswith(normalized_candidate):
                return True
        return False

    @staticmethod
    def _is_project_record(record: dict[str, object]) -> bool:
        content_type = str(record.get("contenttype") or "").strip().lower()
        if content_type == "project":
            return True

        relations = record.get("relations")
        if not isinstance(relations, dict):
            return False
        categories = relations.get("categories")
        if isinstance(categories, list):
            for category in categories:
                if not isinstance(category, dict):
                    continue
                code = str(category.get("code") or "").strip().lower().lstrip("/")
                title = str(category.get("title") or "").strip().lower()
                if code == "project" or title == "project":
                    return True
        return False

    @classmethod
    def _project_related_organizations(cls, project: dict[str, object]) -> list[dict[str, object]]:
        related_organizations: list[dict[str, object]] = []
        for association in cls._project_associations(project):
            if cls._looks_like_organization_record(association):
                related_organizations.append(association)
        return related_organizations

    @staticmethod
    def _project_associations(project: dict[str, object]) -> list[dict[str, object]]:
        relations = project.get("relations")
        if not isinstance(relations, dict):
            return []

        associations = relations.get("associations", [])
        if isinstance(associations, list):
            return [association for association in associations if isinstance(association, dict)]
        if isinstance(associations, dict):
            flattened: list[dict[str, object]] = []
            for value in associations.values():
                if isinstance(value, list):
                    flattened.extend(item for item in value if isinstance(item, dict))
                elif isinstance(value, dict):
                    flattened.append(value)
            return flattened
        return []

    @staticmethod
    def _association_attributes(association: dict[str, object]) -> dict[str, object]:
        attributes = association.get("attributes")
        return attributes if isinstance(attributes, dict) else {}

    @classmethod
    def _looks_like_organization_record(cls, association: dict[str, object]) -> bool:
        if any(key in association for key in ("legalName", "shortName", "vatNumber")):
            return True

        attribute_type = str(cls._association_attributes(association).get("type") or association.get("type") or "").lower()
        if attribute_type in {"coordinator", "participant", "partner"}:
            return True

        address = association.get("address")
        return isinstance(address, dict) and any(key in address for key in ("country", "city", "postalCode"))

    @classmethod
    def _organization_name(cls, organization: dict[str, object]) -> str | None:
        for key in ("legalName", "name", "title", "shortName"):
            value = str(organization.get(key) or "").strip()
            if value:
                return value
        return None

    @classmethod
    def _organization_role(cls, organization: dict[str, object]) -> str | None:
        value = str(cls._association_attributes(organization).get("type") or organization.get("type") or "").strip().lower()
        return value or None

    @classmethod
    def _organization_country(cls, organization: dict[str, object]) -> str | None:
        address = organization.get("address")
        if isinstance(address, dict):
            value = str(address.get("country") or "").strip()
            if value:
                return value
        return str(organization.get("country") or "").strip() or None

    @classmethod
    def _organization_ec_contribution(cls, organization: dict[str, object]) -> float | None:
        attributes = cls._association_attributes(organization)
        return cls._to_float(attributes.get("ecContribution") or organization.get("ecContribution"))

    @classmethod
    def _organization_sort_key(cls, organization: dict[str, object]) -> tuple[int, str]:
        role = cls._organization_role(organization) or ""
        priority = {"coordinator": 0, "participant": 1}.get(role, 2)
        return (priority, cls._normalize_name(cls._organization_name(organization)))

    @classmethod
    def _project_framework(cls, project: dict[str, object]) -> str | None:
        direct_value = str(project.get("frameworkProgramme") or "").strip()
        if direct_value:
            return direct_value

        for association in cls._project_associations(project):
            value = str(association.get("frameworkProgramme") or "").strip()
            if value:
                return value
        return None

    @classmethod
    def _project_topic(cls, project: dict[str, object]) -> tuple[str | None, str | None]:
        direct_topic = str(project.get("topics") or project.get("topic") or "").strip()
        if direct_topic:
            return direct_topic, str(project.get("topicTitle") or "").strip() or None

        for association in cls._project_associations(project):
            association_type = str(cls._association_attributes(association).get("type") or association.get("type") or "").strip()
            if association_type != "relatedTopic":
                continue
            code = str(association.get("code") or association.get("id") or "").strip() or None
            title = str(association.get("title") or association.get("shortTitle") or "").strip() or None
            return code, title
        return None, None

    @classmethod
    def _project_funding_scheme(cls, project: dict[str, object]) -> str | None:
        for key in ("fundingScheme", "typeOfAction"):
            value = str(project.get(key) or "").strip()
            if value:
                return value
        return None

    @staticmethod
    def _project_keywords(project: dict[str, object]) -> str | None:
        keywords = project.get("keywords")
        if isinstance(keywords, list):
            return ", ".join(str(item).strip() for item in keywords if str(item).strip()) or None
        return str(keywords or "").strip() or None

    @staticmethod
    def _project_content_body(project: dict[str, object]) -> str | None:
        content = project.get("content")
        if not isinstance(content, dict):
            return None
        return str(content.get("body") or "").strip() or None

    @staticmethod
    def _to_float(value: object) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(str(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _excerpt(value: object, max_length: int = 320) -> str | None:
        text = " ".join(str(value or "").split())
        if not text:
            return None
        if len(text) <= max_length:
            return text
        return text[: max_length - 1].rstrip() + "…"

    @staticmethod
    def _year_from_date(value: str | None) -> int | None:
        if not value:
            return None
        try:
            return date.fromisoformat(value).year
        except ValueError:
            return None
