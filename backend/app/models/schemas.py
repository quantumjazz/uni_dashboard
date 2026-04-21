from typing import Any

from pydantic import BaseModel, Field


class IndicatorDefinition(BaseModel):
    id: str
    title: str
    strategic_question: str
    panel: str
    source: str
    dataset: str
    description: str
    unit: str | None = None
    frequency: str | None = None
    default_countries: list[str] = Field(default_factory=list)
    dimensions: dict[str, Any] = Field(default_factory=dict)
    aggregate_dimension: str | None = None
    breakdown_dimension: str | None = None
    notes: str | None = None


class DataPoint(BaseModel):
    source: str
    dataset: str
    indicator: str
    country: str
    year: int
    value: float
    series_key: str | None = None
    series_label: str | None = None
    unit: str | None = None
    note: str | None = None
    dimensions: dict[str, str] = Field(default_factory=dict)


class DataResponse(BaseModel):
    indicator: IndicatorDefinition
    rows: list[DataPoint]
    summary: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class BatchDataError(BaseModel):
    message: str


class BatchDataResponse(BaseModel):
    results: dict[str, DataResponse] = Field(default_factory=dict)
    errors: dict[str, BatchDataError] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CountryOption(BaseModel):
    code: str
    label: str


class InstitutionOption(BaseModel):
    id: str
    display_name: str
    country_code: str | None = None
    works_count: int | None = None
    cited_by_count: int | None = None
    homepage_url: str | None = None
    eter_id: str | None = None
    ror: str | None = None
    aliases: list[str] = Field(default_factory=list)


class ResearchTrendPoint(BaseModel):
    year: int
    works_count: int
    cited_by_count: int
    oa_works_count: int
    open_access_share: float | None = None


class ResearchInstitutionSummary(BaseModel):
    source: str
    institution: InstitutionOption
    works_count: int
    cited_by_count: int
    summary_stats: dict[str, Any] = Field(default_factory=dict)
    counts_by_year: list[ResearchTrendPoint] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExternalSourceStatus(BaseModel):
    source: str
    status: str
    message: str
    institution_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CordisProjectRecord(BaseModel):
    project_id: str
    rcn: int | None = None
    acronym: str | None = None
    title: str
    framework_programme: str | None = None
    funding_scheme: str | None = None
    topic: str | None = None
    topic_title: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    project_status: str | None = None
    institution_role: str | None = None
    institution_name: str | None = None
    institution_ec_contribution: float | None = None
    project_ec_max_contribution: float | None = None
    project_total_cost: float | None = None
    keyword_summary: str | None = None
    objective_excerpt: str | None = None
    cordis_url: str | None = None


class CordisProjectsResponse(BaseModel):
    source: str
    status: str
    message: str
    institution_id: str
    projects: list[CordisProjectRecord] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class QualityReportSummary(BaseModel):
    report_id: str
    report_type: str | None = None
    scope: str | None = None
    decision: str | None = None
    agency: str | None = None
    agency_listing_status: str | None = None
    agency_listing_note: str | None = None
    agency_listing_valid_to: str | None = None
    agency_register_url: str | None = None
    agency_reports_url: str | None = None
    decision_date: str | None = None
    valid_to: str | None = None
    report_url: str | None = None


class QualityReportAnalysisRequest(BaseModel):
    report_id: str
    report_url: str | None = None
    report_type: str | None = None
    scope: str | None = None
    decision: str | None = None
    agency: str | None = None


class QualityReportFinding(BaseModel):
    excerpt: str
    page_number: int | None = None
    signal: str | None = None


class QualityReportAnalysisResponse(BaseModel):
    report_id: str
    status: str
    message: str
    source_url: str | None = None
    resolved_pdf_url: str | None = None
    page_count: int | None = None
    extracted_page_count: int | None = None
    recommendation_count: int = 0
    condition_count: int = 0
    recommendations: list[QualityReportFinding] = Field(default_factory=list)
    conditions: list[QualityReportFinding] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class QualityThemeReportTarget(BaseModel):
    report_id: str
    report_url: str | None = None
    report_type: str | None = None
    scope: str | None = None
    decision: str | None = None
    agency: str | None = None
    decision_date: str | None = None
    institution_id: str | None = None
    institution_name: str | None = None
    country_code: str | None = None


class QualityReportThemeSummaryRequest(BaseModel):
    institution_id: str | None = None
    institution_name: str | None = None
    peer_mode: str | None = None
    filters: dict[str, Any] = Field(default_factory=dict)
    reports: list[QualityThemeReportTarget] = Field(default_factory=list)
    peer_reports: list[QualityThemeReportTarget] = Field(default_factory=list)


class QualityThemeSummaryItem(BaseModel):
    theme_id: str
    label: str
    description: str | None = None
    total_count: int = 0
    recommendation_count: int = 0
    condition_count: int = 0
    report_count: int = 0
    institution_count: int = 0
    report_share: float | None = None
    comparison_label: str | None = None
    comparison_note: str | None = None
    peer_institution_count: int = 0
    peer_institution_share: float | None = None
    sample_excerpt: str | None = None
    sample_page_number: int | None = None
    sample_signal: str | None = None
    sample_report_id: str | None = None
    sample_institution_name: str | None = None


class QualityThemeRecurringItem(BaseModel):
    theme_id: str
    label: str
    finding_type: str
    count: int = 0
    report_count: int = 0
    report_share: float | None = None
    comparison_label: str | None = None
    comparison_note: str | None = None
    sample_excerpt: str | None = None
    sample_page_number: int | None = None
    sample_signal: str | None = None
    sample_report_id: str | None = None
    sample_institution_name: str | None = None


class QualityReportThemeSummaryResponse(BaseModel):
    status: str
    message: str
    requested_report_count: int = 0
    analyzed_report_count: int = 0
    reports_with_findings: int = 0
    requested_peer_report_count: int = 0
    analyzed_peer_report_count: int = 0
    peer_institutions_analyzed: int = 0
    recommendation_count: int = 0
    condition_count: int = 0
    themes: list[QualityThemeSummaryItem] = Field(default_factory=list)
    recurring_recommendations: list[QualityThemeRecurringItem] = Field(default_factory=list)
    recurring_conditions: list[QualityThemeRecurringItem] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class QualityInstitutionStatus(BaseModel):
    source: str
    institution_id: str
    status: str
    current_status: str | None = None
    agency: str | None = None
    decision_date: str | None = None
    summary: str
    reports: list[QualityReportSummary] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class QualityInstitutionResponse(BaseModel):
    institution_id: str
    deqar: QualityInstitutionStatus
    neaa: ExternalSourceStatus
    benchmarking: ExternalSourceStatus
    metadata: dict[str, Any] = Field(default_factory=dict)


class PagePanelDefinition(BaseModel):
    id: str
    title: str
    description: str | None = None
    indicator_ids: list[str] = Field(default_factory=list)
    chart_type: str | None = None
    layout: str | None = None


class PageDefinition(BaseModel):
    id: str
    slug: str
    title: str
    description: str
    status: str
    context_type: str
    controls: list[str] = Field(default_factory=list)
    panels: list[PagePanelDefinition] = Field(default_factory=list)
