import {
  downloadIndicatorCsv,
  fetchBatchData,
  fetchCountries,
  fetchIndicators,
  fetchInstitutionSearch,
  fetchPages,
  fetchProjectsExtraction,
  fetchProjectsStatus,
  fetchQualityReportAnalysis,
  fetchQualityThemeSummary,
  fetchQualityStatus,
  fetchResearchSummary,
} from "./api.js";
import { ensureRoute, onRouteChange, resolveSlug } from "./router.js";

const CHART_COLORS = ["#2563eb", "#7c3aed", "#16a34a", "#e55c20", "#0891b2", "#be185d", "#ca8a04"];
const BG_HIGHLIGHT = "#2563eb";
const BG_OTHER = "#94a3b8";
const QUALITY_PEER_MODES = [
  {
    value: "country",
    label: "Same-country",
    note: "Prefer similar-size institutions in the same country, then broaden only if the local pool is too thin.",
  },
  {
    value: "regional",
    label: "European similar",
    note: "Prefer similar-size higher-education institutions across Europe, then broaden globally only if needed.",
  },
  {
    value: "global",
    label: "Global similar",
    note: "Use a similar-size higher-education cohort globally.",
  },
];

const charts = {};
const state = {
  pages: [],
  pagesBySlug: new Map(),
  indicators: {},
  countryLabels: {},
  activePage: null,
  currentIndicator: null,
  currentInstitution: null,
  currentInstitutions: [],
  institutionOptions: new Map(),
  institutionResults: [],
  institutionSearchMode: "browse",
  researchSummaryCache: new Map(),
  projectsStatusCache: new Map(),
  qualityStatusCache: new Map(),
  qualityReportSelection: new Map(),
  qualityReportAnalysisCache: new Map(),
  qualityThemeSummaryCache: new Map(),
  qualityPeerMode: "regional",
  currentCountries: ["BG", "EU27_2020", "DE", "RO"],
  currentYearRange: { from: 2015, to: 2024 },
};

const pageTitle = document.getElementById("page-title");
const pageDescription = document.getElementById("page-description");
const pageContext = document.getElementById("page-context");
const topNav = document.getElementById("top-nav");
const countryDropdown = document.getElementById("country-dropdown");
const countryToggle = document.getElementById("country-toggle");
const countryMenu = document.getElementById("country-menu");
const countryCount = document.getElementById("country-count");
const yearRange = document.getElementById("year-range");
const exportButton = document.getElementById("export-button");
const countriesControl = document.getElementById("countries-control");
const yearRangeControl = document.getElementById("year-range-control");
const institutionControl = document.getElementById("institution-control");
const institutionDropdown = document.getElementById("institution-dropdown");
const institutionToggle = document.getElementById("institution-toggle");
const institutionSelectionLabel = document.getElementById("institution-selection-label");
const institutionCount = document.getElementById("institution-count");
const institutionMenu = document.getElementById("institution-menu");
const institutionHelp = document.getElementById("institution-help");
const kpiGrid = document.getElementById("kpi-grid");
const pageContent = document.getElementById("page-content");

function debounce(fn, delay = 400) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

function sourceLabel(source) {
  if (source === "eurostat") return "Eurostat";
  if (source === "derived") return "Eurostat-derived";
  return source;
}

function contextLabel(contextType) {
  if (contextType === "country_context") return "Country context";
  if (contextType === "institution_context") return "Institution context";
  if (contextType === "mixed_context") return "Mixed context";
  return "Dashboard page";
}

function pageStatusLabel(status) {
  if (status === "active") return "Live";
  if (status === "ready") return "Ready";
  if (status === "processing") return "Processing";
  if (status === "blocked_by_credentials") return "Needs credentials";
  if (status === "unavailable") return "Unavailable";
  return "Planned";
}

function qualityPeerModeMeta(value) {
  return QUALITY_PEER_MODES.find((option) => option.value === value) || QUALITY_PEER_MODES[1];
}

function isHtmlPanelType(chartType) {
  return [
    "blocked_notice",
    "quality_status",
    "quality_reports",
    "quality_benchmarking",
    "overview_national_snapshot",
    "overview_trend_watch",
    "overview_institution_brief",
  ].includes(chartType);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function countryLabel(code) {
  return state.countryLabels[code] || code;
}

function institutionLabel(institution) {
  if (!institution) return "No institution selected";
  return institution.country_code ? `${institution.display_name} · ${institution.country_code}` : institution.display_name;
}

function truncateText(value, maxLength = 96) {
  const text = String(value ?? "");
  if (text.length <= maxLength) return text;
  return `${text.slice(0, Math.max(maxLength - 1, 1)).trimEnd()}…`;
}

function formatValue(value, unit) {
  if (unit === "percent") return value.toFixed(1) + "%";
  if (unit === "persons" || Number.isInteger(value) || Math.abs(value) >= 1000) {
    return new Intl.NumberFormat("en", { maximumFractionDigits: 0 }).format(value);
  }
  return value.toFixed(1);
}

function formatDelta(value, unit) {
  if (unit === "percent") {
    return `${value >= 0 ? "+" : ""}${Math.abs(value).toFixed(1)} pp`;
  }
  return `${value >= 0 ? "+" : ""}${formatValue(Math.abs(value), unit)}`;
}

function formatCurrency(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
  return new Intl.NumberFormat("en", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(Number(value));
}

function unitLabel(unit) {
  if (unit === "percent") return "% of population";
  if (unit === "persons") return "persons";
  return unit || "";
}

function formatCalendarDate(value) {
  if (!value) return "n/a";
  const parsed = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat("en", {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  }).format(parsed);
}

function formatDateTime(value) {
  if (!value) return "n/a";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat("en", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "UTC",
    timeZoneName: "short",
  }).format(parsed);
}

function datasetFreshnessShort(metadata) {
  const ageDays = metadata?.dataset_age_days;
  if (!Number.isFinite(ageDays)) return "";
  if (ageDays === 0) return "Local snapshot updated today";
  if (ageDays === 1) return "Local snapshot updated 1 day ago";
  return `Local snapshot updated ${ageDays} days ago`;
}

function datasetFreshnessNote(metadata) {
  const updatedAt = metadata?.dataset_updated_at;
  const updatedLabel = updatedAt ? formatDateTime(updatedAt) : "an unknown time";
  const ageText = datasetFreshnessShort(metadata);
  return ageText ? `${updatedLabel} • ${ageText}` : updatedLabel;
}

function qualityMatchConfidenceLabel(value) {
  if (value === "high") return "High confidence";
  if (value === "medium") return "Medium confidence";
  if (value === "low") return "Low confidence";
  return "No match";
}

function qualityMatchMethodLabel(value) {
  if (value === "deqar_id") return "Explicit DEQAR ID";
  if (value === "eter_id") return "ETER crosswalk";
  if (value === "ror") return "ROR crosswalk";
  if (value === "registry_crosswalk") return "Registry crosswalk";
  if (value === "website") return "Website host";
  if (value === "exact_name") return "Exact name";
  if (value === "exact_name_and_website") return "Name + website";
  if (value === "website_and_fuzzy_name") return "Website + fuzzy name";
  if (value === "fuzzy_name") return "Fuzzy name";
  return "Dataset matching";
}

function qualityCoverageMixLabel(metadata) {
  const institutional = Number(metadata?.institutional_report_count || 0);
  const programme = Number(metadata?.programme_report_count || 0);
  const monitoring = Number(metadata?.monitoring_report_count || 0);
  const other = Number(metadata?.other_report_count || 0);
  const parts = [];
  if (institutional) parts.push(`${institutional} institutional`);
  if (programme) parts.push(`${programme} programme`);
  if (monitoring) parts.push(`${monitoring} monitoring`);
  if (other) parts.push(`${other} other`);
  return parts.length ? parts.join(" • ") : "No linked reports";
}

function qualityInstitutionalValidityLabel(metadata) {
  if (metadata?.current_institutional_decision_date) {
    if (metadata.current_institutional_valid_to) {
      return formatCalendarDate(metadata.current_institutional_valid_to);
    }
    return "Open-ended / not listed";
  }
  if (metadata?.latest_institutional_valid_to) {
    return formatCalendarDate(metadata.latest_institutional_valid_to);
  }
  return "No institutional review";
}

function qualityInstitutionalValidityNote(metadata) {
  if (metadata?.current_institutional_decision_date) {
    const decisionDate = formatCalendarDate(metadata.current_institutional_decision_date);
    if (metadata.current_institutional_valid_to) {
      return `Current institutional review decided ${decisionDate}`;
    }
    return `Current institutional review decided ${decisionDate}. No end date is listed in this snapshot.`;
  }
  if (metadata?.latest_institutional_decision_date) {
    return `Latest institutional review decided ${formatCalendarDate(metadata.latest_institutional_decision_date)}`;
  }
  return "No institutional-level review is visible in this snapshot.";
}

function formatDurationFromDays(days) {
  if (!Number.isFinite(days)) return "n/a";
  if (days >= 365) return `${(days / 365.25).toFixed(1)} years`;
  if (days >= 60) return `${Math.round(days / 30.4)} months`;
  return `${days} days`;
}

function qualityRiskLevelLabel(value) {
  if (value === "high") return "High watch";
  if (value === "medium") return "Medium watch";
  if (value === "low") return "Stable";
  return "Unknown";
}

function qualityRiskChipClass(value) {
  if (value === "high") return "quality-chip risk-high";
  if (value === "medium") return "quality-chip risk-medium";
  return "quality-chip risk-low";
}

function qualityValidityStatusLabel(value) {
  if (value === "expires_within_12_months") return "Expires <12m";
  if (value === "expires_within_24_months") return "Expires <24m";
  if (value === "active_open_ended") return "Open-ended";
  if (value === "expired") return "Expired";
  if (value === "historical_only") return "Historical only";
  if (value === "no_institutional_review") return "No institutional review";
  return "Active";
}

function qualityValidityCountdownLabel(metadata) {
  const status = metadata?.institutional_validity_status;
  const days = metadata?.institutional_days_remaining;
  if (status === "no_institutional_review") return "No institutional review";
  if (status === "active_open_ended") return "No listed end date";
  if (status === "historical_only") return "Historical only";
  if (status === "expired") {
    if (Number.isFinite(days)) return `${formatDurationFromDays(Math.abs(days))} ago`;
    return "Expired";
  }
  if (Number.isFinite(days)) return formatDurationFromDays(days);
  return metadata?.institutional_valid_to ? formatCalendarDate(metadata.institutional_valid_to) : "n/a";
}

function qualityValidityCountdownNote(metadata) {
  if (metadata?.institutional_validity_label) return metadata.institutional_validity_label;
  if (metadata?.institutional_valid_to) return `Listed validity end date ${formatCalendarDate(metadata.institutional_valid_to)}`;
  return "";
}

function qualityDecisionPatternNote(metadata) {
  const windowYears = Number(metadata?.recent_window_years || 5);
  const recentConditional = Number(metadata?.recent_conditional_decision_count || 0);
  const recentNegative = Number(metadata?.recent_negative_decision_count || 0);
  if (recentNegative > 0) return `${recentNegative} negative decision${recentNegative === 1 ? "" : "s"} in the last ${windowYears} years`;
  if (recentConditional > 0) return `${recentConditional} conditional decision${recentConditional === 1 ? "" : "s"} in the last ${windowYears} years`;
  return `No negative decisions in the last ${windowYears} years`;
}

function qualityCrosswalkValueLabel(metadata) {
  if (metadata?.match_provenance_label) return metadata.match_provenance_label;
  if (metadata?.crosswalk_scheme && metadata?.crosswalk_value) return metadata.crosswalk_value;
  return qualityMatchMethodLabel(metadata?.match_type);
}

function qualityCrosswalkNote(metadata) {
  if (metadata?.match_provenance_note) return metadata.match_provenance_note;
  if (metadata?.crosswalk_note) return metadata.crosswalk_note;
  return metadata?.match_confidence_note || "";
}

function qualityRegistryProfileLabel(record) {
  const parts = [];
  if (record?.registry_canonical_name && record.registry_canonical_name !== record.display_name && record.registry_canonical_name !== record.institution_name) {
    parts.push(record.registry_canonical_name);
  }
  if (record?.registry_institution_type) parts.push(record.registry_institution_type);
  if (record?.registry_legal_status) parts.push(record.registry_legal_status);
  return parts.join(" • ");
}

function qualityPeerMatchSummary(peer) {
  const parts = [peer?.is_primary ? "Selected institution" : "Peer institution"];
  if (peer?.match_provenance_label) {
    parts.push(peer.match_provenance_label);
  } else if (peer?.match_type) {
    parts.push(qualityMatchMethodLabel(peer.match_type));
  } else {
    parts.push("No DEQAR match");
  }
  if (peer?.match_confidence) {
    parts.push(qualityMatchConfidenceLabel(peer.match_confidence));
  }
  return parts.join(" • ");
}

function qualityPeerValidityLabel(peer) {
  if (peer?.institutional_valid_to) return formatCalendarDate(peer.institutional_valid_to);
  return qualityValidityStatusLabel(peer?.institutional_validity_status);
}

function qualityNeaaIsApplicable(quality) {
  return Boolean(quality?.neaa?.metadata?.applicable);
}

function qualityNeaaIsActive(quality) {
  return quality?.neaa?.status === "active";
}

function qualityOverallSourceStatus(quality) {
  if (quality?.deqar?.status === "active" || qualityNeaaIsActive(quality)) return "active";
  return quality?.deqar?.status || quality?.neaa?.status || "unavailable";
}

function qualitySourceTagLabel(quality) {
  if (quality?.deqar?.status === "active" && qualityNeaaIsActive(quality)) return "DEQAR + NEAA";
  if (qualityNeaaIsActive(quality)) return "NEAA";
  return String(quality?.deqar?.source || "quality").toUpperCase();
}

function qualityNeaaValidityLabel(metadata) {
  if (metadata?.valid_to) return formatCalendarDate(metadata.valid_to);
  return metadata?.valid_to_text || "n/a";
}

function qualityNeaaRatingLabel(metadata) {
  const numeric = Number(metadata?.rating_value);
  if (Number.isFinite(numeric) && numeric > 0) return numeric.toFixed(2);
  return metadata?.rating_text || "n/a";
}

function qualityNeaaLinksMarkup(metadata = {}) {
  const links = [];
  if (metadata.full_report_url) {
    links.push(
      `<a class="quality-report-link" href="${escapeHtml(metadata.full_report_url)}" target="_blank" rel="noreferrer">Open NEAA report</a>`,
    );
  }
  if (metadata.annotation_ia_url) {
    links.push(
      `<a class="quality-report-link" href="${escapeHtml(metadata.annotation_ia_url)}" target="_blank" rel="noreferrer">Annotation IA</a>`,
    );
  }
  return links.join("");
}

function qualityNeaaOverlayMarkup(neaa) {
  if (!neaa?.metadata?.applicable) return "";
  const metadata = neaa.metadata || {};

  if (neaa.status !== "active") {
    return qualityNoticeMarkup("NEAA local context", neaa.message || "NEAA did not return a local institutional match right now.", "neutral");
  }

  return `
    <article class="quality-report-detail-card">
      <div class="quality-report-detail-header">
        <div>
          <p class="blocked-kicker">Bulgaria local context</p>
          <h3>NEAA institutional accreditation</h3>
        </div>
        <div class="quality-report-actions">
          ${qualityNeaaLinksMarkup(metadata)}
        </div>
      </div>
      <div class="quality-report-detail-grid">
        ${qualityFactCard("Institution", metadata.matched_institution_name || "n/a")}
        ${qualityFactCard("Current status", metadata.current_status || "n/a")}
        ${qualityFactCard("Decision date", metadata.decision_date ? formatCalendarDate(metadata.decision_date) : (metadata.decision_date_text || "n/a"))}
        ${qualityFactCard("Validity", qualityNeaaValidityLabel(metadata))}
        ${qualityFactCard("Rating / assessment", qualityNeaaRatingLabel(metadata))}
        ${qualityFactCard("Capacity", metadata.capacity_text || "n/a")}
        ${qualityFactCard("Match confidence", qualityMatchConfidenceLabel(metadata.match_confidence), metadata.match_note || "")}
        ${qualityFactCard("Snapshot updated", formatDateTime(metadata.dataset_updated_at), "Live NEAA page parse")}
      </div>
      <p class="quality-report-analysis-copy">${escapeHtml(neaa.message || "")}</p>
      <div class="quality-status-notices compact">
        ${
          metadata.comparison_summary
            ? qualityNoticeMarkup("Local vs DEQAR", metadata.comparison_summary, metadata.comparison_tone || "neutral")
            : ""
        }
        ${(metadata.notes || [])
          .slice(0, 2)
          .map((note) => qualityNoticeMarkup("NEAA note", note, "neutral"))
          .join("")}
      </div>
      <div class="quality-status-meta">
        <p>
          NEAA source:
          <a class="quality-inline-link" href="${escapeHtml(metadata.source_url || "https://www.neaa.government.bg/en/accredited-higher-education-institutions/higher-institutions")}" target="_blank" rel="noreferrer">Higher education institutions</a>
        </p>
        ${
          metadata.full_report_url
            ? `<p>Full report: <a class="quality-inline-link" href="${escapeHtml(metadata.full_report_url)}" target="_blank" rel="noreferrer">Bulgarian version page</a></p>`
            : ""
        }
        ${
          metadata.annotation_pamc_url || metadata.annotation_dl_url || metadata.previous_accreditation_url
            ? `<p>${[
                metadata.annotation_pamc_url
                  ? `<a class="quality-inline-link" href="${escapeHtml(metadata.annotation_pamc_url)}" target="_blank" rel="noreferrer">Annotation PAMC</a>`
                  : "",
                metadata.annotation_dl_url
                  ? `<a class="quality-inline-link" href="${escapeHtml(metadata.annotation_dl_url)}" target="_blank" rel="noreferrer">Annotation DL</a>`
                  : "",
                metadata.previous_accreditation_url
                  ? `<a class="quality-inline-link" href="${escapeHtml(metadata.previous_accreditation_url)}" target="_blank" rel="noreferrer">Previous institutional accreditation</a>`
                  : "",
              ]
                .filter(Boolean)
                .join(" • ")}</p>`
            : ""
        }
      </div>
    </article>
  `;
}

function qualityActivityTableMarkup(reportYears) {
  if (!reportYears?.length) return "";
  return `
    <section class="quality-activity-card">
      <div class="quality-activity-header">
        <div>
          <p class="blocked-kicker">Decision analytics</p>
          <h3>Filtered report activity by year</h3>
        </div>
        <p>Counts below show how the current filtered report set is distributed across institutional, programme, and monitoring activity.</p>
      </div>
      <div class="table-wrap quality-activity-table-wrap">
        <table class="quality-activity-table">
          <thead>
            <tr>
              <th>Year</th>
              <th>Total</th>
              <th>Institutional</th>
              <th>Programme</th>
              <th>Monitoring</th>
              <th>Conditional</th>
              <th>Negative</th>
            </tr>
          </thead>
          <tbody>
            ${reportYears
              .map(
                (row) => `
                  <tr>
                    <td>${escapeHtml(String(row.year))}</td>
                    <td>${escapeHtml(String(row.total || 0))}</td>
                    <td>${escapeHtml(String(row.institutional || 0))}</td>
                    <td>${escapeHtml(String(row.programme || 0))}</td>
                    <td>${escapeHtml(String(row.monitoring || 0))}</td>
                    <td>${escapeHtml(String(row.conditional || 0))}</td>
                    <td>${escapeHtml(String(row.negative || 0))}</td>
                  </tr>
                `,
              )
              .join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function qualityNoticeMarkup(title, body, tone = "neutral") {
  if (!body) return "";
  return `
    <article class="quality-notice ${tone}">
      <strong>${escapeHtml(title)}</strong>
      <p>${escapeHtml(body)}</p>
    </article>
  `;
}

function qualityBlockedDetails(metadata = {}) {
  return [datasetFreshnessNote(metadata), metadata.coverage_notice].filter(Boolean);
}

function qualitySelectionKey(quality) {
  return quality?.institution_id || quality?.deqar?.institution_id || "";
}

function qualityReportScopeLabel(value) {
  if (value === "institutional") return "Institutional";
  if (value === "programme") return "Programme";
  if (value === "monitoring") return "Monitoring";
  return "Other";
}

function qualityDecisionToneLabel(value) {
  if (value === "positive") return "Positive";
  if (value === "conditional") return "Conditional";
  if (value === "negative") return "Negative";
  return "Neutral / other";
}

function preferredQualityReportId(quality, reports = []) {
  if (!reports.length) return "";
  const selectionKey = qualitySelectionKey(quality);
  const savedReportId = state.qualityReportSelection.get(selectionKey);
  if (savedReportId && reports.some((report) => report.report_id === savedReportId)) {
    return savedReportId;
  }

  return reports.find((report) => report.scope === "institutional")?.report_id || reports[0]?.report_id || "";
}

function rememberQualityReportSelection(quality, reportId) {
  const selectionKey = qualitySelectionKey(quality);
  if (!selectionKey || !reportId) return;
  state.qualityReportSelection.set(selectionKey, reportId);
}

function qualityReportOptionLabel(report) {
  const decisionDate = report.decision_date || "n/a";
  return `${decisionDate} • ${qualityReportScopeLabel(report.scope)} • ${truncateText(report.report_type || "Report")}`;
}

function qualityReportAnalysisKey(report) {
  return report?.report_id || report?.report_url || "";
}

function qualityCachedReportAnalysis(report) {
  const key = qualityReportAnalysisKey(report);
  if (!key) return null;
  return state.qualityReportAnalysisCache.get(key) || null;
}

function qualityFactCard(label, value, note = "") {
  return `
    <article class="quality-fact-card">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value || "n/a")}</strong>
      ${note ? `<small>${escapeHtml(note)}</small>` : ""}
    </article>
  `;
}

function qualityAgencyRegisterValue(record) {
  const status = record?.agency_listing_status || record?.agency_register_status;
  const validTo = record?.agency_listing_valid_to || record?.agency_register_valid_to;
  if (status === "EQAR listed" && validTo) {
    return `Listed until ${formatCalendarDate(validTo)}`;
  }
  if (status === "Listing expired" && validTo) {
    return `Expired on ${formatCalendarDate(validTo)}`;
  }
  if (status) {
    return status;
  }
  if (validTo) {
    return formatCalendarDate(validTo);
  }
  return "n/a";
}

function qualityAgencyRegisterLinksMarkup(record) {
  const links = [];
  if (record?.agency_register_url) {
    links.push(
      `<a class="quality-report-link" href="${escapeHtml(record.agency_register_url)}" target="_blank" rel="noreferrer">Open register entry</a>`,
    );
  }
  if (record?.agency_reports_url) {
    links.push(
      `<a class="quality-report-link" href="${escapeHtml(record.agency_reports_url)}" target="_blank" rel="noreferrer">Agency DEQAR reports</a>`,
    );
  }
  return links.join("");
}

function qualityFindingListMarkup(title, findings = []) {
  if (!findings.length) return "";
  return `
    <section class="quality-report-analysis-list">
      <div class="quality-report-analysis-heading">
        <h4>${escapeHtml(title)}</h4>
      </div>
      <ul>
        ${findings
          .map(
            (finding) => `
              <li>
                <p>${escapeHtml(finding.excerpt || "")}</p>
                <small>
                  ${[
                    finding.page_number ? `Page ${finding.page_number}` : "",
                    finding.signal ? `Signal: ${String(finding.signal).replaceAll("_", " ")}` : "",
                  ]
                    .filter(Boolean)
                    .join(" • ")}
                </small>
              </li>
            `,
          )
          .join("")}
      </ul>
    </section>
  `;
}

function formatSharePercent(value) {
  if (!Number.isFinite(value)) return "n/a";
  return `${Math.round(value * 100)}%`;
}

function qualityThemeSampleMarkup(item) {
  if (!item?.sample_excerpt) return "";
  const meta = [
    item.sample_institution_name || "",
    item.sample_page_number ? `Page ${item.sample_page_number}` : "",
    item.sample_signal ? `Signal: ${String(item.sample_signal).replaceAll("_", " ")}` : "",
  ]
    .filter(Boolean)
    .join(" • ");

  return `
    <blockquote class="quality-theme-sample">
      <p>${escapeHtml(item.sample_excerpt)}</p>
      ${meta ? `<small>${escapeHtml(meta)}</small>` : ""}
    </blockquote>
  `;
}

function qualityThemeRecurringListMarkup(title, items = [], emptyCopy = "") {
  return `
    <section class="quality-theme-list">
      <div class="quality-theme-list-header">
        <h4>${escapeHtml(title)}</h4>
      </div>
      ${
        items.length
          ? `
            <ul>
              ${items
                .map(
                  (item) => `
                    <li>
                      <div class="quality-theme-list-row">
                        <strong>${escapeHtml(item.label || "Theme")}</strong>
                        <span>${escapeHtml(String(item.count ?? 0))} excerpts</span>
                      </div>
                      <p>${escapeHtml(item.comparison_note || `${item.report_count || 0} reports in the current filtered set`)}</p>
                      ${qualityThemeSampleMarkup(item)}
                    </li>
                  `,
                )
                .join("")}
            </ul>
          `
          : `<p class="quality-theme-empty">${escapeHtml(emptyCopy || "No recurring themes detected yet.")}</p>`
      }
    </section>
  `;
}

function qualityThemeCardsMarkup(themes = []) {
  if (!themes.length) {
    return qualityNoticeMarkup(
      "No recurring theme buckets yet",
      "The current filtered reports were parsed, but the extracted excerpts did not cluster into recurring themes with the current heuristic rules.",
      "neutral",
    );
  }

  return `
    <div class="quality-theme-grid">
      ${themes
        .map(
          (theme) => `
            <article class="quality-theme-card">
              <div class="quality-theme-card-header">
                <div>
                  <p class="blocked-kicker">Theme</p>
                  <h4>${escapeHtml(theme.label || "Theme")}</h4>
                </div>
                ${theme.comparison_label ? `<span class="quality-chip neutral">${escapeHtml(theme.comparison_label)}</span>` : ""}
              </div>
              <p class="quality-theme-card-copy">${escapeHtml(theme.description || "")}</p>
              <div class="quality-theme-card-stats">
                ${qualityFactCard("Reports", String(theme.report_count ?? 0), theme.report_share !== null ? `${formatSharePercent(theme.report_share)} of analyzed reports` : "")}
                ${qualityFactCard("Recommendations", String(theme.recommendation_count ?? 0), "Recommendation-like excerpts")}
                ${qualityFactCard("Conditions", String(theme.condition_count ?? 0), "Condition-like excerpts")}
                ${
                  theme.peer_institution_share !== null
                    ? qualityFactCard(
                        "Peer prevalence",
                        formatSharePercent(theme.peer_institution_share),
                        `${theme.peer_institution_count || 0} peer institutions`,
                      )
                    : qualityFactCard("Peer prevalence", "n/a", "No peer comparison in this summary")
                }
              </div>
              ${theme.comparison_note ? `<p class="quality-theme-card-note">${escapeHtml(theme.comparison_note)}</p>` : ""}
              ${qualityThemeSampleMarkup(theme)}
            </article>
          `,
        )
        .join("")}
    </div>
  `;
}

function qualityThemeSummaryMarkup(quality, filteredReports, filters, summary) {
  if (!filteredReports.length) {
    return qualityNoticeMarkup(
      "Theme summary unavailable",
      "No reports match the current filters, so there is nothing to summarize.",
      "neutral",
    );
  }

  const peerTargetCount = (quality?.benchmarking?.metadata?.peers || [])
    .filter((peer) => !peer?.is_primary && preferredPeerInstitutionalReview(peer))
    .length;
  if (!summary) {
    return `
      <section class="quality-theme-summary">
        <div class="quality-theme-toolbar">
          <div>
            <p class="blocked-kicker">Filtered report themes</p>
            <h3>Aggregate recommendations across the current report set</h3>
          </div>
          <button type="button" class="quality-theme-summary-button">Analyze reports in view</button>
        </div>
        <p class="quality-theme-copy">
          Aggregates recurring recommendation and condition themes across the current filtered DEQAR reports${
            peerTargetCount ? ` and compares them to ${peerTargetCount} peer institutional reviews.` : "."
          }
        </p>
      </section>
    `;
  }

  if (summary.status === "processing") {
    return `
      <section class="quality-theme-summary">
        <div class="quality-theme-toolbar">
          <div>
            <p class="blocked-kicker">Filtered report themes</p>
            <h3>Aggregate recommendations across the current report set</h3>
          </div>
          <button type="button" class="quality-theme-summary-button" disabled>Analyzing reports…</button>
        </div>
        <p class="quality-theme-copy">Running the current filtered report set through the PDF analysis layer now.</p>
      </section>
    `;
  }

  return `
    <section class="quality-theme-summary">
      <div class="quality-theme-toolbar">
        <div>
          <p class="blocked-kicker">Filtered report themes</p>
          <h3>Aggregate recommendations across the current report set</h3>
        </div>
        <button type="button" class="quality-theme-summary-button">Re-run summary</button>
      </div>
      <p class="quality-theme-copy">${escapeHtml(summary.message || "Theme summary is ready.")}</p>
      <div class="quality-theme-overview">
        ${qualityFactCard("Status", summary.status || "n/a", `Decision years: ${qualityYearRangeLabel(filters)}`)}
        ${qualityFactCard("Reports analyzed", String(summary.analyzed_report_count ?? 0), `${summary.requested_report_count || filteredReports.length} requested`)}
        ${qualityFactCard("Reports with findings", String(summary.reports_with_findings ?? 0), "At least one recommendation or condition excerpt")}
        ${qualityFactCard("Peer reviews", String(summary.peer_institutions_analyzed ?? 0), "Institutional peer reviews used for comparison")}
      </div>
      ${
        summary.metadata?.truncated_report_count || summary.metadata?.truncated_peer_report_count
          ? qualityNoticeMarkup(
              "Analysis cap",
              [
                summary.metadata?.truncated_report_count
                  ? `This summary analyzed the newest ${summary.analyzed_report_count || 0} linked reports in view. ${summary.metadata.truncated_report_count} additional reports were left out to keep the PDF pass bounded.`
                  : "",
                summary.metadata?.truncated_peer_report_count
                  ? `${summary.metadata.truncated_peer_report_count} additional peer institutional reviews were also left out of the comparison pass.`
                  : "",
              ]
                .filter(Boolean)
                .join(" "),
              "neutral",
            )
          : ""
      }
      ${
        summary.status === "active" || summary.status === "ready"
          ? `
            <div class="quality-theme-columns">
              ${qualityThemeRecurringListMarkup(
                "Recurring recommendations",
                summary.recurring_recommendations || [],
                "No recurring recommendation themes were detected in the current filtered set.",
              )}
              ${qualityThemeRecurringListMarkup(
                "Recurring conditions",
                summary.recurring_conditions || [],
                "No recurring condition themes were detected in the current filtered set.",
              )}
            </div>
            ${qualityThemeCardsMarkup(summary.themes || [])}
          `
          : qualityNoticeMarkup("Summary status", summary.message || "The filtered report set could not be summarized right now.", "neutral")
      }
    </section>
  `;
}

function qualityReportAnalysisMarkup(report, analysis) {
  if (!report?.report_url) {
    return qualityNoticeMarkup("PDF analysis unavailable", "No linked report file is available for this record.", "neutral");
  }

  if (!analysis) {
    return `
      <section class="quality-report-analysis">
        <div class="quality-report-analysis-toolbar">
          <div>
            <p class="blocked-kicker">PDF analysis</p>
            <h3>Extract recommendations and conditions</h3>
          </div>
          <button type="button" class="quality-report-analysis-button">Analyze linked PDF</button>
        </div>
        <p class="quality-report-analysis-copy">Runs a lightweight PDF pass over the linked report file and pulls out recommendation-like and condition-like excerpts.</p>
      </section>
    `;
  }

  if (analysis.status === "processing") {
    return `
      <section class="quality-report-analysis">
        <div class="quality-report-analysis-toolbar">
          <div>
            <p class="blocked-kicker">PDF analysis</p>
            <h3>Extract recommendations and conditions</h3>
          </div>
          <button type="button" class="quality-report-analysis-button" disabled>Analyzing PDF…</button>
        </div>
        <p class="quality-report-analysis-copy">Downloading and parsing the linked PDF now.</p>
      </section>
    `;
  }

  return `
    <section class="quality-report-analysis">
      <div class="quality-report-analysis-toolbar">
        <div>
          <p class="blocked-kicker">PDF analysis</p>
          <h3>Extract recommendations and conditions</h3>
        </div>
        <button type="button" class="quality-report-analysis-button">Re-run analysis</button>
      </div>
      <div class="quality-report-analysis-summary">
        ${qualityFactCard("Status", analysis.status || "n/a", analysis.message || "")}
        ${qualityFactCard("Recommendations", String(analysis.recommendation_count ?? 0), "Recommendation-like excerpts")}
        ${qualityFactCard("Conditions", String(analysis.condition_count ?? 0), "Condition or restriction excerpts")}
        ${qualityFactCard("Pages with text", String(analysis.metadata?.pages_with_text ?? analysis.extracted_page_count ?? 0), analysis.page_count ? `${analysis.page_count} total pages` : "")}
      </div>
      ${
        analysis.status === "active" || analysis.status === "ready"
          ? `
            ${(analysis.recommendations?.length || analysis.conditions?.length)
              ? `
                <div class="quality-report-analysis-findings">
                  ${qualityFindingListMarkup("Recommendations", analysis.recommendations || [])}
                  ${qualityFindingListMarkup("Conditions", analysis.conditions || [])}
                </div>
              `
              : qualityNoticeMarkup("No extracted findings", analysis.message || "The PDF text was parsed, but the current heuristics did not detect recommendation-style excerpts.", "neutral")}
          `
          : qualityNoticeMarkup("Analysis status", analysis.message || "The linked PDF could not be analyzed right now.", "neutral")
      }
      ${
        analysis.resolved_pdf_url && analysis.resolved_pdf_url !== report.report_url
          ? `<p class="quality-report-analysis-copy">Resolved PDF file: <a class="quality-inline-link" href="${escapeHtml(analysis.resolved_pdf_url)}" target="_blank" rel="noreferrer">Open extracted source</a></p>`
          : ""
      }
    </section>
  `;
}

async function requestQualityReportAnalysis(report) {
  const cacheKey = qualityReportAnalysisKey(report);
  if (!cacheKey) return null;

  const processing = {
    report_id: report.report_id,
    status: "processing",
    message: "Downloading and parsing the linked PDF now.",
    recommendation_count: 0,
    condition_count: 0,
    recommendations: [],
    conditions: [],
    metadata: {},
  };
  state.qualityReportAnalysisCache.set(cacheKey, processing);

  try {
    const result = await fetchQualityReportAnalysis({
      report_id: report.report_id,
      report_url: report.report_url,
      report_type: report.report_type,
      scope: report.scope,
      decision: report.decision,
      agency: report.agency,
    });
    state.qualityReportAnalysisCache.set(cacheKey, result);
    return result;
  } catch (error) {
    const failed = {
      report_id: report.report_id,
      status: "error",
      message: error.message || "PDF analysis failed.",
      recommendation_count: 0,
      condition_count: 0,
      recommendations: [],
      conditions: [],
      metadata: {},
    };
    state.qualityReportAnalysisCache.set(cacheKey, failed);
    return failed;
  }
}

function qualityThemeSummaryKey(quality, filters, filteredReports) {
  const payload = {
    institutionId: qualitySelectionKey(quality),
    peerMode: state.qualityPeerMode,
    filters,
    reportIds: (filteredReports || []).map((report) => report.report_id),
  };
  return JSON.stringify(payload);
}

function qualityCachedThemeSummary(quality, filters, filteredReports) {
  const key = qualityThemeSummaryKey(quality, filters, filteredReports);
  return state.qualityThemeSummaryCache.get(key) || null;
}

function preferredPeerInstitutionalReview(peer) {
  const reviews = Array.isArray(peer?.institutional_reviews) ? peer.institutional_reviews : [];
  return (
    reviews.find((review) => review?.is_current && review?.report_url)
    || reviews.find((review) => review?.report_url)
    || null
  );
}

function buildQualityThemeSummaryPayload(quality, filteredReports, filters) {
  const peerMode = state.qualityPeerMode;
  const primaryTargets = (filteredReports || []).map((report) => ({
    report_id: report.report_id,
    report_url: report.report_url,
    report_type: report.report_type,
    scope: report.scope,
    decision: report.decision,
    agency: report.agency,
    decision_date: report.decision_date,
    institution_id: quality?.institution_id,
    institution_name: quality?.deqar?.metadata?.matched_institution_name || quality?.deqar?.metadata?.institution_name || "",
  }));
  const peerTargets = (quality?.benchmarking?.metadata?.peers || [])
    .filter((peer) => !peer?.is_primary)
    .map((peer) => {
      const review = preferredPeerInstitutionalReview(peer);
      if (!review?.report_url) return null;
      return {
        report_id: review.report_id,
        report_url: review.report_url,
        report_type: review.report_type,
        scope: "institutional",
        decision: review.decision,
        agency: review.agency,
        decision_date: review.decision_date,
        institution_id: peer.institution_id,
        institution_name: peer.display_name,
        country_code: peer.country_code,
      };
    })
    .filter(Boolean);

  return {
    institution_id: quality?.institution_id,
    institution_name: quality?.deqar?.metadata?.matched_institution_name || quality?.deqar?.metadata?.institution_name || "",
    peer_mode: peerMode,
    filters,
    reports: primaryTargets,
    peer_reports: peerTargets,
  };
}

async function requestQualityThemeSummary(quality, filteredReports, filters) {
  const cacheKey = qualityThemeSummaryKey(quality, filters, filteredReports);
  const payload = buildQualityThemeSummaryPayload(quality, filteredReports, filters);
  const processing = {
    status: "processing",
    message: "Analyzing the current filtered report set now.",
    requested_report_count: filteredReports.length,
    analyzed_report_count: 0,
    reports_with_findings: 0,
    requested_peer_report_count: payload.peer_reports.length,
    analyzed_peer_report_count: 0,
    peer_institutions_analyzed: 0,
    recommendation_count: 0,
    condition_count: 0,
    themes: [],
    recurring_recommendations: [],
    recurring_conditions: [],
    metadata: {},
  };
  state.qualityThemeSummaryCache.set(cacheKey, processing);

  try {
    const result = await fetchQualityThemeSummary(payload);
    state.qualityThemeSummaryCache.set(cacheKey, result);
    return result;
  } catch (error) {
    const failed = {
      status: "error",
      message: error.message || "Theme summary analysis failed.",
      requested_report_count: filteredReports.length,
      analyzed_report_count: 0,
      reports_with_findings: 0,
      requested_peer_report_count: 0,
      analyzed_peer_report_count: 0,
      peer_institutions_analyzed: 0,
      recommendation_count: 0,
      condition_count: 0,
      themes: [],
      recurring_recommendations: [],
      recurring_conditions: [],
      metadata: {},
    };
    state.qualityThemeSummaryCache.set(cacheKey, failed);
    return failed;
  }
}

function qualityReportDecisionYear(report) {
  const year = Number.parseInt(String(report?.decision_date || "").slice(0, 4), 10);
  return Number.isInteger(year) ? year : null;
}

function availableQualityReportYears(reports) {
  return [...new Set(reports.map((report) => qualityReportDecisionYear(report)).filter((year) => Number.isInteger(year)))]
    .sort((left, right) => right - left);
}

function normalizeQualityYearRange(filters) {
  const yearFrom = filters.yearFrom === "all" ? null : Number.parseInt(filters.yearFrom, 10);
  const yearTo = filters.yearTo === "all" ? null : Number.parseInt(filters.yearTo, 10);
  return {
    yearFrom: Number.isInteger(yearFrom) ? yearFrom : null,
    yearTo: Number.isInteger(yearTo) ? yearTo : null,
  };
}

function qualityYearRangeLabel(filters) {
  const normalized = normalizeQualityYearRange(filters);
  if (normalized.yearFrom !== null && normalized.yearTo !== null) {
    return `${normalized.yearFrom}-${normalized.yearTo}`;
  }
  if (normalized.yearFrom !== null) {
    return `From ${normalized.yearFrom}`;
  }
  if (normalized.yearTo !== null) {
    return `Through ${normalized.yearTo}`;
  }
  return "Any year";
}

function isCurrentQualityReport(report) {
  const now = Date.now();
  const decisionTimestamp = parseIsoDateToUtc(report?.decision_date);
  if (decisionTimestamp !== null && decisionTimestamp > now) {
    return false;
  }

  const validToTimestamp = parseIsoDateToUtc(report?.valid_to);
  if (validToTimestamp !== null) {
    return validToTimestamp >= now;
  }

  return decisionTimestamp !== null;
}

function reportMatchesYearRange(report, filters) {
  const normalized = normalizeQualityYearRange(filters);
  if (normalized.yearFrom === null && normalized.yearTo === null) return true;

  const decisionYear = qualityReportDecisionYear(report);
  if (decisionYear === null) return false;

  if (normalized.yearFrom !== null && decisionYear < normalized.yearFrom) return false;
  if (normalized.yearTo !== null && decisionYear > normalized.yearTo) return false;
  return true;
}

function filterQualityReports(reports, filters) {
  return reports.filter((report) => {
    if (filters.scope !== "all" && report.scope !== filters.scope) return false;
    if (filters.decisionTone !== "all" && qualityDecisionTone(report.decision) !== filters.decisionTone) return false;
    if (filters.agency !== "all" && (report.agency || "") !== filters.agency) return false;
    if (!reportMatchesYearRange(report, filters)) return false;
    return true;
  });
}

function buildQualityFilteredReportYears(reports) {
  const years = new Map();

  reports.forEach((report) => {
    const year = qualityReportDecisionYear(report);
    if (year === null) return;

    const row = years.get(year) || {
      year,
      total: 0,
      institutional: 0,
      programme: 0,
      monitoring: 0,
      other: 0,
      conditional: 0,
      negative: 0,
    };

    row.total += 1;
    row[report.scope || "other"] = (row[report.scope || "other"] || 0) + 1;

    const tone = qualityDecisionTone(report.decision);
    if (tone === "conditional") row.conditional += 1;
    if (tone === "negative") row.negative += 1;

    years.set(year, row);
  });

  return [...years.values()].sort((a, b) => b.year - a.year);
}

function qualityReportDetailMarkup(report, analysis = null) {
  if (!report) {
    return `
      <div class="quality-report-detail-card">
        <p class="quality-report-empty">No DEQAR report is selected.</p>
      </div>
    `;
  }

  return `
    <article class="quality-report-detail-card">
      <div class="quality-report-detail-header">
        <div>
          <p class="blocked-kicker">Selected report</p>
          <h3>${escapeHtml(report.report_type || "Report")}</h3>
        </div>
        <div class="quality-report-actions">
          ${
            report.report_url
              ? `<a class="quality-report-link" href="${escapeHtml(report.report_url)}" target="_blank" rel="noreferrer">Open report</a>`
              : ""
          }
          ${qualityAgencyRegisterLinksMarkup(report)}
        </div>
      </div>
      <div class="quality-report-detail-grid">
        ${qualityFactCard("Scope", qualityReportScopeLabel(report.scope))}
        ${qualityFactCard("Decision", report.decision || "n/a")}
        ${qualityFactCard("Agency", report.agency || "n/a")}
        ${qualityFactCard("Agency register", qualityAgencyRegisterValue(report), report.agency_listing_note || "")}
        ${qualityFactCard("Decision date", report.decision_date || "n/a")}
        ${qualityFactCard("Valid to", report.valid_to ? formatCalendarDate(report.valid_to) : "n/a")}
        ${qualityFactCard("Report ID", report.report_id || "n/a")}
      </div>
      ${qualityReportAnalysisMarkup(report, analysis)}
    </article>
  `;
}

function qualityBenchmarkReadinessLabel(value) {
  if (value === "ready") return "Institutional review ready";
  if (value === "partial") return "Programme-heavy only";
  if (value === "limited") return "Needs stronger coverage";
  return "Coverage unknown";
}

function qualityBenchmarkChipClass(value) {
  if (value === "ready") return "quality-chip ready";
  if (value === "partial") return "quality-chip partial";
  return "quality-chip limited";
}

function qualityDecisionTone(value) {
  const normalized = String(value || "").toLowerCase();
  if (!normalized) return "neutral";
  if (normalized.includes("negative") || normalized.includes("withdrawn") || normalized.includes("refused")) {
    return "negative";
  }
  if (normalized.includes("condition") || normalized.includes("restriction")) {
    return "conditional";
  }
  if (normalized.includes("positive")) {
    return "positive";
  }
  return "neutral";
}

function parseIsoDateToUtc(value) {
  if (!value) return null;
  const timestamp = Date.parse(`${value}T00:00:00Z`);
  return Number.isNaN(timestamp) ? null : timestamp;
}

function benchmarkTimelineDomain(peers) {
  const starts = [];
  const ends = [];
  const now = Date.now();

  peers.forEach((peer) => {
    (peer.institutional_reviews || []).forEach((review) => {
      const start = parseIsoDateToUtc(review.decision_date);
      if (!start) return;
      const end = parseIsoDateToUtc(review.valid_to) || (review.is_current ? now : start);
      starts.push(start);
      ends.push(end);
    });
  });

  if (!starts.length || !ends.length) {
    const currentYear = new Date().getUTCFullYear();
    return {
      startMs: Date.UTC(currentYear - 1, 0, 1),
      endMs: Date.UTC(currentYear + 1, 0, 1),
      years: [currentYear - 1, currentYear, currentYear + 1],
    };
  }

  const minYear = new Date(Math.min(...starts)).getUTCFullYear();
  const maxYear = new Date(Math.max(...ends)).getUTCFullYear();
  return {
    startMs: Date.UTC(minYear, 0, 1),
    endMs: Date.UTC(maxYear + 1, 0, 1),
    years: Array.from({ length: maxYear - minYear + 1 }, (_, index) => minYear + index),
  };
}

function timelinePercent(timestamp, startMs, endMs) {
  if (!Number.isFinite(timestamp) || endMs <= startMs) return 0;
  return ((timestamp - startMs) / (endMs - startMs)) * 100;
}

function qualityTimelineMarkup(peers, primaryName = "") {
  const { startMs, endMs, years } = benchmarkTimelineDomain(peers);
  const yearLabels = years.map((year) => `<span>${year}</span>`).join("");
  const yearGrid = years.map(() => "<span></span>").join("");

  const rows = peers
    .map((peer) => {
      const reviews = peer.institutional_reviews || [];
      const reviewMarkup = reviews.length
        ? reviews
            .map((review) => {
              const start = parseIsoDateToUtc(review.decision_date) || startMs;
              const rawEnd = parseIsoDateToUtc(review.valid_to) || (review.is_current ? Date.now() : start);
              const end = Math.max(rawEnd, start + 86400000);
              const left = Math.max(0, Math.min(100, timelinePercent(start, startMs, endMs)));
              const width = Math.max(1.2, Math.min(100 - left, timelinePercent(end, startMs, endMs) - left));
              const label = [
                review.report_type || "Institutional review",
                review.decision || "n/a",
                review.decision_date || "n/a",
                review.valid_to ? `valid to ${review.valid_to}` : review.is_current ? "currently active" : "",
              ]
                .filter(Boolean)
                .join(" • ");
              const className = `quality-timeline-bar ${qualityDecisionTone(review.decision)}${review.is_current ? " current" : ""}`;
              const content = width > 10 ? `<span>${escapeHtml(review.decision_date || "")}</span>` : "";
              if (review.report_url) {
                return `<a class="${className}" href="${escapeHtml(review.report_url)}" target="_blank" rel="noreferrer" style="left:${left}%; width:${width}%;" title="${escapeHtml(label)}">${content}</a>`;
              }
              return `<div class="${className}" style="left:${left}%; width:${width}%;" title="${escapeHtml(label)}">${content}</div>`;
            })
            .join("")
        : `<div class="quality-timeline-empty">No institutional review in this snapshot</div>`;

      return `
        <div class="quality-timeline-row">
          <div class="quality-timeline-peer ${peer.is_primary ? "primary" : ""}">
            <strong>${escapeHtml(peer.display_name || "Unknown institution")}</strong>
            <small>${escapeHtml(peer.is_primary ? "Selected university" : qualityBenchmarkReadinessLabel(peer.readiness))} • ${escapeHtml(String(peer.institutional_report_count || 0))} institutional review${Number(peer.institutional_report_count || 0) === 1 ? "" : "s"}</small>
          </div>
          <div class="quality-timeline-track">
            <div class="quality-timeline-grid">${yearGrid}</div>
            ${reviewMarkup}
          </div>
        </div>
      `;
    })
    .join("");

  return `
    <section class="quality-timeline-card">
      <div class="quality-timeline-header">
        <div>
          <p class="blocked-kicker">QA timeline</p>
          <h3>Institutional review windows across peers</h3>
        </div>
        <p>Focused on ${escapeHtml(primaryName || "the selected university")}. Each band spans the validity window of an institutional review from the DEQAR snapshot. Click a band to open the underlying report.</p>
      </div>
      <div class="quality-timeline-scale">
        <div class="quality-timeline-scale-spacer"></div>
        <div class="quality-timeline-years">${yearLabels}</div>
      </div>
      <div class="quality-timeline-rows">
        ${rows}
      </div>
    </section>
  `;
}

function renderQualityBenchmarkingPanel(chartId, quality) {
  const el = document.getElementById(chartId);
  if (!el) return;
  setChartContentMode(el);

  const metadata = quality.benchmarking.metadata || {};
  const peers = metadata.peers || [];
  if (!peers.length) {
    renderBlockedPanel(chartId, {
      source: quality.benchmarking.source,
      status: quality.benchmarking.status,
      message: quality.benchmarking.message,
    });
    return;
  }

  el.innerHTML = `
    <div class="quality-benchmark-panel">
      <div class="quality-benchmark-summary-grid">
        ${qualityFactCard("Peer set", String(metadata.peer_count ?? peers.length), metadata.peer_group_description || metadata.peer_group_label || "Dynamic peers")}
        ${qualityFactCard("DEQAR matched", String(metadata.matched_peer_count ?? 0), "Institutions with report coverage")}
        ${qualityFactCard("Institutional reviews", String(metadata.institutional_peer_count ?? 0), "Peers with institutional-level QA")}
        ${qualityFactCard("Comparison-ready", String(metadata.ready_peer_count ?? 0), "Peers ready for QA-context comparison")}
      </div>
      <div class="quality-benchmark-summary-grid quality-benchmark-risk-grid">
        ${qualityFactCard("Expiring <12m", String(metadata.expiring_12m_peer_count ?? 0), "Current institutional validity windows")}
        ${qualityFactCard("Expiring <24m", String(metadata.expiring_24m_peer_count ?? 0), "Peers moving toward the next review window")}
        ${qualityFactCard("High watch peers", String(metadata.high_risk_peer_count ?? 0), "Peers with expiry or decision-risk signals")}
        ${qualityFactCard("Open-ended", String(metadata.open_ended_peer_count ?? 0), "Current institutional review with no listed end date")}
      </div>
      ${metadata.peer_selection_note ? qualityNoticeMarkup("Cohort logic", metadata.peer_selection_note, "neutral") : ""}
      ${qualityTimelineMarkup(peers, metadata.primary_institution_name)}
      <div class="table-wrap quality-benchmark-table-wrap">
        <table class="quality-benchmark-table">
          <thead>
            <tr>
              <th>Institution</th>
              <th>Readiness</th>
              <th>Current status</th>
              <th>Validity</th>
              <th>Risk</th>
              <th>Agency</th>
              <th>Date</th>
              <th>Reports</th>
            </tr>
          </thead>
          <tbody>
            ${peers
              .map(
                (peer) => `
                  <tr class="${peer.is_primary ? "quality-peer-row-primary" : ""}">
                    <td>
                      <div class="quality-peer-cell">
                        <strong>
                          ${
                            peer.deqar_url
                              ? `<a class="quality-inline-link" href="${escapeHtml(peer.deqar_url)}" target="_blank" rel="noreferrer">${escapeHtml(peer.display_name || "Unknown institution")}</a>`
                              : escapeHtml(peer.display_name || "Unknown institution")
                          }
                        </strong>
                        <small>
                          ${escapeHtml(qualityPeerMatchSummary(peer))}
                        </small>
                        ${qualityRegistryProfileLabel(peer) ? `<small>${escapeHtml(qualityRegistryProfileLabel(peer))}</small>` : ""}
                        ${peer.peer_selection_note ? `<small>${escapeHtml(peer.peer_selection_note)}</small>` : ""}
                      </div>
                    </td>
                    <td><span class="${qualityBenchmarkChipClass(peer.readiness)}">${escapeHtml(qualityBenchmarkReadinessLabel(peer.readiness))}</span></td>
                    <td>${escapeHtml(peer.current_status || "n/a")}</td>
                    <td>${escapeHtml(qualityPeerValidityLabel(peer))}</td>
                    <td><span class="${qualityRiskChipClass(peer.qa_risk_level)}">${escapeHtml(qualityRiskLevelLabel(peer.qa_risk_level))}</span></td>
                    <td>${escapeHtml(peer.agency || "n/a")}</td>
                    <td>${escapeHtml(peer.decision_date || "n/a")}</td>
                    <td>${escapeHtml(String(peer.report_count ?? 0))}</td>
                  </tr>
                `,
              )
              .join("")}
          </tbody>
        </table>
      </div>
    </div>
  `;
  finalizeChartContentLayout(el);
}

function primaryInstitution() {
  return state.currentInstitutions[0] || null;
}

function selectedInstitutionIds() {
  return new Set(state.currentInstitutions.map((institution) => institution.id));
}

function institutionSelectionSummary() {
  const primary = primaryInstitution();
  if (!primary) return "Select universities";
  if (state.currentInstitutions.length === 1) return primary.display_name;
  return `${primary.display_name} +${state.currentInstitutions.length - 1}`;
}

function updateInstitutionHelp(message) {
  const primary = primaryInstitution();
  if (message) {
    institutionHelp.textContent = message;
    return;
  }
  if (state.activePage?.id === "overview") {
    if (!primary) {
      institutionHelp.textContent = "Choose one or more universities. The Overview page anchors institution-level signals to the first selected university while country metrics stay on Bulgaria and the selected systems.";
      return;
    }
    if (state.currentInstitutions.length === 1) {
      institutionHelp.textContent = `Overview is anchored to ${primary.display_name} for research, EU-project, and quality context. Add more universities if you want to switch the primary institution quickly from the same selector.`;
      return;
    }
    institutionHelp.textContent = `${state.currentInstitutions.length} universities selected. Overview stays anchored to ${primary.display_name}; use Research for side-by-side publication comparisons.`;
    return;
  }
  if (state.activePage?.id === "quality") {
    if (!primary) {
      institutionHelp.textContent = "Choose a university for external QA and benchmarking context. Start typing to search all OpenAlex institutions; the blank menu shows global quick picks.";
      return;
    }
    institutionHelp.textContent = `Quality is focused on ${primary.display_name}. Start typing to search globally; choosing another university recenters the peer timeline and DEQAR context on that institution.`;
    return;
  }
  if (!primary) {
    institutionHelp.textContent = "Choose one or more universities. Start typing to search all OpenAlex institutions; the blank menu shows quick picks.";
    return;
  }
  if (state.currentInstitutions.length === 1) {
    institutionHelp.textContent = `${institutionLabel(primary)} is selected. Add more universities to compare publications and citations.`;
    return;
  }
  institutionHelp.textContent = `${state.currentInstitutions.length} universities selected. KPI cards stay on ${primary.display_name}.`;
}

function closeInstitutionMenu() {
  institutionDropdown.classList.remove("open");
}

function openInstitutionMenu() {
  if (institutionMenu.innerHTML.trim()) {
    institutionDropdown.classList.add("open");
  }
}

function updateInstitutionSelectionUi() {
  institutionSelectionLabel.textContent = institutionSelectionSummary();
  institutionCount.textContent = state.currentInstitutions.length;
  updateInstitutionHelp();
}

function syncCurrentInstitution() {
  state.currentInstitution = primaryInstitution();
  updateInstitutionSelectionUi();
}

function setSelectedInstitutions(institutions, { reload = true } = {}) {
  state.currentInstitutions = institutions;
  syncCurrentInstitution();

  if (reload && state.activePage?.controls?.includes("institution")) {
    void renderCurrentRoute();
  }
}

function renderInstitutionOptions(options, query = "", mode = "browse") {
  state.institutionResults = options;
  state.institutionSearchMode = mode;
  options.forEach((option) => {
    state.institutionOptions.set(option.id, option);
  });

  const selectedIds = selectedInstitutionIds();
  const qualitySingleSelect = state.activePage?.id === "quality";
  const primaryId = primaryInstitution()?.id;
  const menuNote = query
    ? "Search results from OpenAlex. Clear the query to return to quick picks."
    : mode === "featured"
      ? "Dashboard quick picks. Start typing to search all OpenAlex institutions."
      : "Global quick picks across countries. Start typing to search all OpenAlex institutions.";
  const optionsMarkup = options.length
    ? options
        .map(
          (option) => `
            <label class="dropdown-item institution-item" data-id="${option.id}">
              <input
                type="${qualitySingleSelect ? "radio" : "checkbox"}"
                ${qualitySingleSelect ? 'name="quality-institution-focus"' : ""}
                value="${option.id}"
                ${(qualitySingleSelect ? primaryId === option.id : selectedIds.has(option.id)) ? "checked" : ""}
              />
              <span class="institution-item-copy">
                <strong>${option.display_name}</strong>
                <small>${option.country_code || "n/a"} · ${formatValue(option.works_count || 0, "persons")} works</small>
              </span>
            </label>
          `,
        )
        .join("")
    : `<div class="institution-empty">No institutions found for "${query || "the current search"}".</div>`;

  institutionMenu.innerHTML = `
    <input type="text" class="dropdown-search" id="institution-search" placeholder="Search all institutions..." />
    <div class="institution-menu-note">${escapeHtml(menuNote)}</div>
    ${optionsMarkup}
  `;
  const searchInput = institutionMenu.querySelector("#institution-search");
  if (searchInput) {
    searchInput.value = query;
    searchInput.addEventListener("input", (event) => {
      debouncedInstitutionSearch(event.target.value);
    });
  }
  institutionDropdown.classList.add("open");
}

function pinCurrentInstitutions(options) {
  if (!state.currentInstitutions.length) return options;
  const pinned = [];
  const seen = new Set();

  state.currentInstitutions.forEach((institution) => {
    if (!institution || seen.has(institution.id)) return;
    pinned.push(institution);
    seen.add(institution.id);
  });

  options.forEach((institution) => {
    if (seen.has(institution.id)) return;
    pinned.push(institution);
    seen.add(institution.id);
  });

  return pinned;
}

async function searchInstitutions(query = "", { selectFirst = false, reloadOnSelect = false, mode } = {}) {
  const trimmedQuery = query.trim();
  const requestMode = mode || (trimmedQuery ? "search" : "browse");
  const results = await fetchInstitutionSearch(trimmedQuery, requestMode);
  const visibleResults = trimmedQuery ? results : pinCurrentInstitutions(results);
  renderInstitutionOptions(visibleResults, trimmedQuery, requestMode);

  if (selectFirst && results.length) {
    setSelectedInstitutions([results[0]], { reload: reloadOnSelect });
    if (!reloadOnSelect) {
      closeInstitutionMenu();
    }
  }

  return results;
}

async function ensureInstitutionSelection() {
  if (state.currentInstitutions.length) {
    syncCurrentInstitution();
    return state.currentInstitutions;
  }

  const results = await searchInstitutions("", { selectFirst: true, reloadOnSelect: false, mode: "featured" });
  return results.length ? [results[0]] : [];
}

function showSkeleton(containerId, type = "chart") {
  const el = document.getElementById(containerId);
  if (!el) return;
  el.innerHTML = `<div class="loading-skeleton ${type}-skeleton"></div>`;
}

function showError(containerId, message, onRetry) {
  const el = document.getElementById(containerId);
  if (!el) return;
  el.innerHTML = `
    <div class="error-banner">
      <p class="error-message">${message}</p>
      <button type="button" class="retry-btn">Retry</button>
    </div>
  `;
  const btn = el.querySelector(".retry-btn");
  if (btn && onRetry) btn.addEventListener("click", onRetry);
}

function showKpiSkeletons() {
  kpiGrid.innerHTML = Array(4)
    .fill('<article class="kpi-card"><div class="loading-skeleton kpi-skeleton"></div></article>')
    .join("");
}

function disposeCharts() {
  Object.entries(charts).forEach(([key, chart]) => {
    chart.dispose();
    delete charts[key];
  });
}

function initChart(id) {
  const el = document.getElementById(id);
  if (!el) return null;
  const existing = echarts.getInstanceByDom(el);
  if (existing) existing.dispose();
  el.innerHTML = "";
  charts[id] = echarts.init(el);
  return charts[id];
}

function latestYear(rows) {
  return rows.length ? Math.max(...rows.map((row) => row.year)) : null;
}

function latestByCountry(rows) {
  return rows.reduce((acc, row) => {
    if (!acc[row.country] || row.year > acc[row.country].year) {
      acc[row.country] = row;
    }
    return acc;
  }, {});
}

function countryRows(rows, code) {
  return rows.filter((row) => row.country === code).sort((a, b) => a.year - b.year);
}

function pointForYear(rows, country, year) {
  return rows.find((row) => row.country === country && row.year === year) || null;
}

function firstAvailableCountry(rows, preferredCountries = state.currentCountries) {
  const countries = [...new Set(rows.map((row) => row.country))];
  return preferredCountries.find((code) => countries.includes(code)) || countries[0] || null;
}

function makeTrendNarrative(rows, indicator) {
  const bgRows = countryRows(rows, "BG");
  if (!bgRows.length) {
    return `No Bulgaria series is available for ${indicator.title.toLowerCase()} in the selected range.`;
  }

  const latest = bgRows[bgRows.length - 1];
  if (bgRows.length === 1) {
    return `Latest Bulgaria observation is ${formatValue(latest.value, indicator.unit)} in ${latest.year}.`;
  }

  const baseline = bgRows[0];
  const delta = latest.value - baseline.value;
  if (delta === 0) {
    return `Bulgaria is flat versus ${baseline.year}. Latest observation is ${formatValue(latest.value, indicator.unit)} in ${latest.year}.`;
  }

  if (indicator.unit === "percent") {
    return `Bulgaria is ${delta > 0 ? "up" : "down"} ${Math.abs(delta).toFixed(1)} percentage points since ${baseline.year}. Latest observation is ${formatValue(latest.value, indicator.unit)} in ${latest.year}.`;
  }

  return `Bulgaria is ${delta > 0 ? "up" : "down"} ${formatValue(Math.abs(delta), indicator.unit)} since ${baseline.year}. Latest observation is ${formatValue(latest.value, indicator.unit)} in ${latest.year}.`;
}

function makeBarNarrative(rows, indicator) {
  if (!rows.length) {
    return `No data available for ${indicator.title.toLowerCase()} in the selected range.`;
  }

  if (indicator.unit === "persons") {
    const latest = latestYear(rows);
    return `Latest benchmark year is ${latest}. Use these absolute counts as system-size context rather than a performance comparison.`;
  }

  const latest = latestByCountry(rows);
  const bg = latest.BG;
  const eu = latest.EU27_2020;
  if (bg && eu) {
    const diff = bg.value - eu.value;
    const direction = diff >= 0 ? "above" : "below";
    const difference = indicator.unit === "percent" ? `${Math.abs(diff).toFixed(1)} percentage points` : formatValue(Math.abs(diff), indicator.unit);
    return `In ${bg.year}, Bulgaria is ${difference} ${direction} the EU benchmark for ${indicator.title.toLowerCase()}.`;
  }

  const latestRow = rows.slice().sort((a, b) => b.year - a.year)[0];
  return `Latest observed value is ${formatValue(latestRow.value, indicator.unit)} for ${countryLabel(latestRow.country)} in ${latestRow.year}.`;
}

function breakdownSeriesForLatest(rows, preferredCountries = state.currentCountries) {
  const focusCountry = firstAvailableCountry(rows, preferredCountries);
  if (!focusCountry) {
    return { focusCountry: null, year: null, labels: [], values: [] };
  }

  const focusRows = rows.filter((row) => row.country === focusCountry);
  const year = latestYear(focusRows);
  const latestRows = focusRows
    .filter((row) => row.year === year)
    .sort((a, b) => b.value - a.value);

  return {
    focusCountry,
    year,
    labels: latestRows.map((row) => row.series_label || row.series_key || "Series"),
    values: latestRows.map((row) => row.value),
  };
}

function derivedEnrolmentRateRows(dataMap) {
  const enrolmentRows = dataMap.tertiary_enrolment_total || [];
  const population18Rows = dataMap.population_18_24 || [];
  const population25Rows = dataMap.population_25_34 || [];
  const countries = [...new Set(enrolmentRows.map((row) => row.country))];

  return countries
    .map((country) => {
      const enrolmentYears = new Set(countryRows(enrolmentRows, country).map((row) => row.year));
      const population18Years = new Set(countryRows(population18Rows, country).map((row) => row.year));
      const population25Years = new Set(countryRows(population25Rows, country).map((row) => row.year));
      const commonYears = [...enrolmentYears].filter((year) => population18Years.has(year) && population25Years.has(year));
      if (!commonYears.length) return null;

      const year = Math.max(...commonYears);
      const enrolment = pointForYear(enrolmentRows, country, year);
      const population18 = pointForYear(population18Rows, country, year);
      const population25 = pointForYear(population25Rows, country, year);
      const denominator = (population18?.value || 0) + (population25?.value || 0);
      if (!enrolment || denominator <= 0) return null;

      return {
        country,
        year,
        value: (enrolment.value / denominator) * 100,
      };
    })
    .filter(Boolean);
}

function seriesFor(rows) {
  const years = [...new Set(rows.map((row) => row.year))].sort((a, b) => a - b);
  const countries = [...new Set(rows.map((row) => row.country))];
  return {
    years,
    series: countries.map((country, i) => ({
      name: countryLabel(country),
      type: "line",
      smooth: true,
      emphasis: { focus: "series" },
      lineStyle: { width: country === "BG" ? 3 : 2 },
      symbolSize: country === "BG" ? 6 : 4,
      color: CHART_COLORS[i % CHART_COLORS.length],
      data: years.map((year) => {
        const point = rows.find((row) => row.country === country && row.year === year);
        return point ? point.value : null;
      }),
    })),
  };
}

function barSeriesForLatest(rows) {
  const latest = Object.values(latestByCountry(rows)).sort((a, b) => b.value - a.value);
  return {
    countries: latest.map((item) => item.country),
    labels: latest.map((item) => countryLabel(item.country)),
    values: latest.map((item) => item.value),
  };
}

function renderTrendChart(chartId, rows, indicator) {
  const chart = charts[chartId] || initChart(chartId);
  if (!chart) return;
  const { years, series } = seriesFor(rows);
  chart.setOption(
    {
      color: CHART_COLORS,
      animationDuration: 600,
      tooltip: {
        trigger: "axis",
        backgroundColor: "#fff",
        borderColor: "#e2e5ea",
        textStyle: { color: "#1a1d21", fontSize: 12 },
        valueFormatter: (val) => formatValue(val, indicator.unit),
      },
      legend: {
        top: 0,
        textStyle: { fontSize: 12, color: "#5f6673" },
      },
      grid: { left: 16, right: 16, top: 48, bottom: 8, containLabel: true },
      xAxis: {
        type: "category",
        data: years,
        axisLine: { lineStyle: { color: "#e2e5ea" } },
        axisLabel: { color: "#5f6673" },
      },
      yAxis: {
        type: "value",
        name: indicator.unit || "",
        nameTextStyle: { color: "#5f6673", fontSize: 11 },
        splitLine: { lineStyle: { color: "#f0f1f3" } },
        axisLabel: {
          color: "#5f6673",
          formatter: (value) => (value >= 1000 ? `${(value / 1000).toFixed(0)}k` : value),
        },
      },
      series,
    },
    true,
  );
}

function renderBarChart(chartId, rows, indicator) {
  const chart = charts[chartId] || initChart(chartId);
  if (!chart) return;
  const latest = barSeriesForLatest(rows);
  chart.setOption(
    {
      animationDuration: 600,
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        backgroundColor: "#fff",
        borderColor: "#e2e5ea",
        textStyle: { color: "#1a1d21", fontSize: 12 },
        valueFormatter: (val) => formatValue(val, indicator.unit),
      },
      grid: { left: 16, right: 16, top: 8, bottom: 8, containLabel: true },
      xAxis: {
        type: "value",
        name: indicator.unit || "",
        nameTextStyle: { color: "#5f6673", fontSize: 11 },
        splitLine: { lineStyle: { color: "#f0f1f3" } },
        axisLabel: { color: "#5f6673" },
      },
      yAxis: {
        type: "category",
        data: latest.labels,
        axisLine: { lineStyle: { color: "#e2e5ea" } },
        axisLabel: { color: "#5f6673" },
      },
      series: [
        {
          type: "bar",
          data: latest.values,
          barMaxWidth: 32,
          itemStyle: {
            borderRadius: [0, 4, 4, 0],
            color: (params) => (latest.countries[params.dataIndex] === "BG" ? BG_HIGHLIGHT : BG_OTHER),
          },
          label: {
            show: true,
            position: "right",
            fontSize: 11,
            color: "#5f6673",
            formatter: (params) => formatValue(params.value, indicator.unit),
          },
        },
      ],
    },
    true,
  );
}

function renderBreakdownBarChart(chartId, rows, indicator) {
  const chart = charts[chartId] || initChart(chartId);
  if (!chart) return;

  const latest = breakdownSeriesForLatest(rows);
  chart.setOption(
    {
      animationDuration: 600,
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        backgroundColor: "#fff",
        borderColor: "#e2e5ea",
        textStyle: { color: "#1a1d21", fontSize: 12 },
        valueFormatter: (val) => formatValue(val, indicator.unit),
      },
      grid: { left: 16, right: 16, top: 8, bottom: 8, containLabel: true },
      xAxis: {
        type: "value",
        name: indicator.unit || "",
        nameTextStyle: { color: "#5f6673", fontSize: 11 },
        splitLine: { lineStyle: { color: "#f0f1f3" } },
        axisLabel: {
          color: "#5f6673",
          formatter: (value) => (value >= 1000 ? `${(value / 1000).toFixed(0)}k` : value),
        },
      },
      yAxis: {
        type: "category",
        data: latest.labels,
        axisLine: { lineStyle: { color: "#e2e5ea" } },
        axisLabel: { color: "#5f6673", width: 180, overflow: "truncate" },
      },
      series: [
        {
          type: "bar",
          data: latest.values,
          barMaxWidth: 24,
          itemStyle: {
            borderRadius: [0, 4, 4, 0],
            color: CHART_COLORS[0],
          },
          label: {
            show: true,
            position: "right",
            fontSize: 11,
            color: "#5f6673",
            formatter: (params) => formatValue(params.value, indicator.unit),
          },
        },
      ],
    },
    true,
  );
}

function filterResearchPoints(points) {
  return (points || [])
    .filter((point) => point.year >= state.currentYearRange.from && point.year <= state.currentYearRange.to)
    .sort((a, b) => a.year - b.year);
}

function renderResearchMultiSeriesChart(chartId, summaries, metricKey, unit) {
  const chart = charts[chartId] || initChart(chartId);
  if (!chart) return;

  const years = [...new Set(summaries.flatMap((summary) => filterResearchPoints(summary.counts_by_year).map((point) => point.year)))].sort(
    (a, b) => a - b,
  );
  const primaryId = primaryInstitution()?.id;
  const series = summaries.map((summary, index) => {
    const points = filterResearchPoints(summary.counts_by_year);
    return {
      name: summary.institution.display_name,
      type: "line",
      smooth: true,
      emphasis: { focus: "series" },
      symbolSize: summary.institution.id === primaryId ? 6 : 4,
      lineStyle: { width: summary.institution.id === primaryId ? 3 : 2 },
      color: CHART_COLORS[index % CHART_COLORS.length],
      data: years.map((year) => {
        const point = points.find((item) => item.year === year);
        return point ? point[metricKey] : null;
      }),
    };
  });

  chart.setOption(
    {
      animationDuration: 600,
      color: CHART_COLORS,
      tooltip: {
        trigger: "axis",
        backgroundColor: "#fff",
        borderColor: "#e2e5ea",
        textStyle: { color: "#1a1d21", fontSize: 12 },
        valueFormatter: (value) => formatValue(value, unit),
      },
      legend: {
        top: 0,
        textStyle: { fontSize: 12, color: "#5f6673" },
      },
      grid: { left: 16, right: 16, top: 48, bottom: 8, containLabel: true },
      xAxis: {
        type: "category",
        data: years,
        axisLine: { lineStyle: { color: "#e2e5ea" } },
        axisLabel: { color: "#5f6673" },
      },
      yAxis: {
        type: "value",
        name: unit || "",
        nameTextStyle: { color: "#5f6673", fontSize: 11 },
        splitLine: { lineStyle: { color: "#f0f1f3" } },
        axisLabel: {
          color: "#5f6673",
          formatter: (value) => (value >= 1000 ? `${(value / 1000).toFixed(0)}k` : value),
        },
      },
      series,
    },
    true,
  );
}

function renderBlockedPanel(chartId, status) {
  const el = document.getElementById(chartId);
  if (!el) return;
  setChartContentMode(el);
  const headlineByStatus = {
    active: "Live dataset",
    blocked_by_credentials: "Credential-gated",
    processing: "Extraction processing",
    ready: "Ready",
    unavailable: "Temporarily unavailable",
  };
  el.innerHTML = `
    <div class="blocked-panel">
      <p class="blocked-kicker">${escapeHtml(status.source).toUpperCase()}</p>
      <h3>${headlineByStatus[status.status] || "Not implemented yet"}</h3>
      <p>${escapeHtml(status.message)}</p>
      ${
        status.details?.length
          ? `<div class="blocked-panel-details">${status.details.map((detail) => `<p>${escapeHtml(detail)}</p>`).join("")}</div>`
          : ""
      }
    </div>
  `;
  finalizeChartContentLayout(el);
}

function renderCordisProjectsPanel(chartId, status) {
  const el = document.getElementById(chartId);
  if (!el) return;
  setChartContentMode(el);

  if (status.status !== "active") {
    renderCordisProjectsActionPanel(chartId, status);
    return;
  }

  const metadata = status.metadata || {};
  const projects = status.projects || [];
  const rows = projects.length
    ? projects
        .map(
          (project) => `
            <tr>
              <td>
                <div class="project-table-cell">
                  <a class="cordis-project-link" href="${escapeHtml(project.cordis_url || "#")}" target="_blank" rel="noreferrer">
                    <strong>${escapeHtml(project.acronym || project.project_id)}</strong>
                  </a>
                  <small>${escapeHtml(project.title || "Untitled project")}</small>
                  ${
                    project.topic
                      ? `<small>${escapeHtml(project.topic_title ? `${project.topic} · ${project.topic_title}` : project.topic)}</small>`
                      : ""
                  }
                </div>
              </td>
              <td>
                <div class="project-table-cell compact">
                  <strong>${escapeHtml(project.institution_role || "n/a")}</strong>
                  <small>${escapeHtml(project.institution_name || "")}</small>
                </div>
              </td>
              <td>
                <div class="project-table-cell compact">
                  <strong>${escapeHtml(project.framework_programme || "n/a")}</strong>
                  <small>${escapeHtml(project.funding_scheme || "Funding scheme unavailable")}</small>
                </div>
              </td>
              <td>
                <div class="project-table-cell compact">
                  <strong>${escapeHtml(project.start_date || "n/a")}</strong>
                  <small>${escapeHtml(project.end_date || "End date unavailable")}</small>
                </div>
              </td>
              <td>
                <div class="project-table-cell compact">
                  <strong>${formatCurrency(project.institution_ec_contribution)}</strong>
                  <small>Institution</small>
                  <small>Project max ${formatCurrency(project.project_ec_max_contribution)}</small>
                </div>
              </td>
            </tr>
          `,
        )
        .join("")
    : `
      <tr>
        <td colspan="5">
          <div class="project-table-empty">No direct institution matches were found in the CORDIS organization table for this export.</div>
        </td>
      </tr>
    `;

  el.innerHTML = `
    <div class="project-summary-grid">
      <article class="project-summary-card">
        <span>Broad CORDIS project hits</span>
        <strong>${formatValue(Number(metadata.project_record_count || metadata.record_count || 0), "persons")}</strong>
      </article>
      <article class="project-summary-card">
        <span>Direct institution matches</span>
        <strong>${formatValue(Number(metadata.direct_match_project_count || projects.length || 0), "persons")}</strong>
      </article>
      <article class="project-summary-card">
        <span>Coordinator roles</span>
        <strong>${formatValue(Number(metadata.coordinator_project_count || 0), "persons")}</strong>
      </article>
      <article class="project-summary-card">
        <span>Partner countries</span>
        <strong>${formatValue(Number(metadata.partner_country_count || 0), "persons")}</strong>
      </article>
    </div>
    <div class="cordis-meta">
      <p>${escapeHtml(status.message)}</p>
      <p>Query: <code>${escapeHtml(metadata.query || "")}</code>${metadata.task_id ? ` · Task ${escapeHtml(metadata.task_id)}` : ""}</p>
    </div>
    <div class="table-wrap">
      <table class="cordis-project-table">
        <thead>
          <tr>
            <th>Project</th>
            <th>${escapeHtml(metadata.institution_name || "Institution")} role</th>
            <th>Programme</th>
            <th>Dates</th>
            <th>Contribution</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
  finalizeChartContentLayout(el);
}

function renderCordisProjectsActionPanel(chartId, status) {
  const el = document.getElementById(chartId);
  if (!el) return;
  setChartContentMode(el);

  if (status.status === "blocked_by_credentials") {
    renderBlockedPanel(chartId, status);
    return;
  }

  const primary = primaryInstitution();
  const actionLabel = status.status === "processing" ? "Refresh CORDIS status" : "Prepare CORDIS project data";
  el.innerHTML = `
    <div class="blocked-panel cordis-action-panel">
      <p class="blocked-kicker">${escapeHtml(status.source).toUpperCase()}</p>
      <h3>${status.status === "processing" ? "Extraction processing" : "Project detail not ready yet"}</h3>
      <p>${escapeHtml(status.message)}</p>
      ${
        primary
          ? `<button type="button" class="cordis-action-btn">${escapeHtml(actionLabel)}</button>`
          : ""
      }
    </div>
  `;

  const button = el.querySelector(".cordis-action-btn");
  if (!button || !primary) return;

  button.addEventListener("click", async () => {
    button.disabled = true;
    button.textContent = status.status === "processing" ? "Refreshing..." : "Starting extraction...";
    try {
      if (status.status === "processing") {
        await renderCurrentRoute();
        return;
      }
      await fetchProjectsExtraction(primary.id);
      await renderCurrentRoute();
    } catch (error) {
      alert(`CORDIS action failed: ${error.message}`);
      button.disabled = false;
      button.textContent = actionLabel;
    }
  });
  finalizeChartContentLayout(el);
}

async function getResearchSummaryCached(institutionId) {
  if (state.researchSummaryCache.has(institutionId)) {
    return state.researchSummaryCache.get(institutionId);
  }
  const summary = await fetchResearchSummary(institutionId);
  state.researchSummaryCache.set(institutionId, summary);
  return summary;
}

async function getQualityStatusCached(institutionId, peerMode = state.qualityPeerMode) {
  const cacheKey = `${institutionId}::${peerMode}`;
  if (state.qualityStatusCache.has(cacheKey)) {
    return state.qualityStatusCache.get(cacheKey);
  }
  const quality = await fetchQualityStatus(institutionId, peerMode);
  state.qualityStatusCache.set(cacheKey, quality);
  return quality;
}

async function getProjectsStatusCached(institutionId) {
  if (state.projectsStatusCache.has(institutionId)) {
    return state.projectsStatusCache.get(institutionId);
  }
  const status = await fetchProjectsStatus(institutionId);
  state.projectsStatusCache.set(institutionId, status);
  return status;
}

function overviewResponseRows(responseMap, indicatorId) {
  return responseMap?.[indicatorId]?.rows || [];
}

function overviewLatestIndicatorYear(responseMap, indicatorIds) {
  const years = indicatorIds
    .map((indicatorId) => responseMap?.[indicatorId]?.metadata?.latest_year ?? latestYear(overviewResponseRows(responseMap, indicatorId)))
    .filter(Number.isFinite);
  return years.length ? Math.max(...years) : null;
}

function overviewComparisonRangeCopy(rows, unit) {
  const latest = latestByCountry(rows);
  const comparisonRows = state.currentCountries
    .filter((code) => code !== "BG" && code !== "EU27_2020")
    .map((code) => latest[code])
    .filter(Boolean);

  if (!comparisonRows.length) return "No country comparisons selected beyond the EU aggregate";
  if (comparisonRows.length === 1) {
    const row = comparisonRows[0];
    return `${countryLabel(row.country)} at ${formatValue(row.value, unit)} in ${row.year}`;
  }

  const values = comparisonRows.map((row) => row.value);
  return `Selected-system range ${formatValue(Math.min(...values), unit)} to ${formatValue(Math.max(...values), unit)}`;
}

function overviewEuComparisonCopy(rows, unit, label = "EU benchmark") {
  const latest = latestByCountry(rows);
  const bg = latest.BG;
  const benchmark = latest.EU27_2020;
  if (!bg || !benchmark) return `${label} unavailable`;
  const diff = bg.value - benchmark.value;
  const direction = diff >= 0 ? "above" : "below";
  return `${Math.abs(diff).toFixed(1)} ${unit === "percent" ? "pp" : unit} ${direction} ${label.toLowerCase()}`;
}

function overviewDeltaSincePrevious(rows, unit, country = "BG") {
  const { latest, previous } = latestAndPrevious(rows, country);
  if (!latest || !previous) return "No earlier comparison";
  return `${formatDelta(latest.value - previous.value, unit)} vs prior year`;
}

function overviewMetricCard({ kicker, title, value, subtitle, detail, tone = "neutral" }) {
  return `
    <article class="overview-metric-card ${tone}">
      <p class="overview-metric-kicker">${escapeHtml(kicker)}</p>
      <h3>${escapeHtml(title)}</h3>
      <strong>${escapeHtml(value)}</strong>
      <span>${escapeHtml(subtitle)}</span>
      <p>${escapeHtml(detail)}</p>
    </article>
  `;
}

function overviewMiniChartState(containerId, title, message) {
  const el = document.getElementById(containerId);
  if (!el) return;
  el.innerHTML = `
    <div class="overview-mini-state">
      <strong>${escapeHtml(title)}</strong>
      <p>${escapeHtml(message)}</p>
    </div>
  `;
}

function qualityCurrentStatusValue(quality) {
  const decision = quality?.deqar?.current_status || quality?.neaa?.metadata?.current_status || "";
  if (!decision) return pageStatusLabel(qualityOverallSourceStatus(quality));
  const tone = qualityDecisionTone(decision);
  if (tone === "positive") return "Positive";
  if (tone === "conditional") return "Conditional";
  if (tone === "negative") return "Negative";
  return truncateText(decision, 36);
}

function renderOverviewNationalSnapshotPanel(chartId, responseMap) {
  const el = document.getElementById(chartId);
  if (!el) return;
  setChartContentMode(el);

  const popRows = overviewResponseRows(responseMap, "population_18_24");
  const attainmentRows = overviewResponseRows(responseMap, "tertiary_attainment_25_34");
  const graduateRows = overviewResponseRows(responseMap, "recent_graduate_employment_rate");
  const internationalRows = overviewResponseRows(responseMap, "international_students_share");
  const researcherRows = overviewResponseRows(responseMap, "researchers_fte");
  const rdRows = overviewResponseRows(responseMap, "rd_expenditure_gdp");

  const pop = latestAndPrevious(popRows);
  const attainment = latestByCountry(attainmentRows);
  const graduate = latestByCountry(graduateRows);
  const international = latestAndPrevious(internationalRows);
  const researchers = latestAndPrevious(researcherRows);
  const rd = latestByCountry(rdRows);

  const cards = [
    overviewMetricCard({
      kicker: "Demand context",
      title: "Population aged 18-24",
      value: pop.latest ? formatValue(pop.latest.value, "persons") : "n/a",
      subtitle: pop.latest ? `Bulgaria • ${pop.latest.year}` : "Bulgaria",
      detail: popRows.length
        ? `${overviewDeltaSincePrevious(popRows, "persons")} • ${overviewComparisonRangeCopy(popRows, "persons")}`
        : "Population context unavailable in the selected range",
    }),
    overviewMetricCard({
      kicker: "Outcomes benchmark",
      title: "Tertiary attainment age 25-34",
      value: attainment.BG ? formatValue(attainment.BG.value, "percent") : "n/a",
      subtitle: attainment.BG ? `Bulgaria • ${attainment.BG.year}` : "Bulgaria",
      detail: attainmentRows.length
        ? `${overviewEuComparisonCopy(attainmentRows, "percent")} • ${overviewDeltaSincePrevious(attainmentRows, "percent")}`
        : "Attainment benchmark unavailable in the selected range",
      tone: attainment.BG && attainment.EU27_2020 ? deltaClassFor(attainment.BG.value - attainment.EU27_2020.value) : "neutral",
    }),
    overviewMetricCard({
      kicker: "Graduate outcomes",
      title: "Recent graduate employment",
      value: graduate.BG ? formatValue(graduate.BG.value, "percent") : "n/a",
      subtitle: graduate.BG ? `Bulgaria • ${graduate.BG.year}` : "Bulgaria",
      detail: graduateRows.length
        ? `${overviewEuComparisonCopy(graduateRows, "percent")} • ${overviewDeltaSincePrevious(graduateRows, "percent")}`
        : "Graduate-employment benchmark unavailable in the selected range",
      tone: graduate.BG && graduate.EU27_2020 ? deltaClassFor(graduate.BG.value - graduate.EU27_2020.value) : "neutral",
    }),
    overviewMetricCard({
      kicker: "International demand",
      title: "International tertiary students",
      value: international.latest ? formatValue(international.latest.value, "persons") : "n/a",
      subtitle: international.latest ? `Bulgaria • ${international.latest.year}` : "Bulgaria",
      detail: internationalRows.length
        ? `${overviewDeltaSincePrevious(internationalRows, "persons")} • ${overviewComparisonRangeCopy(internationalRows, "persons")}`
        : "International-demand context unavailable in the selected range",
    }),
    overviewMetricCard({
      kicker: "Research context",
      title: "R&D expenditure",
      value: rd.BG ? formatValue(rd.BG.value, "percent") : "n/a",
      subtitle: rd.BG ? `Bulgaria • ${rd.BG.year}` : "Bulgaria",
      detail: rdRows.length
        ? `${overviewEuComparisonCopy(rdRows, "percent")} • ${overviewDeltaSincePrevious(rdRows, "percent")}`
        : "R&D expenditure benchmark unavailable in the selected range",
      tone: rd.BG && rd.EU27_2020 ? deltaClassFor(rd.BG.value - rd.EU27_2020.value) : "neutral",
    }),
    overviewMetricCard({
      kicker: "Research context",
      title: "Researchers (FTE)",
      value: researchers.latest ? formatValue(researchers.latest.value, "persons") : "n/a",
      subtitle: researchers.latest ? `Bulgaria • ${researchers.latest.year}` : "Bulgaria",
      detail: researcherRows.length
        ? `${overviewDeltaSincePrevious(researcherRows, "persons")} • ${overviewComparisonRangeCopy(researcherRows, "persons")}`
        : "Research-labour context unavailable in the selected range",
    }),
  ];

  el.innerHTML = `
    <div class="overview-metric-grid">
      ${cards.join("")}
    </div>
  `;
  finalizeChartContentLayout(el);
}

function renderOverviewTrendWatchPanel(chartId, responseMap) {
  const el = document.getElementById(chartId);
  if (!el) return;
  setChartContentMode(el);

  const trendSpecs = [
    {
      indicatorId: "population_18_24",
      title: "Population aged 18-24",
      description: "Demand base across the selected comparison systems.",
    },
    {
      indicatorId: "international_students_share",
      title: "International tertiary students",
      description: "Inbound international demand signal at system level.",
    },
    {
      indicatorId: "tertiary_attainment_25_34",
      title: "Tertiary attainment age 25-34",
      description: "Young-adult attainment benchmark versus selected systems.",
    },
    {
      indicatorId: "recent_graduate_employment_rate",
      title: "Recent graduate employment rate",
      description: "Early-career labour-market outcome signal.",
    },
  ];

  el.innerHTML = `
    <div class="overview-trend-grid">
      ${trendSpecs
        .map(
          (spec) => `
            <article class="overview-trend-card">
              <div class="overview-trend-header">
                <div>
                  <h3>${escapeHtml(spec.title)}</h3>
                  <p>${escapeHtml(spec.description)}</p>
                </div>
              </div>
              <div id="${chartId}-${spec.indicatorId}" class="chart overview-mini-chart"></div>
              <p class="chart-note" id="note-${chartId}-${spec.indicatorId}"></p>
            </article>
          `,
        )
        .join("")}
    </div>
  `;

  trendSpecs.forEach((spec) => {
    const rows = overviewResponseRows(responseMap, spec.indicatorId);
    const response = responseMap?.[spec.indicatorId];
    const innerChartId = `${chartId}-${spec.indicatorId}`;
    const noteEl = document.getElementById(`note-${chartId}-${spec.indicatorId}`);

    if (!rows.length || !response?.indicator) {
      overviewMiniChartState(innerChartId, spec.title, "No data is available for the current filter set.");
      if (noteEl) noteEl.textContent = "Adjust the time range or comparison systems to repopulate this trend.";
      return;
    }

    renderTrendChart(innerChartId, rows, response.indicator);
    if (noteEl) noteEl.textContent = makeTrendNarrative(rows, response.indicator);
  });

  finalizeChartContentLayout(el);
}

function renderOverviewInstitutionBriefPanel(chartId, primary, summary, projects, quality) {
  const el = document.getElementById(chartId);
  if (!el) return;
  setChartContentMode(el);

  const researchPoints = summary ? filterResearchPoints(summary.counts_by_year || []) : [];
  const latestResearchPoint = researchPoints[researchPoints.length - 1] || null;
  const researchBaseline = researchPoints[0] || null;
  const publicationsDelta = latestResearchPoint && researchBaseline && latestResearchPoint.year !== researchBaseline.year
    ? latestResearchPoint.works_count - researchBaseline.works_count
    : null;

  const projectCount = Number(projects?.metadata?.direct_match_project_count || projects?.projects?.length || 0);
  const qaMetadata = quality?.deqar?.metadata || {};
  const qaValidity = quality?.deqar?.status === "active"
    ? qualityValidityCountdownLabel(qaMetadata)
    : (quality?.deqar?.metadata?.next_step || quality?.deqar?.summary || "Quality context unavailable");

  const researchFacts = summary
    ? [
        qualityFactCard(
          "Publications",
          latestResearchPoint ? formatValue(latestResearchPoint.works_count, "persons") : "n/a",
          latestResearchPoint ? `${latestResearchPoint.year}${publicationsDelta === null ? "" : ` • ${formatDelta(publicationsDelta, "persons")} since ${researchBaseline.year}`}` : "No publication-year data in range",
        ),
        qualityFactCard(
          "Total citations",
          formatValue(summary.cited_by_count || 0, "persons"),
          summary.summary_stats?.["2yr_mean_citedness"] != null ? `2-year mean citedness ${Number(summary.summary_stats["2yr_mean_citedness"]).toFixed(2)}` : "Mean citedness unavailable",
        ),
        qualityFactCard(
          "h-index",
          summary.summary_stats?.h_index != null ? formatValue(summary.summary_stats.h_index, "persons") : "n/a",
          summary.summary_stats?.i10_index != null ? `${formatValue(summary.summary_stats.i10_index, "persons")} works with 10+ citations` : "i10-index unavailable",
        ),
        qualityFactCard(
          "Open-access share",
          latestResearchPoint?.open_access_share != null ? formatValue(latestResearchPoint.open_access_share, "percent") : "n/a",
          latestResearchPoint ? `${latestResearchPoint.year} publication year` : "Open-access share unavailable",
        ),
      ].join("")
    : [
        qualityFactCard("Publications", "n/a", "OpenAlex summary unavailable"),
        qualityFactCard("Total citations", "n/a", "OpenAlex summary unavailable"),
        qualityFactCard("h-index", "n/a", "OpenAlex summary unavailable"),
        qualityFactCard("Open-access share", "n/a", "OpenAlex summary unavailable"),
      ].join("");

  const projectFacts = [
    qualityFactCard(
      "Project status",
      pageStatusLabel(projects?.status || "unavailable"),
      projects?.status === "active" ? `${projectCount} direct institution matches in the latest CORDIS export` : (projects?.message || "CORDIS project status unavailable"),
    ),
    qualityFactCard(
      "Direct matches",
      projects?.status === "active" ? formatValue(projectCount, "persons") : "n/a",
      projects?.status === "active" ? `Query ${projects.metadata?.query || "n/a"}` : (projects?.metadata?.query || "Awaiting a completed extraction"),
    ),
    qualityFactCard(
      "Coordinator roles",
      projects?.status === "active" ? formatValue(Number(projects.metadata?.coordinator_project_count || 0), "persons") : "n/a",
      projects?.status === "active" ? `${formatValue(Number(projects.metadata?.partner_country_count || 0), "persons")} partner countries in the current export` : (projects?.metadata?.next_step || "No project extraction ready yet"),
    ),
    qualityFactCard(
      "Latest project start",
      projects?.status === "active" && projects?.metadata?.latest_start_year ? String(projects.metadata.latest_start_year) : "n/a",
      projects?.status === "active" ? `${projects.metadata?.framework_counts ? `${Object.keys(projects.metadata.framework_counts).length} framework buckets` : "Framework mix unavailable"}` : "Projects remain credentialed and extraction-backed",
    ),
  ].join("");

  const qualityFacts = [
    qualityFactCard(
      "QA sources",
      qualitySourceTagLabel(quality),
      quality?.neaa?.status === "active" ? "Bulgarian local overlay active" : "External QA context",
    ),
    qualityFactCard(
      "Current decision",
      qualityCurrentStatusValue(quality),
      quality?.deqar?.decision_date ? `Anchor decision ${formatCalendarDate(quality.deqar.decision_date)}` : (quality?.neaa?.metadata?.decision_date_text || "Decision date unavailable"),
    ),
    qualityFactCard(
      "Institutional validity",
      qaValidity,
      quality?.deqar?.status === "active" ? qualityInstitutionalValidityNote(qaMetadata) : (quality?.deqar?.metadata?.coverage_notice || ""),
    ),
    qualityFactCard(
      "Reports indexed",
      formatValue(Number(quality?.metadata?.report_count || 0), "persons"),
      quality?.deqar?.status === "active" ? qualityCoverageMixLabel(qaMetadata) : (quality?.benchmarking?.message || "Benchmark readiness unavailable"),
    ),
  ].join("");

  el.innerHTML = `
    <div class="overview-institution-grid">
      <article class="overview-section-card">
        <div class="overview-section-header">
          <div>
            <p class="blocked-kicker">OpenAlex</p>
            <h3>${escapeHtml(primary?.display_name || "Selected university")}</h3>
          </div>
          <div class="indicator-tag">Research visibility</div>
        </div>
        <div class="overview-section-facts">
          ${researchFacts}
        </div>
        <p class="chart-note">
          ${escapeHtml(summary ? "These indicators stay anchored to the first selected university and should be read as visibility signals, not a full research evaluation." : "Select or reload a university to restore the institution-level research summary.")}
        </p>
      </article>
      <article class="overview-section-card">
        <div class="overview-section-header">
          <div>
            <p class="blocked-kicker">CORDIS</p>
            <h3>EU project activity</h3>
          </div>
          <div class="indicator-tag">${escapeHtml(pageStatusLabel(projects?.status || "unavailable"))}</div>
        </div>
        <div class="overview-section-facts">
          ${projectFacts}
        </div>
        <p class="chart-note">${escapeHtml(projects?.message || "CORDIS project status unavailable.")}</p>
      </article>
      <article class="overview-section-card">
        <div class="overview-section-header">
          <div>
            <p class="blocked-kicker">DEQAR / NEAA</p>
            <h3>Quality context</h3>
          </div>
          <div class="indicator-tag">${escapeHtml(qualitySourceTagLabel(quality))}</div>
        </div>
        <div class="overview-section-facts">
          ${qualityFacts}
        </div>
        <p class="chart-note">${escapeHtml(quality?.deqar?.summary || quality?.neaa?.message || "Quality context unavailable.")}</p>
      </article>
    </div>
  `;
  finalizeChartContentLayout(el);
}

function makeBreakdownNarrative(rows, indicator) {
  if (!rows.length) {
    return `No data available for ${indicator.title.toLowerCase()} in the selected range.`;
  }

  const latest = breakdownSeriesForLatest(rows);
  if (!latest.focusCountry || latest.year === null || !latest.labels.length) {
    return `No breakdown rows are available for the selected countries.`;
  }

  return `Showing ${latest.labels.length} field series for ${countryLabel(latest.focusCountry)} in ${latest.year}. These are broad-field counts, not institutional programme performance.`;
}

function makeEnrolmentRateNarrative(rows) {
  if (!rows.length) {
    return "No normalized enrolment rate is available for the selected countries.";
  }

  const latest = latestByCountry(rows);
  const bg = latest.BG;
  const eu = latest.EU27_2020;
  if (bg && eu) {
    const diff = bg.value - eu.value;
    const direction = diff >= 0 ? "above" : "below";
    return `In ${bg.year}, Bulgaria's tertiary enrolment rate is ${Math.abs(diff).toFixed(1)} percentage points ${direction} the EU benchmark. Derived as tertiary enrolment divided by population aged 18-34.`;
  }

  const latestRow = rows.slice().sort((a, b) => b.year - a.year)[0];
  return `Latest derived enrolment rate is ${formatValue(latestRow.value, "percent")} for ${countryLabel(latestRow.country)} in ${latestRow.year}.`;
}

function latestAndPrevious(rows, country = "BG") {
  const series = countryRows(rows, country);
  return {
    latest: series[series.length - 1] || null,
    previous: series.length > 1 ? series[series.length - 2] : null,
  };
}

function deltaClassFor(delta, options = {}) {
  if (delta === null || delta === undefined) return "neutral";
  const { preferLower = false } = options;
  const favorable = preferLower ? delta <= 0 : delta >= 0;
  return favorable ? "positive" : "negative";
}

function renderNav() {
  topNav.innerHTML = state.pages
    .map((page) => {
      const activeClass = state.activePage?.slug === page.slug ? "is-active" : "";
      const statusClass = page.status !== "active" ? "is-muted" : "";
      return `
        <a class="nav-link ${activeClass} ${statusClass}" href="#/${page.slug}">
          <span>${page.title}</span>
          <small>${pageStatusLabel(page.status)}</small>
        </a>
      `;
    })
    .join("");
}

function toggleControl(el, isVisible) {
  el.hidden = !isVisible;
}

function syncControls(page) {
  const controls = new Set(page.controls);
  toggleControl(countriesControl, controls.has("countries"));
  toggleControl(yearRangeControl, controls.has("year_range"));
  toggleControl(institutionControl, controls.has("institution"));
  toggleControl(exportButton, controls.has("export"));
  exportButton.disabled = !controls.has("export") || page.status !== "active" || !state.currentIndicator;
  if (page.id === "market") {
    exportButton.textContent = "Export Primary CSV";
    return;
  }
  if (page.id === "research") {
    exportButton.textContent = "Export Context CSV";
    return;
  }
  exportButton.textContent = "Export CSV";
}

function renderPageHeader(page) {
  pageTitle.textContent = page.title;
  pageDescription.textContent = page.description;
  pageContext.textContent = `${contextLabel(page.context_type)} · ${pageStatusLabel(page.status)}`;
}

function renderMarketLayout(page) {
  disposeCharts();
  kpiGrid.hidden = false;

  const panelMarkup = page.panels
    .map(
      (panel) => `
        <section class="panel ${panel.layout === "wide" ? "panel-wide" : ""}">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Market</p>
              <h2>${panel.title}</h2>
              <p class="panel-summary">${panel.description || ""}</p>
            </div>
            <div class="indicator-tag" id="tag-${panel.id}">Loading...</div>
          </div>
          <div id="chart-${panel.id}" class="chart"></div>
          <p class="chart-note" id="note-${panel.id}"></p>
        </section>
      `,
    )
    .join("");

  pageContent.innerHTML = `
    <section class="context-banner panel panel-wide">
      <div class="panel-header">
        <div>
          <p class="panel-kicker">National context</p>
          <h2>External demand signals for Bulgaria and comparison systems</h2>
        </div>
        <div class="indicator-tag">Country context</div>
      </div>
      <p class="chart-note">
        These metrics describe Bulgaria's external market environment. They should not be read as direct university performance.
      </p>
    </section>
    ${panelMarkup}
    <section class="panel panel-wide">
      <div class="panel-header">
        <div>
          <p class="panel-kicker">Reference</p>
          <h2>Market indicator catalogue</h2>
        </div>
      </div>
      <div class="table-wrap">
        <table id="indicator-table">
          <thead>
            <tr>
              <th>Indicator</th>
              <th>Panel</th>
              <th>Source</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>
  `;
}

function renderOutcomesLayout(page) {
  disposeCharts();
  kpiGrid.hidden = false;

  const panelMarkup = page.panels
    .map(
      (panel) => `
        <section class="panel ${panel.layout === "wide" ? "panel-wide" : ""}">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Outcomes</p>
              <h2>${panel.title}</h2>
              <p class="panel-summary">${panel.description || ""}</p>
            </div>
            <div class="indicator-tag" id="tag-${panel.id}">Loading...</div>
          </div>
          <div id="chart-${panel.id}" class="chart"></div>
          <p class="chart-note" id="note-${panel.id}"></p>
        </section>
      `,
    )
    .join("");

  pageContent.innerHTML = `
    <section class="context-banner panel panel-wide">
      <div class="panel-header">
        <div>
          <p class="panel-kicker">Country context</p>
          <h2>How tertiary education is translating into employability and international relevance</h2>
        </div>
        <div class="indicator-tag">Country context</div>
      </div>
      <p class="chart-note">
        These indicators describe system-level outcomes for Bulgaria and comparison systems. They should not be read as direct institutional graduate tracking.
      </p>
    </section>
    ${panelMarkup}
    <section class="panel panel-wide">
      <div class="panel-header">
        <div>
          <p class="panel-kicker">Reference</p>
          <h2>Outcomes indicator catalogue</h2>
        </div>
      </div>
      <div class="table-wrap">
        <table id="indicator-table">
          <thead>
            <tr>
              <th>Indicator</th>
              <th>Panel</th>
              <th>Source</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>
  `;
}

function renderIndicatorTable(page) {
  const tbody = document.querySelector("#indicator-table tbody");
  if (!tbody) return;

  const indicatorIds = [...new Set(page.panels.flatMap((panel) => panel.indicator_ids))];
  tbody.innerHTML = indicatorIds
    .map((id) => state.indicators[id])
    .filter(Boolean)
    .map(
      (item) => `
        <tr>
          <td>${item.title}</td>
          <td>${item.panel}</td>
          <td>${sourceLabel(item.source)}</td>
          <td title="${item.description}">${item.description}</td>
        </tr>
      `,
    )
    .join("");
}

function buildKpiCard(label, value, unit, deltaText, deltaClass) {
  return `
    <article class="kpi-card">
      <p class="kpi-label">${label}</p>
      <h3 class="kpi-value">${value}</h3>
      <p class="kpi-unit">${unit}</p>
      <p class="kpi-delta ${deltaClass}">${deltaText}</p>
    </article>
  `;
}

function renderMarketKpis(dataMap) {
  const pop18Rows = dataMap.population_18_24 || [];
  const pop25Rows = dataMap.population_25_34 || [];
  const attainmentRows = dataMap.tertiary_attainment_25_34 || [];
  const internationalRows = dataMap.international_students_share || [];

  const pop18 = latestByCountry(pop18Rows).BG;
  const pop18Baseline = countryRows(pop18Rows, "BG")[0];
  const pop25 = latestByCountry(pop25Rows).BG;
  const pop25Baseline = countryRows(pop25Rows, "BG")[0];
  const attainment = latestByCountry(attainmentRows).BG;
  const attainmentEu = latestByCountry(attainmentRows).EU27_2020;
  const international = latestByCountry(internationalRows).BG;
  const internationalBaseline = countryRows(internationalRows, "BG")[0];

  const cards = [
    buildKpiCard(
      "Population aged 18-24",
      pop18 ? formatValue(pop18.value, "persons") : "n/a",
      "Bulgaria",
      pop18 && pop18Baseline && pop18.year !== pop18Baseline.year
        ? `${formatDelta(pop18.value - pop18Baseline.value, "persons")} since ${pop18Baseline.year}`
        : "No earlier comparison",
      pop18 && pop18Baseline ? (pop18.value - pop18Baseline.value >= 0 ? "positive" : "negative") : "neutral",
    ),
    buildKpiCard(
      "Population aged 25-34",
      pop25 ? formatValue(pop25.value, "persons") : "n/a",
      "Bulgaria",
      pop25 && pop25Baseline && pop25.year !== pop25Baseline.year
        ? `${formatDelta(pop25.value - pop25Baseline.value, "persons")} since ${pop25Baseline.year}`
        : "No earlier comparison",
      pop25 && pop25Baseline ? (pop25.value - pop25Baseline.value >= 0 ? "positive" : "negative") : "neutral",
    ),
    buildKpiCard(
      "Tertiary attainment 25-34",
      attainment ? formatValue(attainment.value, "percent") : "n/a",
      "Bulgaria",
      attainment && attainmentEu
        ? `${formatDelta(attainment.value - attainmentEu.value, "percent")} vs EU`
        : "EU comparison unavailable",
      attainment && attainmentEu ? (attainment.value - attainmentEu.value >= 0 ? "positive" : "negative") : "neutral",
    ),
    buildKpiCard(
      "International tertiary students",
      international ? formatValue(international.value, "persons") : "n/a",
      "Bulgaria",
      international && internationalBaseline && international.year !== internationalBaseline.year
        ? `${formatDelta(international.value - internationalBaseline.value, "persons")} since ${internationalBaseline.year}`
        : "No earlier comparison",
      international && internationalBaseline ? (international.value - internationalBaseline.value >= 0 ? "positive" : "negative") : "neutral",
    ),
  ];

  kpiGrid.innerHTML = cards.join("");
}

function renderOutcomesKpis(dataMap) {
  const attainmentRows = dataMap.tertiary_attainment_25_34 || [];
  const recentGradRows = dataMap.recent_graduate_employment_rate || [];
  const employmentRows = dataMap.employment_tertiary_25_64 || [];
  const unemploymentRows = dataMap.unemployment_tertiary_25_64 || [];

  const attainment = latestAndPrevious(attainmentRows);
  const recentGrad = latestAndPrevious(recentGradRows);
  const employment = latestAndPrevious(employmentRows);
  const unemployment = latestAndPrevious(unemploymentRows);

  const attainmentDelta = attainment.latest && attainment.previous ? attainment.latest.value - attainment.previous.value : null;
  const recentGradDelta = recentGrad.latest && recentGrad.previous ? recentGrad.latest.value - recentGrad.previous.value : null;
  const employmentDelta = employment.latest && employment.previous ? employment.latest.value - employment.previous.value : null;
  const unemploymentDelta = unemployment.latest && unemployment.previous ? unemployment.latest.value - unemployment.previous.value : null;

  const cards = [
    buildKpiCard(
      "Tertiary attainment 25-34",
      attainment.latest ? formatValue(attainment.latest.value, "percent") : "n/a",
      "Bulgaria",
      attainmentDelta === null ? "No earlier comparison" : `${formatDelta(attainmentDelta, "percent")} vs prior year`,
      deltaClassFor(attainmentDelta),
    ),
    buildKpiCard(
      "Recent graduate employment",
      recentGrad.latest ? formatValue(recentGrad.latest.value, "percent") : "n/a",
      "Bulgaria",
      recentGradDelta === null ? "No earlier comparison" : `${formatDelta(recentGradDelta, "percent")} vs prior year`,
      deltaClassFor(recentGradDelta),
    ),
    buildKpiCard(
      "Employment rate 25-64",
      employment.latest ? formatValue(employment.latest.value, "percent") : "n/a",
      "Bulgaria",
      employmentDelta === null ? "No earlier comparison" : `${formatDelta(employmentDelta, "percent")} vs prior year`,
      deltaClassFor(employmentDelta),
    ),
    buildKpiCard(
      "Unemployment rate 25-64",
      unemployment.latest ? formatValue(unemployment.latest.value, "percent") : "n/a",
      "Bulgaria",
      unemploymentDelta === null ? "No earlier comparison" : `${formatDelta(unemploymentDelta, "percent")} vs prior year`,
      deltaClassFor(unemploymentDelta, { preferLower: true }),
    ),
  ];

  kpiGrid.innerHTML = cards.join("");
}

function renderResearchLayout(page) {
  disposeCharts();
  kpiGrid.hidden = false;
  const primary = primaryInstitution();

  const panelMarkup = page.panels
    .map(
      (panel) => `
        <section class="panel ${panel.layout === "wide" ? "panel-wide" : ""}">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Research</p>
              <h2>${panel.title}</h2>
              <p class="panel-summary">${panel.description || ""}</p>
            </div>
            <div class="indicator-tag" id="tag-${panel.id}">Loading...</div>
          </div>
          <div id="chart-${panel.id}" class="${isHtmlPanelType(panel.chart_type) ? "chart chart-content" : "chart"}"></div>
          <p class="chart-note" id="note-${panel.id}"></p>
        </section>
      `,
    )
    .join("");

  pageContent.innerHTML = `
    <section class="context-banner panel panel-wide">
      <div class="panel-header">
        <div>
          <p class="panel-kicker">Mixed context</p>
          <h2>Institution research visibility plus national R&D conditions</h2>
        </div>
        <div class="indicator-tag">${primary ? institutionSelectionSummary() : "Select universities"}</div>
      </div>
      <p class="chart-note">
        OpenAlex panels compare the selected universities. KPI cards and the CORDIS status below stay anchored to the first selected university. Eurostat panels describe the national environment for Bulgaria and comparison systems.
      </p>
    </section>
    ${panelMarkup}
    <section class="panel panel-wide">
      <div class="panel-header">
        <div>
          <p class="panel-kicker">Reference</p>
          <h2>Research context indicator catalogue</h2>
        </div>
      </div>
      <div class="table-wrap">
        <table id="indicator-table">
          <thead>
            <tr>
              <th>Indicator</th>
              <th>Panel</th>
              <th>Source</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>
  `;
}

function renderResearchKpis(summary) {
  const points = filterResearchPoints(summary.counts_by_year || []);
  const latestPoint = points[points.length - 1] || null;
  const baselinePoint = points[0] || null;
  const hIndex = summary.summary_stats?.h_index;
  const i10Index = summary.summary_stats?.i10_index;
  const meanCitedness = summary.summary_stats?.["2yr_mean_citedness"];

  const publicationsDelta = latestPoint && baselinePoint && latestPoint.year !== baselinePoint.year
    ? latestPoint.works_count - baselinePoint.works_count
    : null;

  const cards = [
    buildKpiCard(
      "Publications",
      latestPoint ? formatValue(latestPoint.works_count, "persons") : "n/a",
      latestPoint ? `${summary.institution.display_name} · ${latestPoint.year}` : summary.institution.display_name,
      publicationsDelta === null || !baselinePoint
        ? "No earlier comparison"
        : `${formatDelta(publicationsDelta, "persons")} since ${baselinePoint.year}`,
      deltaClassFor(publicationsDelta),
    ),
    buildKpiCard(
      "Total citations",
      formatValue(summary.cited_by_count || 0, "persons"),
      summary.institution.display_name,
      meanCitedness != null ? `2-year mean citedness ${Number(meanCitedness).toFixed(2)}` : "2-year mean citedness unavailable",
      "neutral",
    ),
    buildKpiCard(
      "h-index",
      hIndex != null ? formatValue(hIndex, "persons") : "n/a",
      summary.institution.display_name,
      i10Index != null ? `${formatValue(i10Index, "persons")} works with 10+ citations` : "i10-index unavailable",
      "neutral",
    ),
    buildKpiCard(
      "Open-access share",
      latestPoint?.open_access_share != null ? formatValue(latestPoint.open_access_share, "percent") : "n/a",
      latestPoint ? `${summary.institution.display_name} · ${latestPoint.year}` : summary.institution.display_name,
      latestPoint ? "Latest publication-year share" : "Open-access share unavailable",
      "neutral",
    ),
  ];

  kpiGrid.innerHTML = cards.join("");
}

function renderQualityLayout(page) {
  disposeCharts();
  kpiGrid.hidden = false;
  const primary = primaryInstitution();
  const peerModeMeta = qualityPeerModeMeta(state.qualityPeerMode);

  const panelMarkup = page.panels
    .map(
      (panel) => `
        <section class="panel ${panel.layout === "wide" ? "panel-wide" : ""}">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Quality</p>
              <h2>${panel.title}</h2>
              <p class="panel-summary">${panel.description || ""}</p>
            </div>
            <div class="indicator-tag" id="tag-${panel.id}">Loading...</div>
          </div>
          <div id="chart-${panel.id}" class="${isHtmlPanelType(panel.chart_type) ? "chart chart-content" : "chart"}"></div>
          <p class="chart-note" id="note-${panel.id}"></p>
        </section>
      `,
    )
    .join("");

  pageContent.innerHTML = `
    <section class="context-banner panel panel-wide">
      <div class="panel-header">
        <div>
          <p class="panel-kicker">Institution context</p>
          <h2>External quality-assurance status and benchmarking readiness</h2>
        </div>
        <div class="indicator-tag">${primary ? primary.display_name : "Select a university"}</div>
      </div>
      <div class="quality-context-toolbar">
        <p class="chart-note">
          This page uses the first selected university as the active institution. It reads DEQAR status from the downloaded dataset snapshot and currently benchmarks against the <strong>${peerModeMeta.label.toLowerCase()}</strong> cohort.
        </p>
        <label class="field-label quality-cohort-control">
          Peer cohort
          <select id="quality-peer-mode">
            ${QUALITY_PEER_MODES
              .map(
                (option) => `<option value="${option.value}" ${option.value === state.qualityPeerMode ? "selected" : ""}>${option.label}</option>`,
              )
              .join("")}
          </select>
          <span class="field-help">${peerModeMeta.note}</span>
        </label>
      </div>
    </section>
    ${panelMarkup}
  `;
}

function renderQualityKpis(quality, primary) {
  const reportCount = quality.metadata?.report_count ?? quality.deqar?.reports?.length ?? 0;
  const benchmarkingPeerCount = quality.benchmarking?.metadata?.peer_count ?? 0;
  const readyPeerCount = quality.benchmarking?.metadata?.ready_peer_count ?? 0;
  const expiringPeerCount = quality.benchmarking?.metadata?.expiring_12m_peer_count ?? 0;
  const deqarMetadata = quality.deqar?.metadata || {};
  const neaaActive = qualityNeaaIsActive(quality);
  const neaaApplicable = qualityNeaaIsApplicable(quality);
  const qaSourceValue = quality.deqar.status === "active"
    ? (neaaActive ? "DEQAR + NEAA" : pageStatusLabel(quality.deqar.status))
    : (neaaActive ? "NEAA only" : pageStatusLabel(quality.deqar.status));
  const qaSourceUnit = neaaActive
    ? "European + Bulgaria local"
    : (neaaApplicable ? "DEQAR fallback" : "DEQAR");
  const qaSourceNote = quality.deqar.status === "active"
    ? [
        `${qualityMatchConfidenceLabel(deqarMetadata.match_confidence)} match`,
        qualityRiskLevelLabel(deqarMetadata.qa_risk_level),
        neaaActive ? "NEAA local overlay active" : "",
      ]
        .filter(Boolean)
        .join(" • ")
    : (neaaActive
        ? (quality.neaa?.metadata?.comparison_summary || quality.neaa?.message || "NEAA local overlay active")
        : (quality.deqar.metadata?.next_step || "Dataset snapshot not ready"));
  const cards = [
    buildKpiCard(
      "Selected universities",
      String(state.currentInstitutions.length),
      primary ? `Primary: ${primary.display_name}` : "No primary university",
      state.currentInstitutions.length > 1 ? "This page uses the first selected university only" : "Single-university quality context",
      "neutral",
    ),
    buildKpiCard(
      "QA sources",
      qaSourceValue,
      qaSourceUnit,
      qaSourceNote,
      "neutral",
    ),
    buildKpiCard(
      "Reports indexed",
      formatValue(reportCount, "persons"),
      primary ? primary.display_name : "Quality page",
      reportCount
        ? `${reportCount} DEQAR reports in the local snapshot • ${qualityCoverageMixLabel(deqarMetadata)}`
        : "No DEQAR reports found in the local snapshot",
      "neutral",
    ),
    buildKpiCard(
      "Peer benchmarking",
      pageStatusLabel(quality.benchmarking.status),
      quality.benchmarking.metadata?.peer_group_label || "Dynamic peer set",
      readyPeerCount
        ? `${readyPeerCount} of ${benchmarkingPeerCount || readyPeerCount} peers already have institutional QA coverage${expiringPeerCount ? ` • ${expiringPeerCount} expiring within 12 months` : ""}`
        : (quality.benchmarking.metadata?.peer_group_description || quality.benchmarking.metadata?.next_step || "Depends on DEQAR plus later benchmark sources"),
      "neutral",
    ),
  ];

  kpiGrid.innerHTML = cards.join("");
}

function renderOverviewLayout(page) {
  disposeCharts();
  kpiGrid.hidden = false;
  const primary = primaryInstitution();

  const panelMarkup = page.panels
    .map(
      (panel) => `
        <section class="panel ${panel.layout === "wide" ? "panel-wide" : ""}">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Overview</p>
              <h2>${panel.title}</h2>
              <p class="panel-summary">${panel.description || ""}</p>
            </div>
            <div class="indicator-tag" id="tag-${panel.id}">Loading...</div>
          </div>
          <div id="chart-${panel.id}" class="${isHtmlPanelType(panel.chart_type) ? "chart chart-content" : "chart"}"></div>
          <p class="chart-note" id="note-${panel.id}"></p>
        </section>
      `,
    )
    .join("");

  pageContent.innerHTML = `
    <section class="context-banner panel panel-wide overview-banner">
      <div class="panel-header">
        <div>
          <p class="panel-kicker">Executive synthesis</p>
          <h2>Country-level demand and outcomes plus institution-level research, funding, and quality context</h2>
        </div>
        <div class="indicator-tag">${primary ? `${institutionSelectionSummary()} • primary institution` : "Select universities"}</div>
      </div>
      <div class="overview-banner-copy">
        <p class="chart-note">
          National indicators below describe Bulgaria and the selected comparison systems. Institution signals stay anchored to the first selected university and summarize the existing Research and Quality integrations without implying direct causality between the layers.
        </p>
        <p class="chart-note">
          Absolute counts such as population, international students, and researchers are system-size context. Percentage indicators such as attainment, recent graduate employment, and R&amp;D intensity are the clearest benchmark comparisons.
        </p>
      </div>
    </section>
    ${panelMarkup}
  `;
}

function renderOverviewKpis(dataMap, primary, summary, projects, quality) {
  const pop = latestAndPrevious(dataMap.population_18_24 || []);
  const attainmentLatest = latestByCountry(dataMap.tertiary_attainment_25_34 || []);
  const graduateLatest = latestByCountry(dataMap.recent_graduate_employment_rate || []);
  const international = latestAndPrevious(dataMap.international_students_share || []);
  const researchPoints = summary ? filterResearchPoints(summary.counts_by_year || []) : [];
  const latestResearchPoint = researchPoints[researchPoints.length - 1] || null;
  const researchBaseline = researchPoints[0] || null;
  const publicationsDelta = latestResearchPoint && researchBaseline && latestResearchPoint.year !== researchBaseline.year
    ? latestResearchPoint.works_count - researchBaseline.works_count
    : null;
  const projectCount = Number(projects?.metadata?.direct_match_project_count || projects?.projects?.length || 0);

  const cards = [
    buildKpiCard(
      "Population aged 18-24",
      pop.latest ? formatValue(pop.latest.value, "persons") : "n/a",
      pop.latest ? `Bulgaria · ${pop.latest.year}` : "Bulgaria",
      dataMap.population_18_24?.length ? overviewDeltaSincePrevious(dataMap.population_18_24, "persons") : "Population context unavailable",
      "neutral",
    ),
    buildKpiCard(
      "Tertiary attainment 25-34",
      attainmentLatest.BG ? formatValue(attainmentLatest.BG.value, "percent") : "n/a",
      attainmentLatest.BG ? `Bulgaria · ${attainmentLatest.BG.year}` : "Bulgaria",
      attainmentLatest.BG && attainmentLatest.EU27_2020
        ? `${formatDelta(attainmentLatest.BG.value - attainmentLatest.EU27_2020.value, "percent")} vs EU`
        : "EU comparison unavailable",
      attainmentLatest.BG && attainmentLatest.EU27_2020 ? deltaClassFor(attainmentLatest.BG.value - attainmentLatest.EU27_2020.value) : "neutral",
    ),
    buildKpiCard(
      "Recent graduate employment",
      graduateLatest.BG ? formatValue(graduateLatest.BG.value, "percent") : "n/a",
      graduateLatest.BG ? `Bulgaria · ${graduateLatest.BG.year}` : "Bulgaria",
      graduateLatest.BG && graduateLatest.EU27_2020
        ? `${formatDelta(graduateLatest.BG.value - graduateLatest.EU27_2020.value, "percent")} vs EU`
        : "EU comparison unavailable",
      graduateLatest.BG && graduateLatest.EU27_2020 ? deltaClassFor(graduateLatest.BG.value - graduateLatest.EU27_2020.value) : "neutral",
    ),
    buildKpiCard(
      "International tertiary students",
      international.latest ? formatValue(international.latest.value, "persons") : "n/a",
      international.latest ? `Bulgaria · ${international.latest.year}` : "Bulgaria",
      dataMap.international_students_share?.length ? overviewDeltaSincePrevious(dataMap.international_students_share, "persons") : "International-demand context unavailable",
      "neutral",
    ),
    buildKpiCard(
      "Publications",
      latestResearchPoint ? formatValue(latestResearchPoint.works_count, "persons") : "n/a",
      latestResearchPoint ? `${primary?.display_name || "Selected university"} · ${latestResearchPoint.year}` : (primary?.display_name || "Selected university"),
      summary
        ? (publicationsDelta === null ? "No earlier comparison" : `${formatDelta(publicationsDelta, "persons")} since ${researchBaseline.year}`)
        : "OpenAlex summary unavailable",
      summary ? deltaClassFor(publicationsDelta) : "neutral",
    ),
    buildKpiCard(
      "h-index",
      summary?.summary_stats?.h_index != null ? formatValue(summary.summary_stats.h_index, "persons") : "n/a",
      primary?.display_name || "Selected university",
      summary?.summary_stats?.i10_index != null ? `${formatValue(summary.summary_stats.i10_index, "persons")} works with 10+ citations` : "OpenAlex summary unavailable",
      "neutral",
    ),
    buildKpiCard(
      "EU projects",
      projects?.status === "active" ? formatValue(projectCount, "persons") : pageStatusLabel(projects?.status || "unavailable"),
      primary?.display_name || "Selected university",
      projects?.status === "active"
        ? `${formatValue(Number(projects.metadata?.coordinator_project_count || 0), "persons")} coordinator roles`
        : (projects?.metadata?.next_step || projects?.message || "CORDIS status unavailable"),
      "neutral",
    ),
    buildKpiCard(
      "QA status",
      qualityCurrentStatusValue(quality),
      qualitySourceTagLabel(quality),
      quality?.deqar?.decision_date
        ? `Anchor decision ${formatCalendarDate(quality.deqar.decision_date)}`
        : (quality?.deqar?.summary || quality?.neaa?.message || "Quality context unavailable"),
      "neutral",
    ),
  ];

  kpiGrid.innerHTML = cards.join("");
}

function renderPlaceholderPage(page) {
  disposeCharts();
  state.currentIndicator = null;
  kpiGrid.hidden = true;
  kpiGrid.innerHTML = "";
  pageContent.innerHTML = `
    <section class="panel panel-wide placeholder-panel">
      <div class="panel-header">
        <div>
          <p class="panel-kicker">${contextLabel(page.context_type)}</p>
          <h2>${page.title}</h2>
        </div>
        <div class="status-badge">${pageStatusLabel(page.status)}</div>
      </div>
      <p class="placeholder-copy">${page.description}</p>
      <p class="placeholder-copy">
        The route, page metadata, and control visibility are wired now from <code>/api/pages</code>. Market, Outcomes, Research, and Quality are live implementations, while this page remains scaffolded until its source-specific data contracts are ready.
      </p>
    </section>
  `;
}

async function loadOverviewPage(page) {
  const institutions = await ensureInstitutionSelection();
  const primary = primaryInstitution();

  renderOverviewLayout(page);
  showKpiSkeletons();

  page.panels.forEach((panel) => showSkeleton(`chart-${panel.id}`));
  page.panels.forEach((panel) => {
    const note = document.getElementById(`note-${panel.id}`);
    if (note) note.textContent = "";
  });

  state.currentIndicator = null;
  syncControls(page);

  const indicatorIds = [...new Set(page.panels.flatMap((panel) => panel.indicator_ids || []))];
  const [batchResult, summaryResult, projectsResult, qualityResult] = await Promise.allSettled([
    fetchBatchData(indicatorIds, state.currentCountries, state.currentYearRange),
    primary ? getResearchSummaryCached(primary.id) : Promise.resolve(null),
    primary ? getProjectsStatusCached(primary.id) : Promise.resolve(null),
    primary ? getQualityStatusCached(primary.id, state.qualityPeerMode) : Promise.resolve(null),
  ]);

  const batch = batchResult.status === "fulfilled"
    ? batchResult.value
    : { results: {}, errors: {} };
  const responseMap = batch.results || {};
  const dataMap = Object.fromEntries(indicatorIds.map((indicatorId) => [indicatorId, responseMap[indicatorId]?.rows || []]));
  const summary = summaryResult.status === "fulfilled" ? summaryResult.value : null;
  const projects = projectsResult.status === "fulfilled" && projectsResult.value
    ? projectsResult.value
    : {
        source: "cordis",
        status: "unavailable",
        message: "CORDIS project status could not be loaded right now.",
        metadata: {},
        projects: [],
      };
  const quality = qualityResult.status === "fulfilled" && qualityResult.value
    ? qualityResult.value
    : {
        institution_id: primary?.id || "",
        deqar: {
          source: "deqar",
          institution_id: primary?.id || "",
          status: "unavailable",
          current_status: null,
          agency: null,
          decision_date: null,
          summary: "Quality context could not be loaded right now.",
          reports: [],
          metadata: {},
        },
        neaa: {
          source: "neaa",
          status: "unavailable",
          message: "NEAA context could not be loaded right now.",
          institution_id: primary?.id || "",
          metadata: {},
        },
        benchmarking: {
          source: "benchmarking",
          status: "unavailable",
          message: "Benchmarking readiness could not be loaded right now.",
          institution_id: primary?.id || "",
          metadata: {},
        },
        metadata: {},
      };

  renderOverviewKpis(dataMap, primary, summary, projects, quality);

  page.panels.forEach((panel) => {
    const chartId = `chart-${panel.id}`;
    const tagEl = document.getElementById(`tag-${panel.id}`);
    const noteEl = document.getElementById(`note-${panel.id}`);

    if (panel.chart_type === "overview_national_snapshot") {
      const latestYearLabel = overviewLatestIndicatorYear(responseMap, panel.indicator_ids) ?? "n/a";
      if (tagEl) tagEl.textContent = `Eurostat • ${latestYearLabel}`;
      renderOverviewNationalSnapshotPanel(chartId, responseMap);
      if (noteEl) {
        noteEl.textContent = "Absolute counts are system-size context. Percentage indicators use Bulgaria versus the EU benchmark when available, while the selected-country range provides added scale context.";
      }
      return;
    }

    if (panel.chart_type === "overview_trend_watch") {
      if (tagEl) tagEl.textContent = `${state.currentYearRange.from}–${state.currentYearRange.to} • selected systems`;
      renderOverviewTrendWatchPanel(chartId, responseMap);
      if (noteEl) {
        noteEl.textContent = "These four trends condense the existing Market and Outcomes pages into a single executive watchlist for the current filter set.";
      }
      return;
    }

    if (panel.chart_type === "overview_institution_brief") {
      if (tagEl) {
        tagEl.textContent = primary ? `${primary.display_name} • mixed sources` : "Institution context";
      }
      renderOverviewInstitutionBriefPanel(chartId, primary, summary, projects, quality);
      if (noteEl) {
        noteEl.textContent = primary
          ? "Institution signals below stay anchored to the first selected university; use the dedicated Research and Quality pages for deeper drill-down."
          : "Select a university to populate the institution-specific part of the Overview page.";
      }
    }
  });

  syncControls(page);
}

async function loadMarketPage(page) {
  renderMarketLayout(page);
  renderIndicatorTable(page);
  showKpiSkeletons();

  page.panels.forEach((panel) => showSkeleton(`chart-${panel.id}`));
  page.panels.forEach((panel) => {
    const note = document.getElementById(`note-${panel.id}`);
    if (note) note.textContent = "";
  });

  const indicatorIds = [...new Set(page.panels.flatMap((panel) => panel.indicator_ids))];
  state.currentIndicator = indicatorIds[0] || null;
  syncControls(page);

  const batch = await fetchBatchData(indicatorIds, state.currentCountries, state.currentYearRange);
  const responseMap = batch.results || {};
  const dataMap = Object.fromEntries(
    indicatorIds.map((id) => [id, responseMap[id]?.rows || []]),
  );

  renderMarketKpis(dataMap);

  page.panels.forEach((panel) => {
    const indicatorId = panel.indicator_ids[0];
    const response = responseMap[indicatorId];
    const chartId = `chart-${panel.id}`;
    const noteEl = document.getElementById(`note-${panel.id}`);
    const tagEl = document.getElementById(`tag-${panel.id}`);

    if (panel.chart_type === "derived_rate_bar") {
      const derivedRows = derivedEnrolmentRateRows(dataMap);
      const derivedIndicator = {
        title: panel.title,
        unit: "percent",
        source: "derived",
      };

      if (!derivedRows.length) {
        if (tagEl) tagEl.textContent = "Unavailable";
        if (noteEl) noteEl.textContent = "This rate needs enrolment plus population aged 18-24 and 25-34 for the same year.";
        showError(chartId, "Failed to derive enrolment rate", () => {
          void renderCurrentRoute();
        });
        return;
      }

      if (tagEl) {
        tagEl.textContent = `${sourceLabel("derived")} • ${latestYear(derivedRows) ?? "n/a"}`;
      }
      renderBarChart(chartId, derivedRows, derivedIndicator);
      if (noteEl) noteEl.textContent = makeEnrolmentRateNarrative(derivedRows);
      return;
    }

    if (!response || !response.rows.length) {
      if (tagEl) tagEl.textContent = "Unavailable";
      if (noteEl) {
        noteEl.textContent = batch.errors?.[indicatorId]?.message
          || "This panel will populate once the underlying dataset is available for the selected filter set.";
      }
      showError(chartId, "Failed to load panel data", () => {
        void renderCurrentRoute();
      });
      return;
    }

    if (tagEl) {
      tagEl.textContent = `${sourceLabel(response.indicator.source)} • ${response.metadata?.latest_year ?? latestYear(response.rows) ?? "n/a"}`;
    }

    if (panel.chart_type === "breakdown_bar") {
      renderBreakdownBarChart(chartId, response.rows, response.indicator);
      if (noteEl) noteEl.textContent = makeBreakdownNarrative(response.rows, response.indicator);
      return;
    }

    if (panel.chart_type === "bar") {
      renderBarChart(chartId, response.rows, response.indicator);
      if (noteEl) noteEl.textContent = makeBarNarrative(response.rows, response.indicator);
      return;
    }

    renderTrendChart(chartId, response.rows, response.indicator);
    if (noteEl) noteEl.textContent = makeTrendNarrative(response.rows, response.indicator);
  });

  syncControls(page);
}

async function loadOutcomesPage(page) {
  renderOutcomesLayout(page);
  renderIndicatorTable(page);
  showKpiSkeletons();

  page.panels.forEach((panel) => showSkeleton(`chart-${panel.id}`));
  page.panels.forEach((panel) => {
    const note = document.getElementById(`note-${panel.id}`);
    if (note) note.textContent = "";
  });

  const indicatorIds = [...new Set(page.panels.flatMap((panel) => panel.indicator_ids))];
  state.currentIndicator = indicatorIds[0] || null;
  syncControls(page);

  const batch = await fetchBatchData(indicatorIds, state.currentCountries, state.currentYearRange);
  const responseMap = batch.results || {};
  const dataMap = Object.fromEntries(indicatorIds.map((id) => [id, responseMap[id]?.rows || []]));

  renderOutcomesKpis(dataMap);

  page.panels.forEach((panel) => {
    const indicatorId = panel.indicator_ids[0];
    const response = responseMap[indicatorId];
    const chartId = `chart-${panel.id}`;
    const noteEl = document.getElementById(`note-${panel.id}`);
    const tagEl = document.getElementById(`tag-${panel.id}`);

    if (!response || !response.rows.length) {
      if (tagEl) tagEl.textContent = "Unavailable";
      if (noteEl) {
        noteEl.textContent = batch.errors?.[indicatorId]?.message
          || "This panel will populate once the underlying dataset is available for the selected filter set.";
      }
      showError(chartId, "Failed to load panel data", () => {
        void renderCurrentRoute();
      });
      return;
    }

    if (tagEl) {
      tagEl.textContent = `${sourceLabel(response.indicator.source)} • ${response.metadata?.latest_year ?? latestYear(response.rows) ?? "n/a"}`;
    }

    if (panel.chart_type === "bar") {
      renderBarChart(chartId, response.rows, response.indicator);
      if (noteEl) noteEl.textContent = makeBarNarrative(response.rows, response.indicator);
      return;
    }

    renderTrendChart(chartId, response.rows, response.indicator);
    if (noteEl) noteEl.textContent = makeTrendNarrative(response.rows, response.indicator);
  });

  syncControls(page);
}

async function loadResearchPage(page) {
  const institutions = await ensureInstitutionSelection();
  const primary = primaryInstitution();

  renderResearchLayout(page);
  renderIndicatorTable(page);
  showKpiSkeletons();

  page.panels.forEach((panel) => showSkeleton(`chart-${panel.id}`));
  page.panels.forEach((panel) => {
    const note = document.getElementById(`note-${panel.id}`);
    if (note) note.textContent = "";
  });

  const indicatorIds = [...new Set(page.panels.flatMap((panel) => panel.indicator_ids || []))];
  state.currentIndicator = indicatorIds[0] || null;
  syncControls(page);

  if (!institutions.length || !primary) {
    kpiGrid.innerHTML = buildKpiCard("Institution lookup", "n/a", "Research page", "No default university result returned", "neutral");
    page.panels.forEach((panel) => {
      const chartId = `chart-${panel.id}`;
      const tagEl = document.getElementById(`tag-${panel.id}`);
      const noteEl = document.getElementById(`note-${panel.id}`);
      if (tagEl) tagEl.textContent = "Unavailable";
      if (noteEl) noteEl.textContent = "Select one or more universities to load research visibility.";
      showError(chartId, "No university selected", () => {
        void renderCurrentRoute();
      });
    });
    return;
  }

  const summarySettled = await Promise.allSettled(institutions.map((institution) => getResearchSummaryCached(institution.id)));
  const summaries = summarySettled
    .filter((result) => result.status === "fulfilled")
    .map((result) => result.value);
  const primarySummary = summaries.find((summary) => summary.institution.id === primary.id) || summaries[0] || null;

  const [batchResult, projectsResult] = await Promise.allSettled([
    fetchBatchData(indicatorIds, state.currentCountries, state.currentYearRange),
    fetchProjectsStatus(primary.id),
  ]);

  if (primarySummary) {
    renderResearchKpis(primarySummary);
  } else {
    kpiGrid.innerHTML = [
      buildKpiCard("Publications", "n/a", primary.display_name, "Research summary unavailable", "neutral"),
      buildKpiCard("Total citations", "n/a", primary.display_name, "Research summary unavailable", "neutral"),
      buildKpiCard("h-index", "n/a", primary.display_name, "Research summary unavailable", "neutral"),
      buildKpiCard("Open-access share", "n/a", primary.display_name, "Research summary unavailable", "neutral"),
    ].join("");
  }

  const batch = batchResult.status === "fulfilled"
    ? batchResult.value
    : { results: {}, errors: {} };
  const responseMap = batch.results || {};

  page.panels.forEach((panel) => {
    const chartId = `chart-${panel.id}`;
    const noteEl = document.getElementById(`note-${panel.id}`);
    const tagEl = document.getElementById(`tag-${panel.id}`);

    if (panel.chart_type === "research_works_line" || panel.chart_type === "research_citations_line") {
      if (!summaries.length) {
        if (tagEl) tagEl.textContent = "Unavailable";
        if (noteEl) noteEl.textContent = "OpenAlex summaries could not be loaded for the selected universities.";
        showError(chartId, "Failed to load research summary", () => {
          void renderCurrentRoute();
        });
        return;
      }

      const metricKey = panel.chart_type === "research_works_line" ? "works_count" : "cited_by_count";
      const summariesWithPoints = summaries.filter((summary) => filterResearchPoints(summary.counts_by_year || []).length);

      if (!summariesWithPoints.length) {
        if (tagEl) tagEl.textContent = "Unavailable";
        if (noteEl) noteEl.textContent = "No OpenAlex yearly counts are available inside the selected time range.";
        showError(chartId, "No research data in the selected range", () => {
          void renderCurrentRoute();
        });
        return;
      }

      if (tagEl) {
        tagEl.textContent = `OpenAlex • ${summariesWithPoints.length} universities`;
      }

      renderResearchMultiSeriesChart(chartId, summariesWithPoints, metricKey, "persons");
      if (noteEl) {
        noteEl.textContent = panel.chart_type === "research_works_line"
          ? `Comparing publication-year series for ${summariesWithPoints.length} universities. The KPI row stays on ${primary.display_name}, and the latest year should be treated as directional rather than final.`
          : `Comparing citations attached to works grouped by publication year for ${summariesWithPoints.length} universities. The KPI row stays on ${primary.display_name}; this is not lifetime institution citation total.`;
      }
      return;
    }

    if (panel.chart_type === "blocked_notice") {
      const status = projectsResult.status === "fulfilled"
        ? projectsResult.value
        : {
            source: "cordis",
            status: "unavailable",
            message: "CORDIS project status could not be loaded right now.",
            metadata: {},
          };
      if (tagEl) {
        tagEl.textContent = status.status === "active"
          ? `${status.source.toUpperCase()} • ${formatValue(Number(status.metadata?.direct_match_project_count || status.projects?.length || 0), "persons")} direct projects`
          : `${status.source.toUpperCase()} • ${pageStatusLabel(status.status)}`;
      }
      renderCordisProjectsPanel(chartId, status);
      if (noteEl) {
        noteEl.textContent = status.metadata?.next_step || status.message;
      }
      return;
    }

    const indicatorId = panel.indicator_ids[0];
    const response = responseMap[indicatorId];

    if (!response || !response.rows.length) {
      if (tagEl) tagEl.textContent = "Unavailable";
      if (noteEl) {
        noteEl.textContent = batch.errors?.[indicatorId]?.message
          || "This panel will populate once the underlying dataset is available for the selected filter set.";
      }
      showError(chartId, "Failed to load panel data", () => {
        void renderCurrentRoute();
      });
      return;
    }

    if (tagEl) {
      tagEl.textContent = `${sourceLabel(response.indicator.source)} • ${response.metadata?.latest_year ?? latestYear(response.rows) ?? "n/a"}`;
    }

    renderTrendChart(chartId, response.rows, response.indicator);
    if (noteEl) noteEl.textContent = makeTrendNarrative(response.rows, response.indicator);
  });

  syncControls(page);
}

function renderQualityReportsPanel(chartId, quality) {
  const el = document.getElementById(chartId);
  if (!el) return;
  setChartContentMode(el);
  const metadata = quality.deqar.metadata || {};

  if (!quality.deqar.reports?.length) {
    renderBlockedPanel(chartId, {
      source: quality.deqar.source,
      status: quality.deqar.status,
      message: quality.deqar.summary,
      details: qualityBlockedDetails(metadata),
    });
    return;
  }

  const reports = quality.deqar.reports || [];
  const institutionalReportCount = metadata.institutional_report_count ?? reports.filter((report) => /institutional/i.test(report.report_type || "")).length;
  const freshnessShort = datasetFreshnessShort(metadata);
  const coverageMix = qualityCoverageMixLabel(metadata);
  const agencies = [...new Set(reports.map((report) => report.agency).filter(Boolean))].sort((left, right) => left.localeCompare(right));
  const decisionYears = availableQualityReportYears(reports);
  let filters = {
    scope: "all",
    decisionTone: "all",
    agency: "all",
    yearFrom: "all",
    yearTo: "all",
  };

  function renderExplorer() {
    const filteredReports = filterQualityReports(reports, filters);
    const selectedReportId = preferredQualityReportId(quality, filteredReports);
    const selectedReport = filteredReports.find((report) => report.report_id === selectedReportId) || null;
    const selectedAnalysis = selectedReport ? qualityCachedReportAnalysis(selectedReport) : null;
    const themeSummary = qualityCachedThemeSummary(quality, filters, filteredReports);
    if (selectedReport) {
      rememberQualityReportSelection(quality, selectedReport.report_id);
    }

    const selectedIndex = selectedReport
      ? filteredReports.findIndex((report) => report.report_id === selectedReport.report_id)
      : -1;
    const institutionalInView = filteredReports.filter((report) => report.scope === "institutional").length;
    const currentInView = filteredReports.filter((report) => isCurrentQualityReport(report)).length;
    const conditionalInView = filteredReports.filter((report) => qualityDecisionTone(report.decision) === "conditional").length;
    const negativeInView = filteredReports.filter((report) => qualityDecisionTone(report.decision) === "negative").length;
    const agenciesInView = new Set(filteredReports.map((report) => report.agency).filter(Boolean)).size;
    const filteredReportYears = buildQualityFilteredReportYears(filteredReports).slice(0, 8);
    const summaryCopy = filteredReports.length === reports.length
      ? `${reports.length} reports loaded${institutionalReportCount ? `, including ${institutionalReportCount} institutional review${institutionalReportCount === 1 ? "" : "s"}` : ""}.`
      : `${filteredReports.length} of ${reports.length} reports match the current filters${institutionalInView ? `, including ${institutionalInView} institutional review${institutionalInView === 1 ? "" : "s"}` : ""}.`;

    el.innerHTML = `
      <div class="quality-report-explorer">
        <div class="quality-report-toolbar">
          <div class="quality-report-summary">
            <p class="blocked-kicker">DEQAR reports</p>
            <h3>${escapeHtml(metadata.matched_institution_name || metadata.institution_name || "Selected institution")}</h3>
            <p>${escapeHtml(summaryCopy)}</p>
          </div>
          <div class="quality-report-controls">
            <div class="quality-report-filter-grid">
              <label class="field-label quality-report-filter">
                Scope
                <select class="quality-report-filter-select" data-filter-key="scope">
                  <option value="all" ${filters.scope === "all" ? "selected" : ""}>All scopes</option>
                  <option value="institutional" ${filters.scope === "institutional" ? "selected" : ""}>Institutional</option>
                  <option value="programme" ${filters.scope === "programme" ? "selected" : ""}>Programme</option>
                  <option value="monitoring" ${filters.scope === "monitoring" ? "selected" : ""}>Monitoring</option>
                  <option value="other" ${filters.scope === "other" ? "selected" : ""}>Other</option>
                </select>
              </label>
              <label class="field-label quality-report-filter">
                Decision
                <select class="quality-report-filter-select" data-filter-key="decisionTone">
                  <option value="all" ${filters.decisionTone === "all" ? "selected" : ""}>All decisions</option>
                  <option value="positive" ${filters.decisionTone === "positive" ? "selected" : ""}>Positive</option>
                  <option value="conditional" ${filters.decisionTone === "conditional" ? "selected" : ""}>Conditional</option>
                  <option value="negative" ${filters.decisionTone === "negative" ? "selected" : ""}>Negative</option>
                  <option value="neutral" ${filters.decisionTone === "neutral" ? "selected" : ""}>Neutral / other</option>
                </select>
              </label>
              <label class="field-label quality-report-filter">
                Agency
                <select class="quality-report-filter-select" data-filter-key="agency">
                  <option value="all" ${filters.agency === "all" ? "selected" : ""}>All agencies</option>
                  ${agencies
                    .map(
                      (agency) => `<option value="${escapeHtml(agency)}" ${filters.agency === agency ? "selected" : ""}>${escapeHtml(agency)}</option>`,
                    )
                    .join("")}
                </select>
              </label>
              <label class="field-label quality-report-filter">
                Decision year from
                <select class="quality-report-filter-select" data-filter-key="yearFrom">
                  <option value="all" ${filters.yearFrom === "all" ? "selected" : ""}>Any start year</option>
                  ${decisionYears
                    .map(
                      (year) => `<option value="${year}" ${String(filters.yearFrom) === String(year) ? "selected" : ""}>${year}</option>`,
                    )
                    .join("")}
                </select>
              </label>
              <label class="field-label quality-report-filter">
                Decision year to
                <select class="quality-report-filter-select" data-filter-key="yearTo">
                  <option value="all" ${filters.yearTo === "all" ? "selected" : ""}>Any end year</option>
                  ${decisionYears
                    .map(
                      (year) => `<option value="${year}" ${String(filters.yearTo) === String(year) ? "selected" : ""}>${year}</option>`,
                    )
                    .join("")}
                </select>
              </label>
            </div>
            <label class="field-label quality-report-selector">
              Select report
              <select class="quality-report-select" ${filteredReports.length ? "" : "disabled"}>
                ${
                  filteredReports.length
                    ? filteredReports
                      .map(
                        (report) => `
                          <option value="${report.report_id}" ${selectedReport?.report_id === report.report_id ? "selected" : ""}>
                            ${escapeHtml(qualityReportOptionLabel(report))}
                          </option>
                        `,
                      )
                      .join("")
                    : '<option value="">No reports match current filters</option>'
                }
              </select>
            </label>
          </div>
        </div>
        <div class="quality-report-context-grid">
          ${qualityFactCard("Reports in view", String(filteredReports.length), filteredReports.length === reports.length ? "No filters applied" : "Subset of the full DEQAR snapshot")}
          ${qualityFactCard("Institutional in view", String(institutionalInView), "Institutional accreditation or review records")}
          ${qualityFactCard("Agencies in view", String(agenciesInView), filters.agency === "all" ? "Distinct agencies after current filters" : filters.agency)}
          ${qualityFactCard("Snapshot updated", formatDateTime(metadata.dataset_updated_at), freshnessShort || "")}
        </div>
        <div class="quality-analytics-grid">
          ${qualityFactCard("Current in view", String(currentInView), "Reports whose listed validity has not ended")}
          ${qualityFactCard("Conditional in view", String(conditionalInView), "After scope, agency, and decision-year filters")}
          ${qualityFactCard("Negative in view", String(negativeInView), "Negative, refused, withdrawn, or revoked decisions")}
          ${qualityFactCard("Decision years", qualityYearRangeLabel(filters), "Applied to report decision dates")}
        </div>
        <div class="quality-status-notices compact">
          ${qualityNoticeMarkup("Coverage note", metadata.coverage_notice, "neutral")}
          ${
            metadata.qa_risk_level && metadata.qa_risk_level !== "low"
              ? qualityNoticeMarkup("QA risk watch", metadata.qa_risk_summary, "warning")
              : ""
          }
          ${
            metadata.match_confidence === "low"
              ? qualityNoticeMarkup(
                  "Verify match",
                  "This DEQAR record was linked by fuzzy institution name only. Review the DEQAR source record before using it in high-stakes comparisons.",
                  "warning",
                )
              : ""
          }
        </div>
        <div class="quality-theme-panel">
          ${qualityThemeSummaryMarkup(quality, filteredReports, filters, themeSummary)}
        </div>
        <div class="quality-report-detail">
          ${
            selectedReport
              ? qualityReportDetailMarkup(selectedReport, selectedAnalysis)
              : `
                <div class="quality-report-detail-card">
                  <p class="quality-report-empty">No reports match the current filters. Clear one or more filters to inspect a report.</p>
                </div>
              `
          }
        </div>
        ${qualityActivityTableMarkup(filteredReportYears)}
      </div>
    `;

    el.querySelectorAll(".quality-report-filter-select").forEach((input) => {
      input.addEventListener("change", (event) => {
        const nextFilters = {
          ...filters,
          [event.target.dataset.filterKey]: event.target.value,
        };
        if (nextFilters.yearFrom !== "all" && nextFilters.yearTo !== "all") {
          const yearFrom = Number.parseInt(nextFilters.yearFrom, 10);
          const yearTo = Number.parseInt(nextFilters.yearTo, 10);
          if (Number.isInteger(yearFrom) && Number.isInteger(yearTo) && yearFrom > yearTo) {
            if (event.target.dataset.filterKey === "yearFrom") {
              nextFilters.yearTo = nextFilters.yearFrom;
            } else if (event.target.dataset.filterKey === "yearTo") {
              nextFilters.yearFrom = nextFilters.yearTo;
            }
          }
        }
        filters = nextFilters;
        renderExplorer();
      });
    });

    const reportSelectEl = el.querySelector(".quality-report-select");
    if (reportSelectEl && filteredReports.length) {
      reportSelectEl.addEventListener("change", (event) => {
        rememberQualityReportSelection(quality, event.target.value);
        renderExplorer();
      });
    }

    const analysisButton = el.querySelector(".quality-report-analysis-button");
    if (analysisButton && selectedReport && !analysisButton.disabled) {
      analysisButton.addEventListener("click", async () => {
        analysisButton.disabled = true;
        await requestQualityReportAnalysis(selectedReport);
        renderExplorer();
      });
    }

    const themeSummaryButton = el.querySelector(".quality-theme-summary-button");
    if (themeSummaryButton && filteredReports.length && !themeSummaryButton.disabled) {
      themeSummaryButton.addEventListener("click", async () => {
        themeSummaryButton.disabled = true;
        await requestQualityThemeSummary(quality, filteredReports, filters);
        renderExplorer();
      });
    }

    if (selectedIndex < 0 && filteredReports.length === 0) {
      state.qualityReportSelection.delete(qualitySelectionKey(quality));
    }

    finalizeChartContentLayout(el);
  }

  renderExplorer();
}

function renderQualityStatusPanel(chartId, quality) {
  const el = document.getElementById(chartId);
  if (!el) return;
  setChartContentMode(el);
  const metadata = quality.deqar.metadata || {};
  const deqarActive = quality.deqar.status === "active";
  const neaaMarkup = qualityNeaaOverlayMarkup(quality.neaa);

  if (!deqarActive && !qualityNeaaIsApplicable(quality)) {
    renderBlockedPanel(chartId, {
      source: quality.deqar.source,
      status: quality.deqar.status,
      message: quality.deqar.summary,
      details: qualityBlockedDetails(metadata),
    });
    return;
  }

  if (!deqarActive) {
    el.innerHTML = `
      <div class="blocked-panel quality-status-panel">
        ${qualityNoticeMarkup("DEQAR unavailable", quality.deqar.summary || "The DEQAR snapshot did not return institutional status for this university.", "neutral")}
        ${neaaMarkup || qualityNoticeMarkup("NEAA local context", quality.neaa?.message || "No Bulgarian local overlay is available right now.", "neutral")}
      </div>
    `;
    finalizeChartContentLayout(el);
    return;
  }

  const matchDescriptor = qualityMatchMethodLabel(metadata.match_type);
  const matchLabel = qualityMatchConfidenceLabel(metadata.match_confidence);
  const coverageMix = qualityCoverageMixLabel(metadata);
  const freshnessShort = datasetFreshnessShort(metadata);
  el.innerHTML = `
    <div class="blocked-panel quality-status-panel">
      <p class="blocked-kicker">${escapeHtml(quality.deqar.source).toUpperCase()}</p>
      <h3>${escapeHtml(metadata.matched_institution_name || metadata.institution_name || "Matched institution")}</h3>
      <div class="quality-fact-grid">
        ${qualityFactCard("Current decision", quality.deqar.current_status || "n/a")}
        ${qualityFactCard("Agency", quality.deqar.agency || "n/a")}
        ${qualityFactCard("Agency register", qualityAgencyRegisterValue(metadata), metadata.agency_register_note || "")}
        ${qualityFactCard("Anchor decision date", quality.deqar.decision_date ? formatCalendarDate(quality.deqar.decision_date) : "n/a")}
        ${qualityFactCard("Match confidence", matchLabel, metadata.match_confidence_note || "")}
        ${qualityFactCard("Match provenance", qualityCrosswalkValueLabel(metadata), qualityCrosswalkNote(metadata))}
        ${qualityFactCard("Coverage mix", coverageMix, metadata.coverage_summary || "")}
        ${qualityFactCard("Institutional validity", qualityInstitutionalValidityLabel(metadata), qualityInstitutionalValidityNote(metadata))}
        ${qualityFactCard("First recorded decision", formatCalendarDate(metadata.first_decision_date), metadata.latest_decision_date ? `Latest recorded decision: ${formatCalendarDate(metadata.latest_decision_date)}` : "")}
        ${qualityFactCard("Snapshot updated", formatDateTime(metadata.dataset_updated_at), freshnessShort || "")}
      </div>
      <p>${escapeHtml(quality.deqar.summary)}</p>
      <div class="quality-analytics-grid">
        ${qualityFactCard("QA risk", qualityRiskLevelLabel(metadata.qa_risk_level), metadata.qa_risk_summary || "")}
        ${qualityFactCard("Institutional review age", formatDurationFromDays(metadata.institutional_review_age_days), metadata.anchor_institutional_decision_date ? `Since ${formatCalendarDate(metadata.anchor_institutional_decision_date)}` : "No institutional review date")}
        ${qualityFactCard("Listed time to expiry", qualityValidityCountdownLabel(metadata), qualityValidityCountdownNote(metadata))}
        ${qualityFactCard("Decision pattern", `${metadata.recent_conditional_decision_count ?? 0} conditional`, qualityDecisionPatternNote(metadata))}
      </div>
      <div class="quality-status-notices">
        ${qualityNoticeMarkup("Coverage note", metadata.coverage_notice, "neutral")}
        ${
          metadata.qa_risk_level && metadata.qa_risk_level !== "low"
            ? qualityNoticeMarkup("QA risk watch", metadata.qa_risk_summary, "warning")
            : ""
        }
        ${
          metadata.match_confidence === "low"
            ? qualityNoticeMarkup(
                "Verify match",
                "This DEQAR record was linked by fuzzy institution name only. Review the DEQAR source record before using it in high-stakes comparisons.",
                "warning",
              )
            : ""
        }
      </div>
      <div class="quality-status-meta">
        <p>
          Match method:
          <strong>${escapeHtml(matchDescriptor)}</strong>
          ${metadata.match_value ? ` • ${escapeHtml(metadata.match_value)}` : ""}
        </p>
        ${
          metadata.match_provenance_label
            ? `<p>Match provenance: <strong>${escapeHtml(metadata.match_provenance_label)}</strong>${metadata.match_confidence ? ` • ${escapeHtml(qualityMatchConfidenceLabel(metadata.match_confidence))}` : ""}</p>`
            : ""
        }
        ${
          metadata.crosswalk_scheme
            ? `<p>Crosswalk: <strong>${escapeHtml(metadata.crosswalk_scheme)}</strong>${metadata.openalex_ror ? ` • OpenAlex ROR ${escapeHtml(metadata.openalex_ror)}` : ""}${metadata.matched_ror ? ` • DEQAR ROR ${escapeHtml(metadata.matched_ror)}` : ""}${metadata.matched_eter_id ? ` • DEQAR ETER ${escapeHtml(metadata.matched_eter_id)}` : ""}</p>`
            : ""
        }
        ${
          qualityRegistryProfileLabel(metadata)
            ? `<p>Registry profile: <strong>${escapeHtml(qualityRegistryProfileLabel(metadata))}</strong>${metadata.registry_country_code ? ` • ${escapeHtml(metadata.registry_country_code)}` : ""}${metadata.registry_website_host ? ` • ${escapeHtml(metadata.registry_website_host)}` : ""}</p>`
            : ""
        }
        <p>
          DEQAR record:
          ${
            metadata.deqar_url
              ? `<a class="quality-inline-link" href="${escapeHtml(metadata.deqar_url)}" target="_blank" rel="noreferrer">${escapeHtml(metadata.deqar_id || "Open record")}</a>`
              : escapeHtml(metadata.deqar_id || "n/a")
          }
        </p>
        ${
          metadata.agency_register_status || metadata.agency_register_url || metadata.agency_reports_url
            ? `<p>Agency register: <strong>${escapeHtml(qualityAgencyRegisterValue(metadata))}</strong>${metadata.agency_register_url ? ` • <a class="quality-inline-link" href="${escapeHtml(metadata.agency_register_url)}" target="_blank" rel="noreferrer">Register entry</a>` : ""}${metadata.agency_reports_url ? ` • <a class="quality-inline-link" href="${escapeHtml(metadata.agency_reports_url)}" target="_blank" rel="noreferrer">Agency DEQAR reports</a>` : ""}</p>`
            : ""
        }
      </div>
      ${neaaMarkup}
    </div>
  `;
  finalizeChartContentLayout(el);
}

function setChartContentMode(el) {
  el.classList.add("chart-content");
  el.style.height = "auto";
  el.style.minHeight = "0";
  el.style.overflow = "visible";
}

function finalizeChartContentLayout(el) {
  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      el.style.height = "auto";
      const contentHeight = el.scrollHeight;
      if (contentHeight > 0) {
        el.style.height = `${contentHeight}px`;
      }
    });
  });
}

function syncContentPanelHeights() {
  document.querySelectorAll(".chart.chart-content").forEach((el) => {
    el.style.height = "auto";
    const contentHeight = el.scrollHeight;
    if (contentHeight > 0) {
      el.style.height = `${contentHeight}px`;
    }
  });
}

async function loadQualityPage(page) {
  const institutions = await ensureInstitutionSelection();
  const primary = primaryInstitution();

  renderQualityLayout(page);
  const peerModeSelect = document.getElementById("quality-peer-mode");
  if (peerModeSelect) {
    peerModeSelect.addEventListener("change", (event) => {
      const nextMode = event.target.value;
      if (nextMode === state.qualityPeerMode) return;
      state.qualityPeerMode = nextMode;
      void renderCurrentRoute();
    });
  }
  showKpiSkeletons();

  page.panels.forEach((panel) => showSkeleton(`chart-${panel.id}`));
  page.panels.forEach((panel) => {
    const note = document.getElementById(`note-${panel.id}`);
    if (note) note.textContent = "";
  });

  state.currentIndicator = null;
  syncControls(page);

  if (!institutions.length || !primary) {
    kpiGrid.innerHTML = buildKpiCard("Institution lookup", "n/a", "Quality page", "No default university result returned", "neutral");
    page.panels.forEach((panel) => {
      const chartId = `chart-${panel.id}`;
      const tagEl = document.getElementById(`tag-${panel.id}`);
      const noteEl = document.getElementById(`note-${panel.id}`);
      if (tagEl) tagEl.textContent = "Unavailable";
      if (noteEl) noteEl.textContent = "Select a university to load external quality context.";
      showError(chartId, "No university selected", () => {
        void renderCurrentRoute();
      });
    });
    return;
  }

  const quality = await getQualityStatusCached(primary.id, state.qualityPeerMode);
  renderQualityKpis(quality, primary);

  page.panels.forEach((panel) => {
    const chartId = `chart-${panel.id}`;
    const noteEl = document.getElementById(`note-${panel.id}`);
    const tagEl = document.getElementById(`tag-${panel.id}`);

    if (panel.chart_type === "quality_status") {
      if (tagEl) tagEl.textContent = `${qualitySourceTagLabel(quality)} • ${pageStatusLabel(qualityOverallSourceStatus(quality))}`;
      renderQualityStatusPanel(chartId, quality);
      if (noteEl) {
        noteEl.textContent = quality.deqar.status === "active"
          ? (quality.neaa?.metadata?.comparison_summary || quality.deqar.summary)
          : (quality.neaa?.message || quality.deqar.metadata?.next_step || quality.deqar.summary);
      }
      return;
    }

    if (panel.chart_type === "quality_reports") {
      if (tagEl) tagEl.textContent = `${quality.deqar.source.toUpperCase()} • ${quality.metadata?.report_count ?? 0} reports`;
      renderQualityReportsPanel(chartId, quality);
      if (noteEl) {
        noteEl.textContent = quality.deqar.reports?.length
          ? `Select from ${quality.deqar.reports.length} DEQAR reports in the local snapshot to inspect decisions, agencies, dates, and report files.`
          : quality.deqar.metadata?.next_step || quality.deqar.summary;
      }
      return;
    }

    if (panel.chart_type === "quality_benchmarking") {
      if (tagEl) tagEl.textContent = `BENCHMARKING • ${pageStatusLabel(quality.benchmarking.status)}`;
      renderQualityBenchmarkingPanel(chartId, quality);
      if (noteEl) {
        noteEl.textContent = quality.benchmarking.metadata?.peer_count
          ? `Timeline bars show institutional review validity windows across ${quality.benchmarking.metadata.peer_count} peers in the ${quality.benchmarking.metadata.peer_group_label || "current cohort"}. The table below shows the DEQAR comparison coverage behind that view.`
          : (quality.benchmarking.metadata?.next_step || quality.benchmarking.message);
      }
      return;
    }
  });

  syncControls(page);
}

function renderFatalError(message) {
  kpiGrid.hidden = true;
  pageContent.innerHTML = `
    <section class="panel panel-wide">
      <div class="error-banner">
        <p class="error-message">Failed to initialize dashboard: ${message}</p>
        <button type="button" onclick="location.reload()">Reload page</button>
      </div>
    </section>
  `;
}

async function renderCurrentRoute() {
  const slug = ensureRoute(state.pages);
  const activePage = state.pagesBySlug.get(slug) || state.pages[0];
  if (!activePage) return;

  state.activePage = activePage;
  renderPageHeader(activePage);
  renderNav();
  syncControls(activePage);

  if (activePage.id === "overview" && activePage.status === "active") {
    await loadOverviewPage(activePage);
    return;
  }

  if (activePage.id === "market" && activePage.status === "active") {
    await loadMarketPage(activePage);
    return;
  }

  if (activePage.id === "outcomes" && activePage.status === "active") {
    await loadOutcomesPage(activePage);
    return;
  }

  if (activePage.id === "research" && activePage.status === "active") {
    await loadResearchPage(activePage);
    return;
  }

  if (activePage.id === "quality" && activePage.status === "active") {
    await loadQualityPage(activePage);
    return;
  }

  if (activePage.status !== "active") {
    renderPlaceholderPage(activePage);
    syncControls(activePage);
    return;
  }

  renderPlaceholderPage(activePage);
  syncControls(activePage);
}

async function exportCurrentIndicator() {
  if (!state.currentIndicator) return;
  try {
    const blob = await downloadIndicatorCsv(state.currentIndicator, state.currentCountries, state.currentYearRange);
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `${state.activePage?.id || "dashboard"}-${state.currentIndicator}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  } catch (err) {
    alert(`CSV export failed: ${err.message}`);
  }
}

function populateCountries(countries) {
  state.countryLabels = Object.fromEntries(countries.map((country) => [country.code, country.label]));
  countryMenu.innerHTML =
    `<input type="text" class="dropdown-search" placeholder="Search countries..." />` +
    countries
      .map(
        (country) => `
          <label class="dropdown-item" data-label="${country.label.toLowerCase()}">
            <input type="checkbox" value="${country.code}" ${state.currentCountries.includes(country.code) ? "checked" : ""} />
            ${country.label}
          </label>
        `,
      )
      .join("");

  const searchInput = countryMenu.querySelector(".dropdown-search");
  searchInput.addEventListener("input", () => {
    const query = searchInput.value.toLowerCase();
    countryMenu.querySelectorAll(".dropdown-item").forEach((item) => {
      item.style.display = item.dataset.label.includes(query) ? "" : "none";
    });
  });

  updateCountryCount();
}

function updateCountryCount() {
  const checked = countryMenu.querySelectorAll('input[type="checkbox"]:checked');
  countryCount.textContent = checked.length;
}

const debouncedReload = debounce(() => {
  void renderCurrentRoute();
}, 400);

const debouncedInstitutionSearch = debounce((query) => {
  void searchInstitutions(query, { selectFirst: false, reloadOnSelect: false });
}, 250);

function wireEvents() {
  countryToggle.addEventListener("click", (event) => {
    event.stopPropagation();
    countryDropdown.classList.toggle("open");
  });

  document.addEventListener("click", (event) => {
    if (!countryDropdown.contains(event.target)) {
      countryDropdown.classList.remove("open");
    }
    if (!institutionDropdown.contains(event.target)) {
      closeInstitutionMenu();
    }
  });

  countryMenu.addEventListener("change", (event) => {
    if (event.target.type !== "checkbox") return;
    state.currentCountries = [...countryMenu.querySelectorAll('input[type="checkbox"]:checked')].map((cb) => cb.value);
    updateCountryCount();
    debouncedReload();
  });

  yearRange.addEventListener("change", () => {
    const [from, to] = yearRange.value.split(":").map(Number);
    state.currentYearRange = { from, to };
    debouncedReload();
  });

  institutionToggle.addEventListener("click", (event) => {
    event.stopPropagation();
    if (institutionDropdown.classList.contains("open")) {
      closeInstitutionMenu();
      return;
    }
    if (!state.institutionResults.length || state.institutionSearchMode === "featured") {
      void searchInstitutions("", { selectFirst: false, reloadOnSelect: false, mode: "browse" });
      return;
    }
    openInstitutionMenu();
  });

  institutionMenu.addEventListener("change", (event) => {
    if (event.target.type !== "checkbox" && event.target.type !== "radio") return;
    const institutionId = event.target.value;
    const option = state.institutionOptions.get(institutionId) || state.institutionResults.find((item) => item.id === institutionId);
    if (!option) return;

    if (state.activePage?.id === "quality") {
      setSelectedInstitutions([option], { reload: true });
      closeInstitutionMenu();
      return;
    }

    if (event.target.checked) {
      if (selectedInstitutionIds().has(institutionId)) return;
      setSelectedInstitutions([...state.currentInstitutions, option], { reload: true });
      return;
    }

    if (state.currentInstitutions.length === 1) {
      event.target.checked = true;
      updateInstitutionHelp("At least one university must stay selected.");
      return;
    }

    setSelectedInstitutions(
      state.currentInstitutions.filter((institution) => institution.id !== institutionId),
      { reload: true },
    );
  });

  exportButton.addEventListener("click", exportCurrentIndicator);
  onRouteChange(() => {
    void renderCurrentRoute();
  });
}

async function init() {
  try {
    const [pages, rawIndicators, countries] = await Promise.all([fetchPages(), fetchIndicators(), fetchCountries()]);
    state.pages = pages;
    state.pagesBySlug = new Map(pages.map((page) => [page.slug, page]));
    state.indicators = Object.fromEntries(rawIndicators.map((indicator) => [indicator.id, indicator]));

    populateCountries(countries);
    wireEvents();
    updateInstitutionSelectionUi();

    const initialSlug = resolveSlug(state.pages);
    if (initialSlug) {
      ensureRoute(state.pages);
    }
    await renderCurrentRoute();
  } catch (err) {
    renderFatalError(err.message);
  }
}

await init();

window.addEventListener("resize", () => {
  syncContentPanelHeights();
  Object.values(charts).forEach((chart) => chart.resize());
});
