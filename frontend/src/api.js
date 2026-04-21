export async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

export function fetchPages() {
  return fetchJson("/api/pages");
}

export function fetchCountries() {
  return fetchJson("/api/countries");
}

export function fetchIndicators() {
  return fetchJson("/api/indicators");
}

export function fetchInstitutionSearch(query = "", mode = "browse") {
  const params = new URLSearchParams({ query, mode });
  return fetchJson(`/api/institutions/search?${params.toString()}`);
}

export function fetchResearchSummary(institutionId) {
  return fetchJson(`/api/research/institutions/${encodeURIComponent(institutionId)}/summary`);
}

export function fetchProjectsStatus(institutionId) {
  return fetchJson(`/api/projects/institutions/${encodeURIComponent(institutionId)}`);
}

export function fetchProjectsExtraction(institutionId) {
  return fetchJson(`/api/projects/institutions/${encodeURIComponent(institutionId)}/extract`, {
    method: "POST",
  });
}

export function fetchQualityStatus(institutionId, peerMode = "regional") {
  const params = new URLSearchParams({ peer_mode: peerMode });
  return fetchJson(`/api/quality/institutions/${encodeURIComponent(institutionId)}?${params.toString()}`);
}

export function fetchQualityReportAnalysis(payload) {
  return fetchJson("/api/quality/reports/analyze", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export function fetchQualityThemeSummary(payload) {
  return fetchJson("/api/quality/reports/theme-summary", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export function fetchIndicatorData(indicatorId, countries, yearRange) {
  const params = new URLSearchParams({
    indicator: indicatorId,
    countries: countries.join(","),
    year_from: yearRange.from,
    year_to: yearRange.to,
  });
  return fetchJson(`/api/data?${params.toString()}`);
}

export function fetchBatchData(indicatorIds, countries, yearRange) {
  const params = new URLSearchParams({
    indicators: indicatorIds.join(","),
    countries: countries.join(","),
    year_from: yearRange.from,
    year_to: yearRange.to,
  });
  return fetchJson(`/api/data/batch?${params.toString()}`);
}

export async function downloadIndicatorCsv(indicatorId, countries, yearRange) {
  const params = new URLSearchParams({
    indicator: indicatorId,
    countries: countries.join(","),
    year_from: yearRange.from,
    year_to: yearRange.to,
    format: "csv",
  });
  const response = await fetch(`/api/data?${params.toString()}`);
  if (!response.ok) {
    throw new Error(`Export failed: ${response.status}`);
  }
  return response.blob();
}
