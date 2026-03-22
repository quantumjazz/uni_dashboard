/* ── Constants ── */

const CHART_COLORS = ["#2563eb", "#7c3aed", "#16a34a", "#e55c20", "#0891b2", "#be185d", "#ca8a04"];
const BG_HIGHLIGHT = "#2563eb";
const BG_OTHER = "#94a3b8";

const charts = {};
let currentIndicator = "population_18_24";
let currentCountries = ["BG", "EU27_2020", "DE", "RO"];
let currentYearRange = { from: 2015, to: 2024 };

/* ── DOM refs ── */

const countryDropdown = document.getElementById("country-dropdown");
const countryToggle = document.getElementById("country-toggle");
const countryMenu = document.getElementById("country-menu");
const countryCount = document.getElementById("country-count");
const yearRange = document.getElementById("year-range");
const exportButton = document.getElementById("export-button");

/* ── Utilities ── */

function debounce(fn, delay = 400) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function formatValue(value, unit) {
  if (unit === "percent") return value.toFixed(1) + "%";
  if (value >= 1000) return new Intl.NumberFormat("en", { maximumFractionDigits: 0 }).format(value);
  return value.toFixed(1);
}

function unitLabel(unit) {
  if (unit === "percent") return "% of total";
  if (unit === "persons") return "persons";
  return unit || "";
}

/* ── Loading / Error UI ── */

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
  const grid = document.getElementById("kpi-grid");
  grid.innerHTML = Array(4)
    .fill('<article class="kpi-card"><div class="loading-skeleton kpi-skeleton"></div></article>')
    .join("");
}

/* ── Chart helpers ── */

function initChart(id) {
  const el = document.getElementById(id);
  if (!el) return null;
  const existing = echarts.getInstanceByDom(el);
  if (existing) existing.dispose();
  el.innerHTML = "";
  charts[id] = echarts.init(el);
  return charts[id];
}

function latestByCountry(rows) {
  return rows.reduce((acc, row) => {
    if (!acc[row.country] || row.year > acc[row.country].year) {
      acc[row.country] = row;
    }
    return acc;
  }, {});
}

function makeNarrative(rows, indicator) {
  if (!rows.length) {
    return `No data available for ${indicator.title} in the selected range.`;
  }
  const latest = latestByCountry(rows);
  const bg = latest.BG;
  const eu = latest.EU27_2020;
  if (bg && eu) {
    const diff = bg.value - eu.value;
    const direction = diff >= 0 ? "above" : "below";
    const formatted = formatValue(Math.abs(diff), indicator.unit);
    return `In ${bg.year}, Bulgaria sits ${formatted} ${direction} the EU average for ${indicator.title.toLowerCase()}.`;
  }
  return `Latest observed series year is ${Math.max(...rows.map((row) => row.year))}.`;
}

function seriesFor(rows) {
  const years = [...new Set(rows.map((row) => row.year))].sort((a, b) => a - b);
  const countries = [...new Set(rows.map((row) => row.country))];
  return {
    years,
    series: countries.map((country, i) => ({
      name: country,
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
    values: latest.map((item) => item.value),
  };
}

/* ── Chart renderers ── */

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
      xAxis: { type: "category", data: years, axisLine: { lineStyle: { color: "#e2e5ea" } }, axisLabel: { color: "#5f6673" } },
      yAxis: {
        type: "value",
        name: indicator.unit || "",
        nameTextStyle: { color: "#5f6673", fontSize: 11 },
        splitLine: { lineStyle: { color: "#f0f1f3" } },
        axisLabel: {
          color: "#5f6673",
          formatter: (v) => (v >= 1000 ? (v / 1000).toFixed(0) + "k" : v),
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
        data: latest.countries,
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

/* ── KPI rendering ── */

function renderKpis(rows, indicators) {
  const grid = document.getElementById("kpi-grid");
  const cards = [
    indicators["population_18_24"],
    indicators["tertiary_attainment_25_34"],
    indicators["employment_tertiary_25_64"],
    indicators["rd_expenditure_gdp"],
  ];

  grid.innerHTML = cards
    .map((item) => {
      const sourceRows = rows[item.id] || [];
      const latest = latestByCountry(sourceRows).BG;
      const previous = sourceRows
        .filter((row) => row.country === "BG")
        .sort((a, b) => b.year - a.year)[1];
      const delta = latest && previous ? latest.value - previous.value : null;

      let deltaClass = "neutral";
      if (delta !== null) deltaClass = delta >= 0 ? "positive" : "negative";

      const valueStr = latest ? formatValue(latest.value, item.unit) : "n/a";
      const deltaStr =
        delta === null
          ? "No prior comparison"
          : `${delta >= 0 ? "+" : ""}${formatValue(Math.abs(delta), item.unit)} vs prior year`;

      return `
        <article class="kpi-card">
          <p class="kpi-label">${item.title}</p>
          <h3 class="kpi-value">${valueStr}</h3>
          <p class="kpi-unit">${unitLabel(item.unit)}</p>
          <p class="kpi-delta ${deltaClass}">${deltaStr}</p>
        </article>
      `;
    })
    .join("");
}

/* ── Indicator table ── */

function renderIndicatorTable(indicators) {
  const tbody = document.querySelector("#indicator-table tbody");
  tbody.innerHTML = Object.values(indicators)
    .map(
      (item) => `
      <tr>
        <td>${item.title}</td>
        <td>${item.panel}</td>
        <td>${item.source}</td>
        <td title="${item.description}">${item.description}</td>
      </tr>
    `,
    )
    .join("");
}

/* ── Data fetching ── */

async function fetchIndicatorData(indicatorId) {
  const params = new URLSearchParams({
    indicator: indicatorId,
    countries: currentCountries.join(","),
    year_from: currentYearRange.from,
    year_to: currentYearRange.to,
  });
  return fetchJson(`/api/data?${params.toString()}`);
}

async function exportCurrentIndicator() {
  try {
    const params = new URLSearchParams({
      indicator: currentIndicator,
      countries: currentCountries.join(","),
      year_from: currentYearRange.from,
      year_to: currentYearRange.to,
      format: "csv",
    });
    const response = await fetch(`/api/data?${params.toString()}`);
    if (!response.ok) throw new Error(`Export failed: ${response.status}`);
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `${currentIndicator}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  } catch (err) {
    alert(`CSV export failed: ${err.message}`);
  }
}

/* ── Country dropdown ── */

async function populateCountries() {
  try {
    const countries = await fetchJson("/api/countries");
    countryMenu.innerHTML =
      `<input type="text" class="dropdown-search" placeholder="Search countries..." />` +
      countries
        .map(
          (c) => `
          <label class="dropdown-item" data-label="${c.label.toLowerCase()}">
            <input type="checkbox" value="${c.code}" ${currentCountries.includes(c.code) ? "checked" : ""} />
            ${c.label}
          </label>
        `,
        )
        .join("");
    updateCountryCount();

    const searchInput = countryMenu.querySelector(".dropdown-search");
    searchInput.addEventListener("input", () => {
      const query = searchInput.value.toLowerCase();
      countryMenu.querySelectorAll(".dropdown-item").forEach((item) => {
        item.style.display = item.dataset.label.includes(query) ? "" : "none";
      });
    });
  } catch (err) {
    countryMenu.innerHTML = `<div class="error-banner"><p class="error-message">Failed to load countries</p></div>`;
  }
}

function updateCountryCount() {
  const checked = countryMenu.querySelectorAll('input[type="checkbox"]:checked');
  countryCount.textContent = checked.length;
}

countryToggle.addEventListener("click", (e) => {
  e.stopPropagation();
  countryDropdown.classList.toggle("open");
});

document.addEventListener("click", (e) => {
  if (!countryDropdown.contains(e.target)) {
    countryDropdown.classList.remove("open");
  }
});

const debouncedReload = debounce(async () => {
  await loadDashboard();
}, 400);

countryMenu.addEventListener("change", (e) => {
  if (e.target.type === "checkbox") {
    currentCountries = [...countryMenu.querySelectorAll('input[type="checkbox"]:checked')].map((cb) => cb.value);
    updateCountryCount();
    debouncedReload();
  }
});

yearRange.addEventListener("change", () => {
  const [from, to] = yearRange.value.split(":").map(Number);
  currentYearRange = { from, to };
  debouncedReload();
});

exportButton.addEventListener("click", exportCurrentIndicator);

/* ── Dashboard loading ── */

async function loadDashboard() {
  /* Show skeletons */
  showKpiSkeletons();
  showSkeleton("market-chart");
  showSkeleton("labor-chart");
  showSkeleton("research-chart");
  document.getElementById("market-note").textContent = "";
  document.getElementById("labor-note").textContent = "";
  document.getElementById("research-note").textContent = "";

  let indicators;
  try {
    const raw = await fetchJson("/api/indicators");
    indicators = Object.fromEntries(raw.map((ind) => [ind.id, ind]));
  } catch (err) {
    document.getElementById("kpi-grid").innerHTML = `
      <div class="error-banner" style="grid-column: 1 / -1;">
        <p class="error-message">Failed to load dashboard: ${err.message}</p>
        <button type="button" onclick="location.reload()">Reload page</button>
      </div>
    `;
    return;
  }

  const selected = [
    "population_18_24",
    "tertiary_attainment_25_34",
    "employment_tertiary_25_64",
    "rd_expenditure_gdp",
  ];

  const results = await Promise.allSettled(selected.map((id) => fetchIndicatorData(id)));

  /* Build data map from settled results */
  const dataMap = {};
  const dataEntries = [];
  results.forEach((result, i) => {
    if (result.status === "fulfilled") {
      dataEntries[i] = result.value;
      dataMap[result.value.indicator.id] = result.value.rows;
    } else {
      dataEntries[i] = null;
      dataMap[selected[i]] = [];
    }
  });

  renderKpis(dataMap, indicators);
  renderIndicatorTable(indicators);

  /* Market panel (index 0) */
  const marketData = dataEntries[0];
  if (marketData) {
    currentIndicator = marketData.indicator.id;
    document.getElementById("tag-market").textContent = currentIndicator;
    initChart("market-chart");
    renderTrendChart("market-chart", marketData.rows, marketData.indicator);
    document.getElementById("market-note").textContent = makeNarrative(marketData.rows, marketData.indicator);
  } else {
    showError("market-chart", "Failed to load market data", debouncedReload);
  }

  /* Labor panel (index 2) */
  const laborData = dataEntries[2];
  if (laborData) {
    initChart("labor-chart");
    renderBarChart("labor-chart", laborData.rows, laborData.indicator);
    document.getElementById("labor-note").textContent = makeNarrative(laborData.rows, laborData.indicator);
  } else {
    showError("labor-chart", "Failed to load employment data", debouncedReload);
  }

  /* Research panel (index 3) */
  const researchData = dataEntries[3];
  if (researchData) {
    initChart("research-chart");
    renderBarChart("research-chart", researchData.rows, researchData.indicator);
    document.getElementById("research-note").textContent = makeNarrative(researchData.rows, researchData.indicator);
  } else {
    showError("research-chart", "Failed to load R&D data", debouncedReload);
  }
}

/* ── Init ── */

await populateCountries();
await loadDashboard();

window.addEventListener("resize", () => {
  Object.values(charts).forEach((chart) => chart.resize());
});
