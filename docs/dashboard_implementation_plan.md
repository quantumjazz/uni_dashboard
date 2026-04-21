# Dashboard Implementation Plan

Last updated: 2026-04-07

## Objective

Convert the recommended public-data MVP scope into an implementation path that fits the current codebase:

- keep the FastAPI + static frontend setup
- extend the existing Eurostat pipeline instead of replacing it
- add page-based navigation and page metadata
- build the Market and Demand page first
- leave the Overview page until the end, as recommended in the scope

## Current Baseline

The current application already provides a useful phase-1 foundation:

- a single generic data endpoint in `backend/app/api/routes.py`
- YAML-driven indicator definitions in `config/indicators.yaml`
- a Eurostat-only orchestration layer in `backend/app/services/data_service.py`
- a normalized `DataPoint` model in `backend/app/models/schemas.py`
- a single static dashboard page in `frontend/src/index.html` and `frontend/src/app.js`

The main limitations for phase 2 are:

- the frontend is one hard-coded page rather than page-driven navigation
- the API can only fetch one indicator at a time
- the current row model assumes one value per `indicator + country + year`
- the cache schema cannot safely store field breakdowns
- institution-level sources such as OpenAlex, CORDIS, and DEQAR do not have dedicated service contracts yet

## Architecture Decisions

### 1. Add a page registry, not more hard-coded frontend sections

Keep indicators declarative and add a second configuration layer for pages.

Create `config/pages.yaml` with:

- page id and route slug
- title and page description
- page status: `active`, `planned`, or `blocked_by_credentials`
- required controls: countries, year range, institution selector, export
- panel definitions
- panel-to-indicator mappings
- context type: `country_context` or `institution_context`

This keeps page structure out of `frontend/src/app.js` and avoids another round of hard-coded panels.

### 2. Keep Eurostat generic, but make institution sources source-specific

Do not force OpenAlex, CORDIS, and DEQAR into the same row shape as Eurostat.

Recommended rule:

- country-context time series continue to use the generic indicator pipeline
- institution-level research, projects, and quality data get dedicated API families

This matches the scope document's product rule that different decision layers should not be forced into one model.

### 3. Build the navigation shell first, but make Market the first real page

The top navigation should expose the five MVP destinations immediately:

- Overview
- Market
- Outcomes
- Research
- Quality

However, the default route should be `Market` until the other pages exist. The `Overview` page should be shown as coming later or inactive until the synthesis layer is actually ready.

### 4. Extend the observation model before adding field-based charts

The Market page requires field breakdowns for entrants and graduates. That means the current normalized row contract needs to evolve before those charts can be built safely.

## Backend Plan

### A. Page metadata layer

Add new backend modules:

- `backend/app/services/page_registry.py`
- page-related Pydantic models in `backend/app/models/schemas.py`

Add endpoint:

- `GET /api/pages`

Response should include enough metadata for the frontend to render navigation and page controls without duplicating config in JavaScript.

Example response shape:

```json
[
  {
    "id": "market",
    "slug": "market",
    "title": "Market and Demand",
    "status": "active",
    "context_type": "country_context",
    "controls": ["countries", "year_range", "export"],
    "panels": [
      {
        "id": "population_trend",
        "title": "Population aged 18-24 trend",
        "indicator_ids": ["population_18_24"],
        "chart_type": "line"
      }
    ]
  }
]
```

Why this first:

- it removes page structure from the frontend
- it gives the navigation and panel layout a single source of truth
- it allows `Overview` to stay defined but disabled until later

### B. Batch data endpoint for page hydration

Keep the current single-indicator endpoint for direct access, but add:

- `GET /api/data/batch`

Query parameters:

- `indicators=population_18_24,population_25_34,...`
- `countries=BG,EU27_2020,RO,PL`
- `year_from=2015`
- `year_to=2024`

Response shape:

```json
{
  "results": {
    "population_18_24": { "...": "existing DataResponse shape" },
    "population_25_34": { "...": "existing DataResponse shape" }
  }
}
```

Implementation notes:

- add `DataService.get_many_indicator_data(...)`
- reuse the existing per-indicator fetch path internally
- return partial success metadata when one indicator fails and others succeed

Why this matters:

- each routed page will need 4 to 6 indicator calls
- the current frontend already compensates with `Promise.allSettled`, but page-level hydration should move server-side
- it keeps the frontend simple while still preserving the generic indicator pipeline

### C. Richer observation model for breakdowns

Extend `DataPoint` to support multiple series within one indicator.

Recommended additions:

- `series_key: str | None`
- `series_label: str | None`
- `dimensions: dict[str, str]`

Recommended database changes:

- add `series_key` column to `data_points`
- add `dimensions_json` column to `data_points`
- change the primary key from `(indicator, country, year)` to `(indicator, country, year, series_key)`

Why this is required:

- `new entrants by field` and `graduates by field` produce multiple rows for the same country and year
- the current schema would either overwrite rows or reject them
- the current parser already sees all Eurostat dimensions, but throws most of them away

Eurostat client changes:

- preserve non-time, non-geo dimensions on parse
- allow page configs to specify a `breakdown_dimension`
- continue to support `aggregate_dimension` for cases such as summing ages

### D. Indicator config additions

Expand `config/indicators.yaml` for the Market page with new Eurostat-backed indicators:

- `population_18_24` already exists
- `population_25_34` already exists
- `tertiary_enrolment_total` from `educ_uoe_enrt01`
- `new_entrants_by_field` from `educ_uoe_ent01` or `educ_uoe_ent02`
- `graduates_by_field` from `educ_uoe_grad02`
- rename or replace `international_students_share`

The current `international_students_share` indicator is mislabeled because it returns `persons`, not a share. Before reusing it on the Market page, rename it to something explicit such as `international_students_count`.

Indicator-level additions that will help phase 2:

- `page_ids`
- `breakdown_dimension`
- `display_unit`
- `normalization_rule` for indicators where Bulgaria vs EU absolute comparison would be misleading

### E. Institution selector foundation

Add a small institution registry contract now, even if only Research and Quality will use it later.

Recommended endpoint:

- `GET /api/institutions/search?query=sofia`

Phase-2 behavior:

- return configured defaults when no external client is implemented yet
- later back with OpenAlex institution search

This lets the frontend add the peer university selector once, then show or hide it depending on the page.

### F. Source-specific API families for later pages

Add these only after the Market page is live, but design for them now:

- `GET /api/research/institutions/{institution_id}/summary`
- `GET /api/research/institutions/{institution_id}/publications`
- `GET /api/projects/institutions/{institution_id}`
- `GET /api/quality/institutions/{institution_id}`

Expected source ownership:

- OpenAlex for research metrics
- CORDIS for project counts, partners, and EU contribution
- DEQAR for quality status and reports

Important implementation rule:

- if credentials for CORDIS or DEQAR are missing, the route should return a structured `blocked_by_credentials` response rather than pretending the data is unavailable

## Frontend Navigation Plan

### Routing approach

Keep the static frontend and add lightweight client-side routing.

Recommended first implementation:

- use hash routes such as `#/market`, `#/outcomes`, `#/research`

Reason:

- it works with the current `StaticFiles` mount immediately
- it avoids adding server-side history fallback just to get multi-page navigation
- it keeps the deployment model unchanged

If clean URLs become important later, add a FastAPI catch-all that serves `index.html` for non-API routes and switch from hash routing to history routing.

### Frontend structure

Split the current monolithic `frontend/src/app.js` into small ES modules:

- `app.js` for bootstrapping
- `router.js` for route state
- `pages.js` for page rendering
- `api.js` for `/api/pages`, `/api/data/batch`, `/api/institutions/search`
- `charts.js` for ECharts helpers
- `controls.js` for countries, year range, and institution selector state

No build tool is required for this; native browser modules are enough.

### Navigation and controls behavior

Global navigation:

- Overview
- Market
- Outcomes
- Research
- Quality

Global controls:

- comparison countries on country-context pages
- year range on pages driven by time series
- peer university selector only on institution-context pages
- export button with page-aware label

Recommended rendering rules:

- keep control state when moving between pages
- hide controls the active page does not use
- show source and latest available year on every chart card
- visually separate `country context` panels from `institution context` panels

### Export behavior

For the first implementation:

- keep CSV export
- scope export to the active page's primary panel or active panel dataset

After batch page data exists, add a page-level export that downloads a structured bundle for all visible panels.

## First Page To Build: Market and Demand

### Why Market first

This matches the scope document's recommended build priority and also fits the current codebase best:

- it is almost entirely Eurostat-backed
- two of its core indicators already exist
- it does not depend on credentials for OpenAlex, CORDIS, or DEQAR
- it forces the right backend changes early: page metadata, batch loading, and breakdown support

### Route and status

- route: `#/market`
- default landing page until `Overview` is ready
- navigation label: `Market`

### Initial page contents

Recommended panels for the first build:

1. Market size trend

- `population_18_24`
- `population_25_34`
- line chart, Bulgaria plus selected comparison countries

2. Tertiary enrolment benchmark

- `tertiary_enrolment_total`
- latest-year comparison bar chart

3. New entrants by field

- `new_entrants_by_field`
- grouped bar or heatmap by field and year

4. Graduates by field

- `graduates_by_field`
- grouped bar or heatmap by field and year

5. International students trend

- `international_students_count`
- trend chart with note that this is country-level internationalisation context, not university performance

### KPI row for the Market page

Use three to four KPI cards:

- latest Bulgaria population aged 18-24
- five-year change in Bulgaria population aged 18-24
- latest Bulgaria population aged 25-34
- latest international tertiary students count

Avoid a Bulgaria vs EU narrative on absolute population counts. For these indicators, use trend language instead of direct absolute-gap language.

### Guardrails for this page

- clearly label all metrics as national context
- do not imply these are direct institutional demand figures
- do not narrate Bulgaria vs EU gaps for absolute-size indicators without normalization
- where meaningful, compare change rates rather than raw levels

### Definition of done for the Market page

- navigation shell exists
- `Market` is a routed page
- page metadata is loaded from the backend
- the Market page hydrates through one batch API call
- new field breakdown indicators render correctly
- source and latest-year tags are visible on all panels
- export works for the Market page

## Recommended Delivery Sequence

### Step 1. Foundation

- add `config/pages.yaml`
- add page models and `GET /api/pages`
- add hash-based router and page shell

### Step 2. Data contract upgrade

- extend `DataPoint`
- migrate cache schema for `series_key` and `dimensions_json`
- update Eurostat parsing to preserve breakdown dimensions
- add `GET /api/data/batch`

### Step 3. Market page

- add missing Market indicators
- build the Market route and panels
- update export behavior for page context

### Step 4. Outcomes page

- reuse the same page shell and batch endpoint
- add outcomes-focused Eurostat indicators and page copy

### Step 5. Research page

- add OpenAlex client and research endpoints
- introduce institution selector behavior

### Step 6. Quality page

- add DEQAR integration and credential-aware blocked states

### Step 7. Overview page

- build last, after the other pages define the final KPI and summary model

## Validation Plan

Add at least these checks during implementation:

- backend tests for page registry loading
- backend tests for batch endpoint behavior
- backend tests for Eurostat dimension parsing and cache persistence with `series_key`
- manual frontend verification for route changes, control persistence, and export scope

## Immediate Next Build Task

The next implementation task should be:

1. add `config/pages.yaml` and `GET /api/pages`
2. refactor the frontend into a routed page shell using hash navigation
3. make `#/market` the default page
4. then upgrade the data model for field breakdowns before building Market charts 3 and 4

That sequence gets the information architecture in place first, while still following the scope document's recommendation to build Market before Overview.
