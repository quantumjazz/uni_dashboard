# University Leadership Dashboard

University leadership dashboard focused on a small set of strategic higher-education questions for Bulgaria, the EU benchmark, and selected comparison countries. Phase 1 is implemented with FastAPI, a Eurostat client, SQLite caching, YAML-based indicator configuration, and a lightweight static frontend.

Product planning reference for the next phase lives in [docs/recommended_dashboard_scope.md](docs/recommended_dashboard_scope.md).

## Project structure

```text
docs/
  recommended_dashboard_scope.md
backend/
  app/
    api/
    cache/
    clients/
    models/
    services/
    utils/
    config.py
    main.py
frontend/
  src/
config/
  indicators.yaml
data/
  cache/
```

## Included in Phase 1

- FastAPI backend with required endpoints:
  - `/health`
  - `/api/indicators`
  - `/api/data`
  - `/api/countries`
  - `/api/metadata`
- Eurostat-first data ingestion with a unified internal schema:
  - `source`
  - `dataset`
  - `indicator`
  - `country`
  - `year`
  - `value`
  - `unit`
  - `note`
- SQLite-backed cache with TTL for normalized indicator rows and raw payloads
- YAML indicator registry for easy extension
- Minimal leadership-oriented frontend with KPI cards, trend/comparison charts, metadata table, and CSV export

## Indicator design

Indicators live in [config/indicators.yaml](config/indicators.yaml). Each indicator defines:

- strategic question and dashboard panel
- official source and dataset
- dimensions passed to the source API
- default comparison countries
- title, description, frequency, and unit

This keeps data selection separate from the code so new indicators can be added without changing endpoint contracts.

## Backend design

### Main modules

- [backend/app/config.py](backend/app/config.py): environment-driven settings
- [backend/app/clients/eurostat.py](backend/app/clients/eurostat.py): Eurostat API client and JSON-stat parser
- [backend/app/cache/database.py](backend/app/cache/database.py): SQLite initialization and connection helpers
- [backend/app/cache/repository.py](backend/app/cache/repository.py): cache and normalized data persistence
- [backend/app/services/indicator_registry.py](backend/app/services/indicator_registry.py): YAML indicator loading
- [backend/app/services/data_service.py](backend/app/services/data_service.py): orchestration, cache lookup, summaries
- [backend/app/api/routes.py](backend/app/api/routes.py): REST endpoints

### Data model

The internal normalized observation model is defined in [backend/app/models/schemas.py](backend/app/models/schemas.py). The main row shape is:

```json
{
  "source": "eurostat",
  "dataset": "demo_pjan",
  "indicator": "population_18_24",
  "country": "BG",
  "year": 2024,
  "value": 432100.0,
  "unit": "persons",
  "note": "optional note"
}
```

## Frontend design

The frontend is intentionally simple in phase 1:

- static files in [frontend/src/index.html](frontend/src/index.html), [frontend/src/styles.css](frontend/src/styles.css), and [frontend/src/app.js](frontend/src/app.js)
- served directly by FastAPI to avoid an extra build system
- ECharts loaded from CDN
- Bulgaria highlighted by default
- narrative text generated from simple comparison rules

## Run locally

1. Create and activate a virtual environment.
2. Install dependencies.
3. Start the FastAPI app.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn backend.app.main:app --reload
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000).

## Environment variables

Optional environment variables:

```bash
APP_ENV=development
DEBUG=true
CACHE_TTL_HOURS=24
EUROSTAT_BASE_URL=https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data
DATABASE_URL=sqlite:////absolute/path/to/dashboard.db
DEQAR_INSTITUTIONS_CSV_PATH=/absolute/path/to/deqar-institutions.csv
DEQAR_REPORTS_CSV_PATH=/absolute/path/to/deqar-reports.csv
DEQAR_AGENCIES_CSV_PATH=/absolute/path/to/deqar-agencies.csv
EHESO_ETER_INSTITUTIONS_CSV_PATH=/absolute/path/to/eter-institutions.csv
NEAA_BASE_URL=https://www.neaa.government.bg
NEAA_HIGHER_INSTITUTIONS_PATH=/en/accredited-higher-education-institutions/higher-institutions
```

The quality page now reads DEQAR from the downloadable CSV snapshot rather than a live DEQAR API. Download the current files from [EQAR's dataset page](https://www.eqar.eu/qa-results/download-data-sets/) and point the backend at the local CSV paths above.

For Bulgarian institutions, the quality page also overlays live NEAA institutional-accreditation context from the official NEAA higher-education institutions page. The defaults above target the public English listing page and usually do not need to be changed.

If you have an EHESO/ETER institution export, you can also point `EHESO_ETER_INSTITUTIONS_CSV_PATH` at a local CSV seed. The crosswalk loader will ingest it automatically on first use and merge it into the institution registry before OpenAlex and DEQAR matching runs. A seed-format guide lives in [data/eheso/README.md](data/eheso/README.md).

## Notes and next steps

- OECD and World Bank clients are not implemented yet; the internal structure is ready for them.
- Eurostat dataset dimension codes can vary by dataset. If a configured indicator returns no data, adjust the dimension codes in the YAML file rather than changing the service layer.
- For phase 2, add OECD ingestion, richer metadata lookup, and more leadership-specific comparison panels.
- For the recommended product scope and source strategy for the next dashboard phase, see [docs/recommended_dashboard_scope.md](docs/recommended_dashboard_scope.md).
