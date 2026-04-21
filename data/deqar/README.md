# DEQAR Snapshot

Place the downloaded DEQAR CSV snapshot files in this directory:

- `data/deqar/deqar-institutions.csv`
- `data/deqar/deqar-reports.csv`
- `data/deqar/deqar-agencies.csv`

Default download commands:

```bash
mkdir -p data/deqar
curl -L https://backend.deqar.eu/static/daily-csv/deqar-institutions.csv -o data/deqar/deqar-institutions.csv
curl -L https://backend.deqar.eu/static/daily-csv/deqar-reports.csv -o data/deqar/deqar-reports.csv
curl -L https://backend.deqar.eu/static/daily-csv/deqar-agencies.csv -o data/deqar/deqar-agencies.csv
```

The app uses these repo-local defaults unless you override them with:

- `DEQAR_INSTITUTIONS_CSV_PATH`
- `DEQAR_REPORTS_CSV_PATH`
- `DEQAR_AGENCIES_CSV_PATH`
