# EHESO / ETER Seed

Place an optional institution seed CSV at:

- `data/eheso/eter-institutions.csv`

Or point the app at a different file with:

- `EHESO_ETER_INSTITUTIONS_CSV_PATH=/absolute/path/to/eter-institutions.csv`

The ingest is flexible, but it works best when the CSV includes:

- `eter_id`
- `country_code`
- `canonical_name`

Useful optional columns:

- `official_name`
- `aliases`
- `homepage_url`
- `institution_type`
- `legal_status`
- `ror`
- `openalex_id`
- `wikidata_id`
- `erasmus_code`

Alias values can be separated with `;` or `|`.

The seed is loaded into the institution crosswalk automatically and is meant to improve OpenAlex <-> DEQAR matching before fuzzy fallback logic is used.
