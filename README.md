# UK Open Data

*Mercifully Open Source.*

A curation layer over UK public data, built to serve browsers — transport,
policing, food safety, local government and more, with a London/Westminster
focus. Every Monday a pipeline re-downloads each publisher's file, tidies
it into one analysis-ready, schema-declared parquet, and publishes it —
raw file, tidy parquet, documentation and full version history — to
Hugging Face dataset repos that a browser app can query directly:
DuckDB-Wasm range requests, plain `fetch()`, no backend, no API keys.
CORS and Range support are verified mechanically on every run, not
assumed.

This repo holds the code and metadata only — **never data**. See
[CLAUDE.md](CLAUDE.md) for the architecture, the design rationale and the
working guide.

## How it works

```
GitHub Actions (Mondays 06:00 UTC)
┌────────────────────────────────────────────────────────────────┐
│ run_all.py — for every sources/*/fetch.py:                     │
│   download → tidy to parquet → validate against declared schema│
│   validation fails? → quarantine (nothing uploads; Hugging Face│
│   keeps serving the last good version; other sources carry on) │
│ sync each dataset family to its Hugging Face repo              │
│   (hub skips unchanged bytes; 1st run of the month tags        │
│    archive-YYYY-MM — an immutable snapshot of everything)      │
│ clobber the "backup" GitHub release with current files         │
│ commit catalog.json — its git history is the audit log         │
│ smoke_test.py — prove CORS + Range on every published URL      │
└────────────────────────────────────────────────────────────────┘
```

## Using the data

Start from [`catalog.json`](catalog.json) — the machine-readable index of
every published file: URLs, sha256, sizes, row counts, time coverage,
provenance and source health. Apps hardcode one URL (the catalog) and
discover everything else from it:

```js
const catalog = await (await fetch(
  "https://raw.githubusercontent.com/jhumbl/uk-open-data/main/catalog.json"
)).json();
const entry = catalog.datasets["dft_veh0132_ulev_by_la.parquet"];
// cache-bust with the checksum so users get fresh data after each refresh
const res = await fetch(entry.download_url + "?v=" + entry.sha256.slice(0, 8));
// derived tables are parquet — read in-browser with hyparquet or duckdb-wasm
```

Or query straight from DuckDB (CLI, Python, R or Wasm):

```sql
SELECT ons_geography, quarter, "count"
FROM 'hf://datasets/jhumbl/dft-vehicle-licensing/dft_veh0132_ulev_by_la.parquet'
WHERE fuel = 'Total' AND keepership = 'Total'
LIMIT 10;
```

URL patterns:

    latest:     https://huggingface.co/datasets/<ns>/<family>/resolve/main/<filename>
    snapshot:   https://huggingface.co/datasets/<ns>/<family>/resolve/archive-YYYY-MM/<filename>
    any commit: https://huggingface.co/datasets/<ns>/<family>/resolve/<sha>/<filename>
    duckdb:     hf://datasets/<ns>/<family>/<filename>

Snapshot URLs serve with the same CORS + Range support as `main`, so apps
can query history — including publishers' silent revisions — directly.

Each Hugging Face repo's dataset card documents its tables' columns,
licence, cadence and interpretation caveats — read the caveats before
drawing conclusions; that context is the point of this project.

## Datasets

<!-- BEGIN GENERATED DATASETS TABLE (generate_readme.py) -->
| File | Description | Source | Cadence | Data |
| --- | --- | --- | --- | --- |
| `dft_veh0105_licensed_by_la.ods` (+ `.parquet`) | VEH0105: Licensed vehicles by body type, fuel type and local authority | DfT / DVLA | Quarterly | [dft-vehicle-licensing](https://huggingface.co/datasets/jhumbl/dft-vehicle-licensing) |
| `dft_veh0132_ulev_by_la.ods` (+ `.parquet`) | VEH0132: Licensed ultra low emission vehicles (ULEVs) by local authority | DfT / DVLA | Quarterly | [dft-vehicle-licensing](https://huggingface.co/datasets/jhumbl/dft-vehicle-licensing) |
| `fsa_fhrs_london_food_hygiene.parquet` (+ `_raw_xml.zip`) | Food hygiene ratings (FHRS) for every rated food business in London | FSA | Daily | [fsa-food-hygiene-london](https://huggingface.co/datasets/jhumbl/fsa-food-hygiene-london) |
| `gla_public_realm_trees.csv` (+ `.parquet`) | London Public Realm Trees: location, species and climate suitability of ~1.14m publicly maintained trees | GLA / GiGL | Occasional | [gla-trees](https://huggingface.co/datasets/jhumbl/gla-trees) |
| `police_crime_london.parquet` (+ `_outcomes.parquet`, `_stop_search.parquet`) | Street-level crime, outcomes and stop & search for London (Metropolitan & City of London Police), Dec 2010 to present | Home Office / data.police.uk | Monthly | [police-crime-london](https://huggingface.co/datasets/jhumbl/police-crime-london) |
<!-- END GENERATED DATASETS TABLE -->

## When a source breaks

Publishers move pages and change formats without warning; the pipeline is
built so that never serves you garbage. A file that fails validation is
quarantined: nothing is uploaded, the Hugging Face repo keeps serving the
last known-good version, and every other source carries on unaffected.
Consumers see stale-but-valid data — `catalog.json` records each source's
health and `last_success` says exactly how stale — and repeated failures
are auto-filed as GitHub Issues.

## Running the pipeline

    pip install -r requirements.txt
    python run_all.py --no-hf              # fetch + validate only, no upload
    python run_all.py                      # full run (needs HF_TOKEN)
    python smoke_test.py                   # verify CORS + Range on published URLs

To add a dataset, see **CLAUDE.md** for the source contract and
step-by-step instructions (the repo is designed to be maintained with
Claude Code, but the steps are the same for humans).

## Prior art

The pattern is a variant of
[git scraping](https://simonwillison.net/2020/Oct/9/git-scraping/) (Simon
Willison; GitHub's [Flat Data](https://octo.github.com/projects/flat-data)
develops the same idea): a scheduled workflow re-fetches changing
resources and the commit history becomes the change log. Here only
*metadata* is git-scraped — `catalog.json`'s history records when every
dataset changed — while the data itself lives on Hugging Face, following
the Actions → HF datasets → DuckDB stack proven in production by
[Datadex](https://github.com/davidgasquez/datadex) and
[Datania](https://github.com/davidgasquez/datania).

## Licences

Pipeline code: MIT. Each dataset carries its publisher's licence — see its
Hugging Face card (UK government sources are typically the
[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)).
