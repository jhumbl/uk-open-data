# uk-open-data

A curation layer over UK public data, built to serve browsers.

Every Monday a pipeline re-downloads selected public datasets (transport,
policing, local government, …), tidies each into one analysis-ready,
schema-declared parquet, and publishes it — raw file, tidy parquet,
documentation and full version history — to Hugging Face dataset repos
that a browser app can query directly: DuckDB-Wasm range requests, plain
`fetch()`, no backend, no API keys. CORS and Range support are verified
mechanically on every run, not assumed.

This repo holds the code and metadata only — **never data**. See
[DESIGN.md](DESIGN.md) for the full architecture and the reasoning behind
it, and [CLAUDE.md](CLAUDE.md) for the working guide.

## Using the data

Start from [`catalog.json`](catalog.json) — the machine-readable index of
every published file: URLs, sha256, sizes, row counts, time coverage,
provenance and source health. Apps hardcode one URL (the catalog) and
discover everything else from it.

URL patterns:

    latest:     https://huggingface.co/datasets/<ns>/<family>/resolve/main/<filename>
    snapshot:   https://huggingface.co/datasets/<ns>/<family>/resolve/archive-YYYY-MM/<filename>
    any commit: https://huggingface.co/datasets/<ns>/<family>/resolve/<sha>/<filename>
    duckdb:     hf://datasets/<ns>/<family>/<filename>

Each Hugging Face repo's dataset card documents its tables' columns,
licence, cadence and interpretation caveats — read the caveats before
drawing conclusions; that context is the point of this project.

## Datasets

<!-- BEGIN GENERATED DATASETS TABLE (generate_readme.py) -->
| File | Description | Source | Cadence | Data |
| --- | --- | --- | --- | --- |
| `dft_veh0105_licensed_by_la.ods` (+ `.parquet`) | VEH0105: Licensed vehicles by body type, fuel type and local authority | DfT / DVLA | Quarterly | [dft-vehicle-licensing](https://huggingface.co/datasets/jhumbl/dft-vehicle-licensing) |
| `dft_veh0132_ulev_by_la.ods` (+ `.parquet`) | VEH0132: Licensed ultra low emission vehicles (ULEVs) by local authority | DfT / DVLA | Quarterly | [dft-vehicle-licensing](https://huggingface.co/datasets/jhumbl/dft-vehicle-licensing) |
<!-- END GENERATED DATASETS TABLE -->

## Running the pipeline

    pip install -r requirements.txt
    python run_all.py --no-hf              # fetch + validate only, no upload
    python run_all.py                      # full run (needs HF_TOKEN)
    python smoke_test.py                   # verify CORS + Range on published URLs

## Licence

Pipeline code: MIT. The data carries its publishers' licences (Open
Government Licence v3.0 unless a dataset's card says otherwise).
