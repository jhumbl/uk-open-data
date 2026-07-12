# CLAUDE.md

Guidance for Claude Code (and humans) working in this repository.

**Read DESIGN.md first** for anything architectural — it records the
decisions already made, the alternatives already rejected, and why. This
file is the working guide: layout, contracts, runbooks, gotchas. Where the
two seem to disagree, DESIGN.md wins.

## What this repo is

A curation layer over UK public data, built to serve browsers. Every
Monday a GitHub Actions workflow re-downloads each source, tidies it into
analysis-ready parquet, validates it against a declared schema, and syncs
it to **Hugging Face dataset repos** (one per dataset family). The git
repo holds **only code and metadata — never data**. `main` on each HF repo
is the latest data; a monthly git tag `archive-YYYY-MM` snapshots history.

Stable URL patterns (safe to build apps against):

    latest:    https://huggingface.co/datasets/<ns>/<family>/resolve/main/<filename>
    snapshot:  https://huggingface.co/datasets/<ns>/<family>/resolve/archive-YYYY-MM/<filename>
    duckdb:    hf://datasets/<ns>/<family>/<filename>

`<ns>` is `HF_NAMESPACE` in `lib/common.py`. The serving contract — CORS +
HTTP Range on those URLs — is *verified* by `smoke_test.py` on every run,
never assumed (DESIGN.md §2 explains the episode behind this rule).

## Layout

    run_all.py                  Orchestrator: fetch/validate every source, sync
                                each family to HF, update catalog.json
    smoke_test.py               CORS + Range check on every published URL
    generate_readme.py          Regenerates README.md's dataset table
    lib/common.py               Shared helpers (download, hashing, parquet,
                                catalog I/O, HF URL builders)
    lib/hf_sync.py              Upload to HF: create_repo, create_commit
                                (hub skips unchanged files), monthly tags
    lib/cards.py                Renders each family's HF dataset card from
                                metadata — deterministic, no timestamps
    catalog.json                Machine-readable index: per-file sha256, size,
                                timestamps, URLs, stats, source health, HF
                                revisions. Committed weekly by CI — its git
                                history is the audit log.
    families.json               Family-level card metadata (title, description)
                                — the only metadata that has no per-source home
    sources/<name>/fetch.py     One fetcher per source (see contract below)
    sources/<name>/datapackage.json   Per-source truth: licence, cadence,
                                caveats, schema, hf_repo

## The dataset-family model

Data lives in one HF repo per **family** — defined by a simple test: *one
dataset card you can write honestly* (DESIGN.md §5). Files share a repo
when they share provenance, licence, cadence and caveats. Group by topic
within publisher (`dft-vehicle-licensing`), never by publisher
(`dft-everything`).

- The pipeline's unit stays the **individual source**: fetching,
  validation and quarantine are per-source; one table's format change
  never blocks its siblings. `hf_repo` in datapackage.json says where a
  source's outputs land.
- Filenames stay globally unique across ALL repos (publisher-prefixed
  source name), so regrouping is a one-field config change, never a
  collision hunt.
- Card generation asserts every source in a family declares the same
  licence — if that assertion fires, the family grouping is wrong: split
  it rather than weakening the card.

## Naming convention

Every source directory — and therefore every published filename, since
filenames must be prefixed with their source directory name — starts with
a short lowercase **publisher code**, e.g. `dft` (Department for
Transport), `ons` (Office for National Statistics):

    <publisher-code>_<table-id>_<short-description>

Example: `dft_veh0105_licensed_by_la` — DfT, table VEH0105, licensed
vehicles by local authority.

`sources/` stays a flat directory — no per-publisher subfolders.
Publishers get renamed, merged or split over time; the prefix gives
grouped-by-publisher browsability without the fragility of real nesting.
Family repo names are lowercase-hyphenated: `dft-vehicle-licensing`.

## The source contract

Each `sources/<name>/fetch.py` must expose:

```python
def fetch(out_dir: Path) -> list[Path]:
    """Write one or more files into out_dir and return their paths."""
```

Rules:
- Raise an exception on failure. Do NOT catch-and-continue at the top
  level; run_all.py isolates failures per source and reports them.
- Output filenames must be globally unique across ALL sources and stable
  across runs (they ARE the public URL). Filenames must match the source
  directory name exactly: `dft_veh0105_licensed_by_la.ods`, not `data.ods`.
- Always publish the raw file as downloaded (provenance), plus **one
  long/tidy-format parquet** written via `lib.common.write_parquet` — no
  CSV/JSON siblings. The tidy step is best-effort: wrap it in try/except
  so a publisher's format change degrades the output rather than failing
  the run (see tidy_to_parquet in the VEH0105 source). Aggregate parquets
  (`<name>_agg_<slug>.parquet`) are added when a real dashboard needs one,
  not speculatively.
- The derived parquet's columns are declared as a Frictionless Table
  Schema (`schema.fields`) on its resource in datapackage.json — the
  single source of truth. fetch.py builds its column order from it
  (`lib.common.load_resource_schema`) and validate() enforces it
  (`lib.common.check_parquet_schema`), so a schema/code mismatch
  quarantines the source instead of publishing a surprise. The schema is
  API just like the filename (invariant 6).
- Optionally expose `validate(files: list[Path]) -> None`, raising with a
  clear message if the output doesn't look right (magic bytes, size
  bounds, expected columns, plausible row counts — shape, not values). If
  validation raises, run_all.py quarantines the files: nothing is
  uploaded, HF keeps serving the previous good version, the source is
  flagged. Every source SHOULD have one; publishers change formats
  without warning.
- Two more optional hooks feed catalog.json: `stats(files) ->
  dict[filename, dict]` (use `lib.common.parquet_stats`: row_count plus
  min/max of a period column whose lexicographic order equals temporal
  order), and a module-level `FETCHED_FROM: dict[filename, url]`
  populated during fetch() with the URL each file was actually downloaded
  from. Both are best-effort — failures warn, never quarantine.
- If the publisher's direct URL is unstable, scrape their stable landing
  page for the current link (see the VEH0105 source for the pattern).
- datapackage.json must include `hf_repo` (the family repo the outputs
  land in) and the family must have an entry in families.json.

## Adding a new source (the most common task)

1. Decide the family (DESIGN.md §5). New family → add an entry to
   `families.json` (title + honest description).
2. `mkdir sources/<publisher>_<table-id>_<short-description>` and write
   `fetch.py` following the contract. Copy the VEH0105 source as a
   starting point.
3. Write `datapackage.json`: name, title, description, homepage, licence,
   `sources` (long-form citation), `update_cadence` (full sentence),
   caveats (these matter — data + context is the whole product), a
   `schema.fields` block on each parquet resource, `hf_repo`, and the two
   short display fields for the README table: `provider` (e.g.
   `"DfT / DVLA"`) and `cadence` (e.g. `"Quarterly"`).
4. Test locally without uploading: `python run_all.py --only <name>
   --no-hf`, inspect `data/` and the new catalog.json entries. Run it
   TWICE — the second run must print `unchanged` for every file (the
   determinism gotcha below).
5. Full test against real HF: `python run_all.py --only <name>` (needs
   HF_TOKEN), then `python smoke_test.py`. The repo is created
   automatically; check the card renders and the viewer shows the table.
6. `python generate_readme.py` to add the source to the README table
   (it appears only after a successful sync).
7. Do NOT commit anything in `data/` (gitignored) and do not hand-edit
   generated artifacts: catalog.json, the README table, or HF dataset
   cards (all three are overwritten by the next run).

## Local commands

    pip install -r requirements.txt
    python run_all.py --no-hf              # everything, no upload
    python run_all.py --only <source>      # one source, full sync
    python run_all.py                      # full weekly run
    python smoke_test.py                   # verify the serving contract
    python generate_readme.py              # refresh README dataset table
    git diff catalog.json                  # see what changed

Uploading needs a WRITE token from
https://huggingface.co/settings/tokens — either as `HF_TOKEN` in the
environment (CI has it as an Actions secret of that name) or stored once
via `hf auth login --token <token>`. `--no-hf` runs everything except
upload and works without the token or the huggingface_hub package.

## When a source breaks (runbook)

External publishers move pages and change formats without warning. The
pipeline is built so this is contained and easy to repair:

**How you'll know.** Fetches are retried with backoff (3 attempts — see
`_with_retries` in lib/common.py) before a source counts as failed, so
plain network blips rarely surface. When a source does fail, the workflow
run shows as failed, and from the **2nd consecutive failure** it opens (or
comments on) a GitHub Issue titled `Source broken: <name>` — a single red
run with no issue usually means a transient that will self-heal next week.
catalog.json records health under `"sources"` — `status` (`fetch_failed` =
couldn't get the file; `validation_failed` = got a file but it doesn't
look right; `config_failed` = bad hf_repo/families.json wiring),
`consecutive_failures`, `last_error`. Family-level upload problems appear
under `"families"` as `sync_failed`.

**What's happened to the data.** Nothing bad: broken output is
quarantined, so the HF repo still serves the last good version, and all
other sources continued updating normally. Consumers see stale-but-valid
data; `last_success` in the catalog says exactly how stale.

**To fix:**
1. Reproduce locally: `python run_all.py --only <name> --no-hf`.
2. Diagnose — the common cases:
   - `fetch_failed`: the landing page or link pattern changed. Open the
     source's homepage (in its datapackage.json), find where the file
     lives now, update the URL/regex in fetch.py.
   - `validation_failed`: the file's format changed. Download it, inspect,
     update the parsing (e.g. tidy_to_parquet) AND validate() — and the
     datapackage.json schema block if the derived columns change. If the
     *raw* format genuinely changed (e.g. ODS → xlsx), keep the published
     filename stable if at all possible (invariant 6); if the extension
     must change, publish both old and new names for a transition period
     and note it in the README.
   - `sync_failed`: an HF-side problem — check the token is valid and the
     hub status page before changing any code.
3. Re-run `python run_all.py --only <name> --no-hf` until it reports the
   files CHANGED (or unchanged) with no failures; sanity-check `data/`.
4. Commit, push, then trigger the workflow manually (Actions → Run
   workflow). Reintegration is automatic: the next successful run uploads
   the files, health flips back to `ok`, and the catalog's `last_changed`
   records the gap.
5. Close the `Source broken` issue.

There is no special recovery state — a fixed source is just a source
whose next run succeeds.

## Invariants — do not break these

(DESIGN.md §10 is the authoritative list; this is the working summary.)

1. **No data in git.** Only code, docs, and catalog/datapackage metadata
   are committed. If a change would commit a data file, stop.
2. **catalog.json is generated.** Change run_all.py, never the file; its
   committed history is the audit log — preserve that property.
3. **Schemas are declared and enforced.** Additive changes fine; breaking
   ones need a transition (old + new shape published in parallel).
4. **One source failing must never block the others** — and one family's
   sync failing must never block other families.
5. **The serving promise is tested, not assumed.** If we claim it in a
   README, CI proves it. Never weaken smoke_test.py to make a run green.
6. **Filenames are API — and so are derived-parquet schemas.** Renaming a
   published file or moving it between families is a URL migration:
   permitted (sole consumer) but always deliberate, never accidental.
7. **Respect publishers.** Accurate User-Agent (lib/common.py), weekly
   fetch cadence at most, licences honoured and displayed.
8. **Snapshots are immutable.** Never move or delete an `archive-*` tag.

## Conventions

- Python 3.12, stdlib + requests + pandas + odfpy + pyarrow +
  huggingface_hub only. Justify any new dependency in the commit message
  and add it exact-pinned to requirements.txt.
- Type hints and docstrings on public functions. Keep scripts runnable as
  plain `python <script>.py` — no packaging/install step.
- Print progress lines (this runs headless in CI; logs are the UI).
- Prefer boring, readable code over cleverness. Sources are maintained
  rarely, by people (or Claudes) with no memory of writing them.

## Gotchas

- **Byte-determinism is load-bearing twice over**: identical content must
  produce identical bytes on every OS, or (a) every run flags spurious
  "changed" files and (b) the HF uploader commits new blobs for unchanged
  data, wrecking chunk dedup and the no-commit-when-unchanged property.
  The culprits are platform-dependent *metadata* defaults, not the data —
  pin them: gzip needs `mtime=0`, zip entries need fixed timestamps AND
  `create_system = 3` (defaults differ between Windows and Unix — this
  bit us in v1), CSVs need `lineterminator="\n"`. Parquet is handled by
  `lib.common.write_parquet` (strips the pandas metadata blob, pins
  compression), but the footer's `created_by` embeds the pyarrow version
  and cannot be removed — that's why requirements.txt pins exact `==`
  versions. Bumping pandas/pyarrow will flag every parquet as "changed"
  once; do it deliberately and say so in the commit message. Sanity check
  for any new source: run twice, second run must say "unchanged" and the
  HF sync must report "no changes — no commit".
- **Generated dataset cards must be deterministic too**: no timestamps,
  no run metadata in lib/cards.py output, or every weekly run creates a
  pointless HF commit and the run-twice check fails.
- The hub does the skip-unchanged work server-side (`create_commit` drops
  no-op additions and refuses empty commits) — run_all.py deliberately
  re-uploads everything valid and lets HF decide. Don't add client-side
  "only upload changed files" logic; the sha256 change detection in
  run_all.py is for the catalog and humans, not the uploader.
- DfT publishes ODS files with title/notes rows above the header and
  layouts that shift between releases — hence the defensive tidy steps.
- Most weekly runs find quarterly/annual sources unchanged. "0 files
  changed" is a normal, successful run, not a bug.
- GitHub disables the cron after 60 days without repo activity; the
  weekly catalog commit prevents this while the workflow is green. If the
  workflow has been failing for weeks, re-enable it in the Actions tab
  after fixing.
- HF repos are created by the pipeline (`create_repo(exist_ok=True)`);
  adding a source never involves the HF website. But the *namespace*
  (`HF_NAMESPACE` in lib/common.py) and the token are account-level,
  manual, one-time setup.
- lib/hf_sync.py resolves the token as HF_TOKEN env var first, then the
  `hf auth login` cache, and fails with a useful message if neither is
  present — instead of a 401 mid-run.
