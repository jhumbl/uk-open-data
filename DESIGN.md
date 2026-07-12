# Design & Philosophy

The founding design document for the rebuilt open-data pipeline. This
records not just *what* the architecture is, but *why* — including the
failure that motivated the rebuild and the alternatives we rejected.
Written 2026-07-13, before the first line of the new repo.

---

## 1. What this project is

A **curation layer over UK public data, built to serve browsers.**

A pipeline re-downloads selected public datasets (transport, policing,
local government, and whatever else the blog needs) on a weekly schedule,
tidies each one into analysis-ready parquet, documents its caveats, and
publishes it — with full version history — somewhere a **browser app can
query it directly**: DuckDB-Wasm range requests, plain `fetch()`, no
backend, no API keys.

The primary consumer is a separate website/blog repo: dashboards, apps,
reports and tools built on this data. R, Python and native DuckDB users
are welcome secondary consumers, but every design decision here is judged
**browser-first**.

The project's value is *not* mirroring (publishers already host their own
files). It is:

- **Tidying** — publishers ship ODS files with shifting layouts and title
  rows; we ship one clean, schema-declared, long-format parquet.
- **Context** — every dataset carries licence, provenance, cadence and
  honest interpretation caveats. Data without context is how numbers get
  misused.
- **History** — publishers overwrite their files. We keep every version,
  which means we can catch and show *silent revisions* — something the
  official portals structurally cannot do. For a data blog, revisions are
  content.

No existing project occupies this niche for UK/London civic data. The
architecture pattern itself, however, is well-proven (see §9, Prior art).

## 2. The lesson this design is built on

The first version of this project stored data as GitHub Release assets and
its README promised: *"Release assets are served with permissive CORS, so
a browser app can fetch them directly."* That claim was **false** — GitHub's
release CDN sends no `Access-Control-Allow-Origin` header, repo owners
cannot change that, and the primary consumer (a browser) was locked out of
the primary URLs. The claim sat in the README from the initial commit and
nothing caught it, because every consumer we actually exercised (Python,
curl, CI) ignores CORS. It surfaced only when an outside reader pointed
it out.

Three rules fall out of that episode and shape everything below:

1. **Don't build promises on behaviour you don't control and don't test.**
   The serving contract must be verified, mechanically, on every run
   (§7, smoke test).
2. **The consumer path is the thing to test.** Validating our files is
   necessary but not sufficient; CI must exercise what a *browser* does.
3. **Hold platforms loosely.** Free infrastructure changes under you.
   Every platform dependency needs a priced exit: what breaks, what it
   costs to leave, what triggers leaving.

## 3. Architecture

Three cleanly separated repos, one platform boundary:

    ┌─────────────────────────┐     weekly cron      ┌──────────────────────────────┐
    │  GitHub: pipeline repo  │ ───────────────────▶ │  Hugging Face: dataset repos │
    │  code, metadata,        │   fetch → validate   │  (one per dataset family)    │
    │  catalog.json, CI       │   → tidy → upload    │  raw + parquet + card        │
    └─────────────────────────┘                      │  main = latest               │
               │                                     │  tags  = monthly snapshots   │
               │ backup: --clobber current           └──────────────────────────────┘
               ▼ files to one GitHub release                        ▲
    ┌─────────────────────────┐                                     │ CORS + Range
    │  GitHub: release backup │                                     │ (verified weekly)
    └─────────────────────────┘                      ┌──────────────────────────────┐
                                                     │  presentation repo(s)        │
                                                     │  blog, dashboards, tools     │
                                                     └──────────────────────────────┘

**The pipeline repo (GitHub)** holds code, per-source metadata, the
catalog, and CI. It never holds data — not in git, and (except the dumb
backup release) not in releases either.

**The data lives on Hugging Face**, in one dataset repo per *dataset
family* (§5). HF dataset repos are git repos over chunk-deduplicated
storage, which collapses three subsystems from v1 into platform features:

| Concept   | v1 (GitHub Releases)                   | Now (HF)                                  |
|-----------|----------------------------------------|-------------------------------------------|
| Latest    | rolling `latest` release, `--clobber`  | `main` branch — `resolve/main/<file>`      |
| Snapshots | `archive-YYYY-MM` releases, size-tiered| git tag per month — `resolve/<tag>/<file>` |
| Change log| catalog.json git history               | that, plus HF commit history               |

**The presentation layer is a separate repo** (Quarto/Astro/whatever it
becomes — not this project's concern). The entire contract between the
two is: `catalog.json` + stable HF URLs. Aggregate parquets built *for*
dashboards still live on the data side of that line, because they are
published, versioned, schema-declared data artifacts.

**The backup** is one rolling GitHub release that the weekly run clobbers
with the current files. ~10 lines of workflow, no logic. It exists so
that an HF account problem can never cost us current data (deep history
would be recovered by cloning the HF repos while notice lasts — and most
sources' history is independently re-fetchable anyway, e.g. DfT tables
embed their full time series; data.police.uk retains its monthly archives
permanently).

## 4. Why Hugging Face (and why not the alternatives)

Requirements: free, CORS + HTTP range requests (DuckDB-Wasm), no file-size
squeeze (largest file ~900 MB), version history, weekly CI uploads,
tolerable platform risk for a sole-developer project.

**HF meets all of them, verified empirically** (2026-07): `resolve/` URLs
return `Access-Control-Allow-Origin` and 206 partial content on both
redirect hops, with `Content-Range` explicitly exposed. Public datasets
get 1 TB free. DuckDB has native `hf://` support; the hub gives every
dataset a free browsable viewer and SQL console. The same architecture
(GitHub Actions → HF datasets → DuckDB) has run in production for years
(Datadex/Datania — §9).

Storage arithmetic for versioning: chunk-level dedup means a new version
of a parquet costs roughly its changed row groups. Even assuming **zero**
dedup, our monthly-updating ~1.5 GB of police data is ~19 GB/year against
a 1 TB allowance — decades of runway, with `super_squash_history` as a
relief valve that trades history for space if ever needed.

Rejected alternatives, for the record:

- **GitHub Releases as serving layer** — no CORS, not configurable. (Fine
  as archival storage; that's why the backup release survives.)
- **raw.githubusercontent.com** — has CORS, but serves git-committed files
  only: puts data in git (violates a core invariant, unbounded history
  growth, 100 MB/file).
- **GitHub Pages via workflow artifact** — has CORS, keeps data out of
  git, but ~100 MB/file and ~1 GB/site can't hold the police parquets
  unpartitioned.
- **Cloudflare R2 (+ custom domain)** — technically excellent, zero egress
  fees, and owning the domain would make URLs permanently stable. Rejected
  on cost/ceremony: requires a paid domain and a card on file. Explicitly
  the first thing to revisit if the project earns real traffic (§8).
- **Cloudflare Worker CORS proxy over releases** — custom infrastructure
  to maintain forever, all bytes flow through it, 100k req/day cap vs
  DuckDB-Wasm's chatty range-request pattern.
- **AWS S3** — metered egress on a public dataset is an open-ended bill.

## 5. The dataset-family model

**One HF repo per dataset family, where a family is defined by a simple
test: one dataset card you can write honestly.** Files share a repo when
they share provenance, licence, cadence and caveats — so the card, the
citation and the "About" section are singular and true.

- `dft-vehicle-licensing` — VEH0105, VEH0132, future VEH tables. Same
  publisher, licence, quarterly cadence, same DVLA-stock caveats.
- `police-crime-london` — crimes, outcomes, stop & search. One family.
- `fsa-food-hygiene-london` — its own repo, even at one table.
- **Not** `dft-everything`: a publisher is not a family. DfT's road
  safety, traffic counts and spend tables are different topics that would
  force a grab-bag card. Group by *topic within publisher*.

This mirrors HF's semantic model (cards, viewer configs, download stats,
likes are all per-repo) and how the prior art does it (`datania/aemet` is
all AEMET weather data, not one file). Each table appears as a named
`config` in the repo's card, so the HF viewer renders each one properly.

Two rules keep grouping mistakes cheap, because grouping is a judgment
call and scope drifts (v1's working folder was named `highways-data-repo`
three topics ago):

1. **The pipeline's unit stays the individual table.** Fetching,
   validation and quarantine operate per-source (`dft_veh0105_...`); one
   table's format change never blocks its siblings. A source's
   `datapackage.json` simply declares `hf_repo` to say where its outputs
   land.
2. **Filenames stay globally unique across all repos** (publisher-prefixed
   source name, as in v1). Regrouping is then a one-field config change,
   never a collision hunt.

## 6. The source contract (carried over from v1, refined)

Everything that made v1's pipeline robust survives — these decisions were
storage-independent and correct:

- `sources/<name>/fetch.py` exposes `fetch(out_dir) -> list[Path]`;
  raises on failure; run_all isolates failures per source.
- Naming: `<publisher-code>_<table-id>_<short-description>`, flat
  `sources/` directory, filenames match the source name.
- **`datapackage.json` is the single source of truth** per source: title,
  description, homepage, licence, long-form citation, cadence, caveats,
  the derived parquet's Frictionless Table Schema (enforced at validation
  — schema/code mismatch quarantines the source), display fields for
  generated docs, and now `hf_repo`.
- Optional `validate(files)` — shape checks (magic bytes, columns,
  plausible row counts). Validation failure **quarantines**: nothing
  uploads, HF keeps serving the last good version, the source is flagged.
  Every source should have one; publishers change formats without warning.
- Optional `stats(files)` and `FETCHED_FROM` feed the catalog
  (best-effort; never quarantine).
- Scrape stable landing pages when direct URLs are unstable.
- **Byte-determinism** everywhere (pinned zip/gzip metadata, pinned
  pandas/pyarrow versions, `lineterminator`, stripped parquet metadata).
  In v1 this prevented spurious "changed" flags; now it additionally
  maximises chunk dedup and lets the uploader skip unchanged files. Same
  discipline, doubled payoff. Sanity check for new sources: run twice,
  second run must report unchanged.

**Artifact tiers per source** — the browser-first refinement:

1. **Raw file** — exactly as downloaded. Provenance and reproducibility.
2. **One full tidy parquet** — the complete dataset in long format. Full
   data, never pre-filtered; filtering is the app's job. Parquet only —
   no CSV/JSON siblings.
3. **Aggregate parquets, as needed** (`<name>_agg_<slug>.parquet`) —
   small rollups sized for dashboards (hundreds of KB, not hundreds of
   MB). Additive artifacts with declared schemas, added when a real
   dashboard needs one rather than speculatively. "What will a dashboard
   query?" is a standard question when adding a source.

## 7. Publishing, versioning and the serving contract

**Weekly run** (GitHub Actions, Monday cron + manual dispatch):

1. Fetch and validate every source (failures isolated and quarantined).
2. Upload each family's output folder to its HF repo via
   `huggingface_hub` — which hashes files and **skips unchanged ones
   automatically**, creating no commit when nothing changed. Repos are
   auto-created (`create_repo(exist_ok=True)`); adding a source never
   involves the HF UI.
3. First run of each month: tag every repo `archive-YYYY-MM`. Snapshots
   of *everything*, every month — dedup makes them ~free, which is why
   v1's size-tiered archive schedule (`archive.py`) no longer exists.
4. Clobber the GitHub backup release with current files.
5. Regenerate docs: pipeline README table and each family's HF dataset
   card (body + the `configs:`/`data_files` YAML that drives HF's viewer)
   from `datapackage.json` — one metadata source, two rendered faces.
   Commit `catalog.json`; its git history remains the audit log.
6. **Smoke-test the serving promise**: request published HF URLs with
   `Origin` and `Range` headers; assert `access-control-allow-origin`
   and HTTP 206. A provider behaviour change becomes a red run and an
   auto-filed issue — never a silently false README again.

Failure handling carries over from v1: retries with backoff, per-source
quarantine, `Source broken: <name>` issues from the second consecutive
failure, health recorded in the catalog.

**URLs consumers use:**

    latest:    https://huggingface.co/datasets/<ns>/<family>/resolve/main/<filename>
    snapshot:  https://huggingface.co/datasets/<ns>/<family>/resolve/archive-YYYY-MM/<filename>
    any commit: .../resolve/<sha>/<filename>
    duckdb:    hf://datasets/<ns>/<family>/<filename>

`catalog.json` records, per file: URLs, sha256, size, timestamps
(first_seen / last_changed / last_checked), stats, fetched_from, source
health, and the HF revision of the last sync. Apps hardcode one URL — the
catalog — and discover everything else from it. Snapshot URLs are served
with the same CORS as `main`, so dashboards can query *history* directly
(v1's archives never could).

URL stability is a soft promise, not a hard invariant: the sole developer
of the consuming apps has accepted that migrations mean updating them.
The catalog centralises URLs precisely so that stays a one-place change.

## 8. Platform risks, accepted and priced

| Risk | Exposure | Mitigation / exit |
|---|---|---|
| HF anonymous rate limit (~1,000 req/hr/IP) | Per *visitor* IP, so it scales with audience; DuckDB-Wasm is chatty. The number to watch. | Aggregates keep dashboard queries small; if real traffic arrives, that's the trigger to spend: R2 + domain, or HF PRO. |
| HF free-tier policy drift | Free public storage has tightened twice (storage limits 2024, rate limits 2025). | We're squarely their target use (public parquet, documented, tiny at <2 GB vs 1 TB). Backup release holds current data; HF repos are clonable with history; sources mostly re-fetchable. |
| Single platform holds deep history | Snapshots mainly protect against silent publisher revisions — valuable, not existential. | Clone-on-notice; publishers retain most raw history themselves. |
| Family grouping proves wrong | Card/URL churn when regrouping. | Per-table pipeline unit + globally unique filenames make it a config change. |
| GitHub Actions cron disabled after 60 days' inactivity | Pipeline silently stops. | Weekly catalog commit keeps the repo active while green (v1 behaviour, kept). |

Spending money is not failure. The free design is the right one *now*
(zero consumers, sole developer); the documented triggers for upgrading
are real dashboard traffic (→ R2 + own domain for owned, unmetered URLs)
or HF throttling in practice.

## 9. Prior art and what we add

- **Git scraping** (Simon Willison) and **Flat Data** (GitHub OCTO) —
  scheduled Actions + git history as changelog. We git-scrape *metadata*
  (the catalog); the data itself outgrew git.
- **Datadex / Datania / Filecoin Data Portal** (David Gasquez) — the
  proof that Actions → HF datasets → DuckDB runs for years at zero cost.
  We adopted from them: per-family HF repos, `huggingface_hub`'s
  hash-and-skip uploads, viewer `configs:` in generated cards, and the
  reassurance that HF's free tier sustains exactly this. The Filecoin
  portal's site (Astro + DuckDB at *build* time, pre-rendered charts) is
  a pattern the presentation repo should consider alongside runtime Wasm.
- **Our World in Data** — the institutional-scale version of
  pipeline + versioned data + browser viz + written context.
- **Official portals** (data.gov.uk, London Datastore) — catalogs of
  links to publisher files. No tidying, no schemas, no revision history,
  no browser-queryable formats.

What none of the lightweight prior art has — and this project's actual
identity: **declared schemas enforced at publish time, validation with
quarantine, a committed audit log, honest caveats, and revision tracking
of publishers who silently rewrite their numbers.** The pipeline is a
means; curation and context are the product.

## 10. Invariants

1. **No data in git.** Code, docs and metadata only. (The backup release
   is release assets, not git.)
2. **catalog.json is generated.** Change the generator, never the file;
   its committed history is the audit log — preserve that property.
3. **Schemas are declared and enforced.** Every derived parquet's columns
   live in its datapackage.json and are checked before publishing.
   Additive schema changes are fine; breaking ones need a transition.
4. **One source failing must never block the others.**
5. **The serving promise is tested, not assumed.** CORS + Range smoke
   test on real published URLs, every run. If we claim it in a README,
   CI proves it.
6. **Filenames are globally unique and stable.** URL migrations are
   permitted (sole consumer) but always deliberate, never accidental.
7. **Respect publishers.** Accurate User-Agent, weekly fetch cadence at
   most, licences honoured and displayed.
8. **Prefer boring code.** Python 3.12, stdlib + requests + pandas +
   pyarrow + huggingface_hub. New dependencies must be justified. Scripts
   stay runnable as plain `python <script>.py`. Sources are maintained
   rarely, by people (or Claudes) with no memory of writing them — write
   for them.

## 11. What v1 had that this design deletes

For the record, so nobody reinvents them:

- **`archive.py` and size-tiered snapshot releases** — a workaround for
  full-copy archive storage; dedup + tags made it pointless.
- **`archive_tier_override`, the snapshot map, immutable-archive rules** —
  existed only to serve the above.
- **The 1000-assets-per-release ceiling** and its batching concerns.
- **GitHub release URLs as the public consumer contract** — the false
  CORS promise. Releases demote to disaster backup.

What was *kept* is the larger list: the source contract, quarantine,
determinism, the catalog, the metadata-first philosophy, and the weekly
rhythm. The rebuild is a re-homing of the data, not a rethink of the
pipeline.
