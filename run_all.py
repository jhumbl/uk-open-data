"""Run every source's fetch.py, validate outputs, sync each family to its
Hugging Face dataset repo, update catalog.json.

Usage:
    python run_all.py [--out data] [--only <source_name>] [--no-hf]

Behaviour:
  * Each directory under sources/ containing a fetch.py is a source. Its
    datapackage.json must name the family repo its outputs land in
    (`hf_repo`), and families.json must have an entry for that family.
  * fetch(out_dir) is called for each; files it returns are then checked by
    the source's optional validate(files) hook. Validation failure means
    "the publisher probably changed something" — the files are quarantined
    (NOT uploaded), so the HF repo keeps serving the last known-good version.
  * A valid file whose sha256 differs from catalog.json (or is new) is
    "changed" — reported for humans/logs. Upload skipping is the hub's job:
    ALL valid files are handed to create_commit, which drops unchanged ones
    server-side and creates no commit when nothing changed.
  * After the per-source loop, each family with at least one successful
    source this run is synced: files + regenerated dataset card uploaded,
    `archive-YYYY-MM` tag ensured, revision recorded in the catalog. A
    family sync failure never blocks other families (invariant 4 applies at
    both levels).
  * Failures go to data/.failed as "name<TAB>consecutive_failures<TAB>reason"
    lines; the workflow fails the job on any failure AFTER the healthy
    sources have synced, and files a GitHub Issue once something has failed
    2+ runs in a row. Changed filenames go to data/.changed (one per line)
    for the log and the backup-release step.
  * Optional per-source hooks feed catalog.json: stats(files) returns
    per-filename computed stats stored under the entry's "stats" key, and a
    module-level FETCHED_FROM dict (filename -> resolved URL) is stored as
    "fetched_from". Both are best-effort: failures warn, missing hooks are
    fine.
  * Per-source health (status, last_success, consecutive_failures) is
    tracked in catalog.json under "sources"; per-family sync health under
    "families".
  * This script always exits 0 unless something unexpected breaks the
    orchestrator itself — per-source failure reporting is the workflow's job.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
import traceback
from pathlib import Path

from lib.common import (
    REPO_ROOT,
    hf_repo_url,
    hf_resolve_url,
    load_catalog,
    load_datapackage,
    load_families,
    save_catalog,
    sha256_file,
    utc_now_iso,
)

SOURCES_DIR = REPO_ROOT / "sources"
PIPELINE_REPO_URL = "https://github.com/jhumbl/uk-open-data"


def discover_sources(only: str | None) -> list[Path]:
    dirs = sorted(
        d for d in SOURCES_DIR.iterdir() if d.is_dir() and (d / "fetch.py").exists()
    )
    if only:
        dirs = [d for d in dirs if d.name == only]
        if not dirs:
            sys.exit(f"No source named '{only}' under sources/")
    return dirs


def load_module(source_dir: Path):
    spec = importlib.util.spec_from_file_location(
        f"sources.{source_dir.name}.fetch", source_dir / "fetch.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def record_failure(health_map: dict, name: str, kind: str, reason: str) -> str:
    """Update health in the catalog; return a one-line .failed summary."""
    health = health_map.setdefault(name, {})
    health["status"] = kind  # "fetch_failed" | "validation_failed" | "sync_failed"
    health["last_error"] = reason.strip().splitlines()[-1][:300]
    health["last_failure"] = utc_now_iso()
    health["consecutive_failures"] = health.get("consecutive_failures", 0) + 1
    return f"{name}\t{health['consecutive_failures']}\t[{kind}] {health['last_error']}"


def family_card(family: str, catalog: dict) -> str:
    """Build the family's dataset card from every member source that has
    published data (not just the sources run this time — `--only` must not
    shrink the card)."""
    from lib.cards import build_card

    published_sources = {e["source"] for e in catalog["datasets"].values()}
    packages = []
    for source_dir in sorted(SOURCES_DIR.iterdir()):
        if not (source_dir / "datapackage.json").exists():
            continue
        package = load_datapackage(source_dir)
        if package.get("hf_repo") == family and package["name"] in published_sources:
            packages.append(package)
    return build_card(family, load_families()[family], packages, PIPELINE_REPO_URL)


def sync_families(
    family_files: dict[str, list[Path]], catalog: dict, failed_lines: list[str]
) -> None:
    """Upload each family's files + card to HF, tag the month, record
    revisions and per-file URLs in the catalog. Failures are isolated
    per family."""
    from lib.hf_sync import ensure_monthly_tag, get_api, sync_family

    api = get_api()
    for family in sorted(family_files):
        files = family_files[family]
        print(f"[family {family}] syncing {len(files)} file(s) to HF")
        try:
            result = sync_family(api, family, files, family_card(family, catalog))
            tag = ensure_monthly_tag(api, family)
        except Exception:
            print(f"  SYNC FAILED — continuing with other families.\n{traceback.format_exc()}")
            failed_lines.append(
                record_failure(
                    catalog.setdefault("families", {}), family, "sync_failed",
                    traceback.format_exc(),
                )
            )
            continue

        health = catalog.setdefault("families", {}).setdefault(family, {})
        health.update(
            status="ok",
            repo_url=hf_repo_url(family),
            revision=result["revision"],
            last_tag=tag,
            last_synced=utc_now_iso(),
            consecutive_failures=0,
        )
        health.pop("last_error", None)

        for path in files:
            catalog["datasets"][path.name].update(
                hf_repo=family,
                download_url=hf_resolve_url(family, path.name),
            )
        print(
            f"  synced at revision {result['revision'][:12]} "
            f"({'new commit' if result['committed'] else 'no changes — no commit'}), "
            f"tag {tag}"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data", help="output directory for fetched files")
    parser.add_argument("--only", default=None, help="run a single source by name")
    parser.add_argument(
        "--no-hf", action="store_true",
        help="fetch/validate/catalog only; skip the Hugging Face sync",
    )
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    catalog = load_catalog()
    families_meta = load_families()
    changed: list[str] = []
    failed_lines: list[str] = []
    family_files: dict[str, list[Path]] = {}

    for source_dir in discover_sources(args.only):
        name = source_dir.name
        print(f"[{name}]")

        # 0. The source must name a family that families.json knows about —
        #    a config mistake, caught before we spend time fetching.
        try:
            family = load_datapackage(source_dir)["hf_repo"]
            if family not in families_meta:
                raise KeyError(
                    f"hf_repo {family!r} has no entry in families.json — "
                    "add its title/description there"
                )
        except Exception:
            print(f"  CONFIG BROKEN — continuing with other sources.\n{traceback.format_exc()}")
            failed_lines.append(
                record_failure(catalog.setdefault("sources", {}), name,
                               "config_failed", traceback.format_exc())
            )
            continue

        # 1. Fetch
        try:
            module = load_module(source_dir)
            files = module.fetch(out_dir)
        except Exception:
            print(f"  FETCH FAILED — continuing with other sources.\n{traceback.format_exc()}")
            failed_lines.append(
                record_failure(catalog.setdefault("sources", {}), name,
                               "fetch_failed", traceback.format_exc())
            )
            continue

        # 2. Validate (optional hook). Failure => quarantine: delete the
        #    files so they can't be uploaded; HF keeps serving last-good.
        validate = getattr(module, "validate", None)
        if validate is not None:
            try:
                validate(files)
            except Exception:
                print(
                    "  VALIDATION FAILED — source data may have changed format. "
                    "Quarantining output; HF keeps the previous good version.\n"
                    f"{traceback.format_exc()}"
                )
                failed_lines.append(
                    record_failure(catalog.setdefault("sources", {}), name,
                                   "validation_failed", traceback.format_exc())
                )
                for path in files:
                    path.unlink(missing_ok=True)
                continue

        # 3. Optional extras: stats(files) hook and FETCHED_FROM provenance.
        #    Stats are garnish — a stats failure must never quarantine a
        #    source, so it only warns.
        file_stats: dict[str, dict] = {}
        stats_hook = getattr(module, "stats", None)
        if stats_hook is not None:
            try:
                file_stats = stats_hook(files) or {}
            except Exception as exc:  # noqa: BLE001
                print(f"  WARNING: stats hook failed ({exc}) — publishing without stats.")
        fetched_from = getattr(module, "FETCHED_FROM", {})

        # 4. Record success + detect changes (informational: the hub does
        #    its own hash comparison at upload time).
        health = catalog.setdefault("sources", {}).setdefault(name, {})
        health.update(status="ok", last_success=utc_now_iso(), consecutive_failures=0)
        health.pop("last_error", None)

        for path in files:
            digest = sha256_file(path)
            entry = catalog["datasets"].setdefault(
                path.name, {"source": name, "first_seen": utc_now_iso()}
            )
            if entry.get("sha256") != digest:
                changed.append(path.name)
                entry["last_changed"] = utc_now_iso()
                print(f"  CHANGED: {path.name}")
            else:
                print(f"  unchanged: {path.name}")
            entry.update(
                sha256=digest,
                size_bytes=path.stat().st_size,
                last_checked=utc_now_iso(),
            )
            for key, value in (("stats", file_stats.get(path.name)),
                               ("fetched_from", fetched_from.get(path.name))):
                if value:
                    entry[key] = value
                else:
                    entry.pop(key, None)

        family_files.setdefault(family, []).extend(files)

        # 5. Orphan check: catalog entries this source owns but didn't
        #    produce this run. Warn only — NEVER prune: derived files are
        #    best-effort (a degraded tidy step must not retire the entry),
        #    and retiring a published filename is a deliberate, manual act
        #    (see CLAUDE.md invariant 6).
        produced = {path.name for path in files}
        for filename, entry in catalog["datasets"].items():
            if entry.get("source") == name and filename not in produced:
                print(
                    f"  NOTE: catalog lists {filename} for {name} but this run "
                    "did not produce it (derived file degraded, or retired?)"
                )

    # 6. Sync each family to HF (unless told not to).
    if args.no_hf:
        print("\n--no-hf: skipping Hugging Face sync; catalog URLs not updated.")
    elif family_files:
        sync_families(family_files, catalog, failed_lines)

    (out_dir / ".changed").write_text(
        "\n".join(changed) + ("\n" if changed else ""), encoding="utf-8"
    )
    (out_dir / ".failed").write_text(
        "\n".join(failed_lines) + ("\n" if failed_lines else ""), encoding="utf-8"
    )
    save_catalog(catalog)

    print(f"\nDone: {len(changed)} file(s) changed, {len(failed_lines)} failure(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
