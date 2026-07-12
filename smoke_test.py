"""Smoke-test the serving promise (DESIGN.md invariant 5).

The primary consumer is a browser: DuckDB-Wasm issues cross-origin Range
requests, so every published URL must return `access-control-allow-origin`
and HTTP 206 partial content — on every redirect hop, because the browser
enforces CORS per hop. v1 of this project promised exactly this and it was
false; this test exists so that can never happen silently again. CI runs it
after every sync; a provider behaviour change becomes a red run.

Usage:
    python smoke_test.py

Checks every `download_url` in catalog.json (latest, i.e. resolve/main),
plus one file per family at its latest monthly snapshot tag — snapshot URLs
carry the same promise (dashboards can query history). Exits non-zero on
any failure.
"""

from __future__ import annotations

import sys

import requests

from lib.common import USER_AGENT, _with_retries, hf_resolve_url, load_catalog

# Any web origin will do: HF answers `access-control-allow-origin: *`.
ORIGIN = "https://example.org"
RANGE_BYTES = 1024


def check_url(url: str) -> list[str]:
    """Fetch url as a browser would; return a list of problems (empty = ok)."""

    def attempt():
        return requests.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Origin": ORIGIN,
                "Range": f"bytes=0-{RANGE_BYTES - 1}",
            },
            timeout=60,
            allow_redirects=True,
        )

    resp = _with_retries(url, attempt)
    problems = []
    for hop in [*resp.history, resp]:
        if "access-control-allow-origin" not in hop.headers:
            problems.append(
                f"hop {hop.url} ({hop.status_code}) has no "
                "access-control-allow-origin header — browsers are locked out"
            )
    if resp.status_code != 206:
        problems.append(
            f"final status {resp.status_code}, expected 206 — Range requests "
            "not honoured; DuckDB-Wasm would have to download whole files"
        )
    if "content-range" not in resp.headers:
        problems.append("no content-range header on the 206 response")
    if len(resp.content) > RANGE_BYTES:
        problems.append(
            f"asked for {RANGE_BYTES} bytes, got {len(resp.content)} — "
            "Range header ignored"
        )
    return problems


def main() -> int:
    catalog = load_catalog()

    urls: list[str] = []
    for filename, entry in sorted(catalog["datasets"].items()):
        if "download_url" in entry:
            urls.append(entry["download_url"])

    # One snapshot URL per family: same promise, different revision.
    for family, health in sorted(catalog.get("families", {}).items()):
        tag = health.get("last_tag")
        first_file = next(
            (
                name
                for name, entry in sorted(catalog["datasets"].items())
                if entry.get("hf_repo") == family and "download_url" in entry
            ),
            None,
        )
        if tag and first_file:
            urls.append(hf_resolve_url(family, first_file, tag))

    if not urls:
        sys.exit("smoke_test.py: catalog.json has no published URLs to test")

    failures = 0
    for url in urls:
        problems = check_url(url)
        if problems:
            failures += 1
            print(f"FAIL {url}")
            for problem in problems:
                print(f"     {problem}")
        else:
            print(f"ok   {url}")

    if failures:
        print(f"\n{failures}/{len(urls)} URL(s) failed the serving contract.")
        return 1
    print(f"\nAll {len(urls)} URL(s) honour CORS + Range. The promise holds.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
