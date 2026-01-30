# Copilot / AI agent instructions — mf-data-pipeline

Purpose: help an AI coding agent be immediately productive in this repository.

- **Repo overview:** this is a small, script-driven ETL pipeline that fetches mutual-fund scheme metadata and NAV history, stores per-scheme CSVs in `data/nav_history/`, produces year-wise CSVs in `data/nav_year/`, and assembles a scheme index at `data/scheme_index.csv`.

- **Key scripts and data flow:**
  - `scripts/fetch_scheme_codes.py` — downloads the raw scheme list and writes `data/scheme_codes.csv` (columns: `SchemeCode`, `SchemeName`).
  - `scripts/fetch_scheme_categories.py` — enriches codes using `https://api.mfapi.in/mf/<code>` and writes `data/scheme_categories.csv`. It uses an adaptive chunking strategy (CHUNK_SIZE, REQUEST_DELAY) based on pending count.
  - `scripts/fetch_nav_history.py` — iterates `data/scheme_codes.csv`, fetches NAV history per scheme, and appends to `data/nav_history/<SchemeCode>.csv` with header `Date,NAV`. Uses a fast tail-reader to skip if already up-to-date (today) and sleeps only when it writes data.
  - `scripts/export_nav_year.py` — reads all `data/nav_history/*.csv` and writes `data/nav_year/nav_year_<year>.csv` files (one file per year). It preserves previously-exported last dates and appends only new rows.
  - `scripts/export_nav_history_all.py` — concatenates per-scheme NAV files into `data/nav_history_all.csv` (full re-write each run).
  - `scripts/build_nav_sqlite.py` — builds `data/mf_nav.db` (table `nav_history`) using `INSERT OR IGNORE` and a primary key (SchemeCode,Date).
  - `scripts/merge_scheme_metadata.py` — combines `scheme_codes.csv` and `scheme_categories.csv` into `data/scheme_index.csv` (columns listed in script).

- **Important patterns & conventions (project-specific):**
  - CSV files are UTF-8 encoded and opened with `newline=""` for cross-platform consistency.
  - Scripts are idempotent where possible: they append only new NAV rows, skip schemes updated today, and re-save chunked category data after each chunk.
  - Network calls use `requests.Session()` with a `User-Agent` header to reduce server-side blocking.
  - Timing controls are explicit: `REQUEST_DELAY`, `BASE_CHUNK_DELAY` and chunk-size heuristics are used to avoid overloading public APIs — do not remove these without confirming rate limits.
  - Concurrency: `fetch_nav_history.py` uses `ThreadPoolExecutor` with a configurable `MAX_WORKERS` constant — reduce this for local testing and increase cautiously for production runs.
  - Date formatting: per-scheme NAV files in `data/nav_history/` use ISO `YYYY-MM-DD` (written by `fetch_nav_history.py`); `export_nav_year.py` expects ISO dates. `fetch_scheme_codes.py` writes AMFI's date string as-is — do not assume it matches the history-format.
  - File layout is assumed relative to repo root; run scripts from repository root so paths like `data/...` resolve correctly.

- **External dependencies:**
  - Python 3.8+ (scripts use standard library + `requests`). Install with `pip install requests` if missing.

- **How to run common developer flows (examples):**
  - Refresh codes and categories (safe to run in sequence):
    ```bash
    python scripts/fetch_scheme_codes.py
    python scripts/fetch_scheme_categories.py
    ```
  - Update NAV history for all schemes (can take long):
    ```bash
    python scripts/fetch_nav_history.py
    ```
  - Export year-wise CSVs, combined history, and rebuild DB/master:
    ```bash
    python scripts/export_nav_year.py
    python scripts/export_nav_history_all.py
    python scripts/build_nav_sqlite.py
    python scripts/merge_scheme_metadata.py
    ```

- **What an AI agent can safely change vs what to ask about:**
  - Safe: small refactors, improving logging/printing, adding type hints, or wrapping repeated code into helpers (preserve the existing CSV headers and file paths).
  - Ask before: changing sleep/delay values, switching APIs, or switching file layout (these are deliberate to avoid rate limits and preserve backward compatibility with downstream consumers).

- **Where to look for examples of patterns:**
  - `scripts/fetch_nav_history.py` — fast tail-reader implementation and append-only NAV updates.
  - `scripts/fetch_scheme_categories.py` — chunking logic and conservative request pacing.
  - `scripts/export_nav_year.py` — transformation from per-scheme files to year-wise CSVs.

- **Debugging tips & testing suggestions:**
  - Reproduce locally from repo root (relative paths assume you run from repo root).
  - Inspect `data/` files for partial results — scripts save incrementally after chunks or appends.
  - For safe local testing, reduce `CHUNK_SIZE` / `MAX_WORKERS` and increase `REQUEST_DELAY`.
  - Mock `https://api.mfapi.in/mf/<code>` and the AMFI `NAVAll.txt` in unit tests; add small test fixtures under `tests/` and a `requirements.txt` if you introduce new deps.
  - Network errors are printed and skipped by design — rerun the scripts to retry transient failures.

If any part of this is unclear or you want more detail (e.g., examples of CSV rows, exact column lists, or unit-test suggestions), tell me which area to expand.
---
> Note: the consolidated guidance and examples are above. If you want a more detailed walkthrough (example CSV rows, code snippets, or test scaffolds), tell me which area to expand.
