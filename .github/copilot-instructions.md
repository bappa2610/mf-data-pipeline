# Copilot / AI agent instructions — mf-data-pipeline

Purpose: help an AI coding agent be immediately productive in this repository.

- **Repo overview:** this is a small, script-driven ETL pipeline that fetches mutual-fund scheme metadata and NAV history, stores per-scheme CSVs in data/nav_history/, produces year-wise wide CSVs in data/nav_wide/, and assembles a master index at data/mf_master.csv.

- **Key scripts and data flow:**
  - `scripts/fetch_scheme_codes.py` — downloads the raw scheme list and writes `data/scheme_codes.csv` (columns: `SchemeCode`, `SchemeName`).
  - `scripts/fetch_scheme_categories.py` — enriches codes using `https://api.mfapi.in/mf/<code>` and writes `data/scheme_categories.csv`. It uses an adaptive chunking strategy (CHUNK_SIZE, REQUEST_DELAY) based on pending count.
  - `scripts/fetch_nav_history.py` — iterates `data/scheme_codes.csv`, fetches NAV history per scheme, and appends to `data/nav_history/<SchemeCode>.csv` with header `Date,NAV`. Uses a fast tail-reader to skip if already up-to-date (today) and sleeps only when it writes data.
  - `scripts/export_nav_wide.py` — reads all `data/nav_history/*.csv` and writes `data/nav_wide/nav_wide_<year>.csv` files (one file per year). It preserves previously-exported last dates and appends only new rows.
  - `scripts/build_master_file.py` — combines `scheme_codes.csv` and `scheme_categories.csv` into `data/mf_master.csv` (columns listed in script).

- **Important patterns & conventions (project-specific):**
  - CSV files are UTF-8 encoded and opened with `newline=""` for cross-platform consistency.
  - Scripts are idempotent where possible: they append only new NAV rows, skip schemes updated today, and re-save chunked category data after each chunk.
  - Network calls use `requests.Session()` with a `User-Agent` header to reduce server-side blocking.
  - Timing controls are explicit: `REQUEST_DELAY`, `BASE_CHUNK_DELAY` and chunk-size heuristics are used to avoid overloading public APIs — do not remove these without confirming rate limits.
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
  - Export wide CSVs and rebuild master:
    ```bash
    python scripts/export_nav_wide.py
    python scripts/build_master_file.py
    ```

- **What an AI agent can safely change vs what to ask about:**
  - Safe: small refactors, improving logging/printing, adding type hints, or wrapping repeated code into helpers (preserve the existing CSV headers and file paths).
  - Ask before: changing sleep/delay values, switching APIs, or switching file layout (these are deliberate to avoid rate limits and preserve backward compatibility with downstream consumers).

- **Where to look for examples of patterns:**
  - `scripts/fetch_nav_history.py` — fast tail-reader implementation and append-only NAV updates.
  - `scripts/fetch_scheme_categories.py` — chunking logic and conservative request pacing.
  - `scripts/export_nav_wide.py` — transformation from per-scheme files to wide-year CSVs.

- **Debugging tips:**
  - Reproduce locally from repo root. If a script exits quickly, check `data/` files for partial results (scripts save incrementally).
  - Network errors are printed and skipped; rerun the script to retry failed items.

If any part of this is unclear or you want more detail (e.g., examples of CSV rows, exact column lists, or unit-test suggestions), tell me which area to expand.
# Copilot / AI Agent Instructions for mf-data-pipeline

**Purpose:** concise guidance to quickly make code changes, add features, or fix bugs in this repository.

**Big Picture**
- **What:** Lightweight Python data pipeline that fetches mutual fund scheme lists, scheme categories, NAV histories, and builds a master CSV (`data/mf_master.csv`).
- **Where:** Source scripts are in `scripts/` and persistent data lives in `data/` (`scheme_codes.csv`, `scheme_categories.csv`, `nav_history/`, `nav_wide/`, `mf_master.csv`).

**Primary data flow (scripts order)**
- `python scripts/fetch_scheme_codes.py` → produces `data/scheme_codes.csv`
- `python scripts/fetch_scheme_categories.py` → enriches `data/scheme_categories.csv` (chunked requests, adaptive delays)
- `python scripts/fetch_nav_history.py` → appends per-scheme files into `data/nav_history/`
- `python scripts/export_nav_wide.py` → (reads nav_history, writes `nav_wide/` exports)
- `python scripts/build_master_file.py` → combines `scheme_codes.csv` + `scheme_categories.csv` into `data/mf_master.csv`

**Key project conventions & patterns**
- Uses only the Python stdlib CSV module and `requests` for HTTP; scripts are single-file CLIs (no CLI framework).
- Data-first approach: scripts create `data/` and `data/nav_history/` directories when needed (no extra bootstrap required).
- Rate-limiting strategy: `fetch_scheme_categories.py` uses adaptive `CHUNK_SIZE` and `REQUEST_DELAY` depending on pending count. Preserve this logic when modifying request loops.
- Files under `data/nav_history/` are appended with newest rows; `fetch_nav_history.py` optimizes by reading the last bytes to skip already-stored dates—keep that pattern when changing write logic.
- Error handling is pragmatic: print and continue (see `scripts/fetch_scheme_categories.py` and `scripts/fetch_nav_history.py`). Prefer preserving idempotence over strict exception propagation.

**Integration points & external dependencies**
- External APIs: `https://api.mfapi.in/mf/<scheme_code>` and `https://www.amfiindia.com/spages/NAVAll.txt` (network access required). Mock or stub these in unit tests.
- Python dependency: `requests` (assume Python 3.8+). No requirements file currently—add `requirements.txt` if you add dependencies.

**Developer workflows & common commands**
- Run a single script: `python scripts/fetch_scheme_codes.py`
- Full refresh (order): run scripts in the order listed above.
- Quick debug: run `fetch_nav_history.py` for a single scheme by modifying the `schemes` list slice in-place or using a small wrapper that passes a limited `CODES_FILE`.

**When changing network logic**
- Keep delays and chunking configurable and non-blocking for local testing. If adding retries, respect existing timeouts and session usage (`requests.Session()`).

**Files to inspect for examples**
- `scripts/build_master_file.py` — shows CSV merge pattern and output headers for `mf_master.csv`.
- `scripts/fetch_scheme_categories.py` — shows chunking, request/session headers, CSV write-after-chunk.
- `scripts/fetch_nav_history.py` — shows efficient file tail read, per-scheme CSV layout (`Date,NAV`).

**Do not assume**
- There are no automated tests or CI workflows in the repo—changes should be verified locally.
- No `requirements.txt` or virtualenv setup file is present; installing `requests` may be necessary: `pip install requests`.

If anything here is unclear or you want this expanded (examples for tests, CI, or a `requirements.txt`), tell me which area to improve. ✨
