# Data Pipeline

A reproducible, incremental pipeline that scrapes [UFCStats](http://ufcstats.com)
and produces the cleaned, analysis-ready fight and fighter datasets.

Rerun it whenever new UFC events have happened — the scrapers are
cache-first and only fetch what's new.

The pipeline has two parts:

- **Core data pipeline (`00`–`02`)** — scrape → clean. General-purpose;
  run by `run_pipeline.R`. This is what you rerun to refresh the data.
- **Research-question steps (`03`, `04`)** — build model datasets and fit
  the model for the current report. These are specific to that research
  question, so they are **not** part of `run_pipeline.R`; run them
  directly when working on that analysis.

## Requirements

- **R** with the project packages (rvest, dplyr/tidyr/readr/stringr/purrr,
  lubridate; brms for the model-fitting step).
- **A headless browser + `chromote`.** ufcstats.com serves a JavaScript
  anti-bot challenge to plain HTTP clients, so the scrapers (stages 00 and
  01) drive a headless **Chromium-based browser** via the `chromote`
  package to run the challenge and read the rendered page.
  - Install: `install.packages("chromote")`.
  - Needs Chrome or Edge installed. The scrapers auto-detect common
    Chrome/Edge paths; to force a specific one, set the `CHROMOTE_CHROME`
    environment variable to the browser executable.
- The non-scraping stages (`02`, `03`) need no browser — they only read
  local files, so `--no-scrape` runs work without chromote.

## Quick start

Refresh the data (scrape + clean), from the project root:

```r
source("scripts/run_pipeline.R")
```

Or from a shell:

```sh
Rscript scripts/run_pipeline.R               # scrape + clean
Rscript scripts/run_pipeline.R --no-scrape   # rebuild clean data from existing raw CSVs
```

To build the report's model datasets and fit the model (research-question
specific — run after the data pipeline, not part of it):

```r
source("scripts/03_make_model_data.R")
source("scripts/04_fit_models.R")   # slow: compiles Stan, runs MCMC
```

## Stages

Each stage reads the previous stage's output and writes its own, so they
can also be run individually in order.

**Core data pipeline** (run by `run_pipeline.R`):

| Script | Does | Output |
|---|---|---|
| `_helpers.R` | Shared scrape/fetch/manifest/parse utilities. Sourced by the scrapers; not run directly. | — |
| `00_scrape_fights.R` | Discover events → fetch/parse event pages → fetch/parse fight pages → join event metadata. | `data/raw/fight_data_raw_enriched.csv` |
| `01_scrape_fighters.R` | Scrape the A–Z fighter directory for physical attributes (height, weight, reach, stance, DOB). Independent of the fight scrape. | `data/raw/fighters_data_raw.csv` |
| `02_clean_fight_data.R` | Standardize fields, parse clocks to seconds, split each fight into two fighter-centric rows, derive features. | `data/clean/fight_data.csv` |
| `run_pipeline.R` | Runs the core pipeline (00–02) end-to-end. | — |

**Research-question steps** (run directly, not via `run_pipeline.R`):

| Script | Does | Output |
|---|---|---|
| `03_make_model_data.R` | Build the two model datasets the current report uses (striking accuracy + win-probability differentials). | `data/model/striking_df.{rds,csv}`, `data/model/win_perf_diffs_df.{rds,csv}` |
| `04_fit_models.R` | Fit + cache the hierarchical Bayesian striking-accuracy model (prior-only and full posterior). **Slow** (compiles Stan, runs MCMC). | `models/fits/fit_prior_acc.rds`, `models/fits/fit_acc_model.rds` |

## How the scrapers stay fast (incremental caching)

Both scrapers are **cache-first and idempotent**:

- Raw HTML is cached under `cache/` (events, fights, fighters).
- A per-page **manifest** in `data/raw/*_manifest.csv` records each page's
  URL, cache path, and fetch/parse status.
- On a rerun, only pages that are **missing or previously failed** are
  fetched, and only pages that are **new or were just refetched** are
  re-parsed.

To force a fully fresh scrape, delete the relevant `cache/` subfolder (or
set `STALE_AFTER_DAYS` in the scraper to a finite number of days).

Because pages are loaded through one headless-browser session (to clear
the JS challenge), fetching is **sequential**, with randomized delays
between requests and retry-with-backoff when the challenge doesn't clear.
The incremental cache is what keeps reruns quick — a routine refresh only
loads the handful of pages from new events.

## Notes

- The fighter table (`01_`) is built independently and is **not** joined
  into the fight data by default; it's available for analyses that need
  physical attributes (height/reach/stance/age).
- `03_make_model_data.R` standardizes all four performance differentials
  (significant strikes, knockdowns, takedowns, control time) with
  `scale()`, so RQ1 coefficients are per-1-SD.
- The full model fit can exceed GitHub's file-size limit; it is also
  hosted externally (see `models/fits/README.md`).
