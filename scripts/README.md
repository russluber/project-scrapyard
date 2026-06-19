# Data Pipeline

A reproducible, incremental pipeline that scrapes [UFCStats](http://ufcstats.com),
cleans the data, builds model-ready datasets, and fits the Bayesian
striking-accuracy model used in the final report.

Rerun it whenever new UFC events have happened — the scrapers are
cache-first and only fetch what's new.

## Requirements

- **R** with the project packages (rvest, dplyr/tidyr/readr/stringr/purrr,
  lubridate, brms for the model stage).
- **A headless browser + `chromote`.** ufcstats.com serves a JavaScript
  anti-bot challenge to plain HTTP clients, so the scrapers (stages 00 and
  01) drive a headless **Chromium-based browser** via the `chromote`
  package to run the challenge and read the rendered page.
  - Install: `install.packages("chromote")`.
  - Needs Chrome or Edge installed. The scrapers auto-detect common
    Chrome/Edge paths; to force a specific one, set the `CHROMOTE_CHROME`
    environment variable to the browser executable.
- The cleaning and model-data stages (02, 03) need no browser — they only
  read local files, so `--no-scrape` runs work without chromote.

## Quick start

From the project root:

```r
# Refresh all data (does NOT refit the slow Bayesian model):
source("scripts/run_pipeline.R")

# Refresh data AND refit the model:
RUN_FIT <- TRUE; source("scripts/run_pipeline.R")
```

Or from a shell:

```sh
Rscript scripts/run_pipeline.R          # data only
Rscript scripts/run_pipeline.R --fit    # data + model fit
Rscript scripts/run_pipeline.R --no-scrape   # rebuild downstream data from existing raw CSVs
```

## Stages

The pipeline is five numbered scripts plus shared helpers and a runner.
Each stage reads the previous stage's output and writes its own, so they
can also be run individually in order.

| Script | Does | Output |
|---|---|---|
| `_helpers.R` | Shared scrape/fetch/manifest/parse utilities. Sourced by the scrapers; not run directly. | — |
| `00_scrape_fights.R` | Discover events → fetch/parse event pages → fetch/parse fight pages → join event metadata. | `data/raw/fight_data_raw_enriched.csv` |
| `01_scrape_fighters.R` | Scrape the A–Z fighter directory for physical attributes (height, weight, reach, stance, DOB). Independent of the fight scrape. | `data/raw/fighters_data_raw.csv` |
| `02_clean_fight_data.R` | Standardize fields, parse clocks to seconds, split each fight into two fighter-centric rows, derive features. | `data/clean/fight_data.csv` |
| `03_make_model_data.R` | Build the two model datasets the report uses (striking accuracy + win-probability differentials). | `data/model/striking_df.{rds,csv}`, `data/model/win_perf_diffs_df.{rds,csv}` |
| `04_fit_models.R` | Fit + cache the hierarchical Bayesian striking-accuracy model (prior-only and full posterior). **Slow** (compiles Stan, runs MCMC). | `models/fits/fit_prior_acc.rds`, `models/fits/fit_acc_model.rds` |
| `run_pipeline.R` | Runs the stages above end-to-end with options. | — |

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
