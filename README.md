# Project Scrapyard

In this project, I investigate two central questions about fighter performance in the [UFC](http://www.ufcstats.com/statistics/events/completed):

1. Which in-fight performance differential metric is the most predictive of winning?

2. What is each fighter's *latent* probability of landing a significant strike, after accounting for gender, weight class, and fight-to-fight randomness?

## Figure Gallery

<p align="center">
  <div style="display: flex; flex-wrap: wrap; justify-content: center; gap: 20px; max-width: 1200px;">
    <img src="figs/n_fights_per_fighter_hist.png" width="300" />
    <img src="figs/victory_bars.png" width="300" />
    <img src="figs/top10_ci_plot.png" width="300" />
    <img src="figs/latent_acc_dist.png" width="300" />
  </div>
</p>


## Data Pipeline

A single, incremental pipeline scrapes [UFCStats](http://ufcstats.com),
cleans the data, builds the model datasets, and fits the Bayesian model.
Rerun it whenever new UFC events occur — the scrapers are cache-first and
only fetch what's new.

```r
# From the project root:
source("scripts/run_pipeline.R")            # refresh all data
RUN_FIT <- TRUE; source("scripts/run_pipeline.R")   # data + refit model
```

```sh
Rscript scripts/run_pipeline.R          # data only
Rscript scripts/run_pipeline.R --fit    # data + model fit
```

See [`scripts/README.md`](scripts/README.md) for full pipeline docs.

## Project Structure

```
root/
├── cache/                      # Raw scraped HTML (events/fights/fighters; git-ignored)
│
├── data/                       # All datasets
│   ├── raw/                    # CSVs straight from scraping + scrape manifests
│   ├── clean/                  # Cleaned, analysis-ready fight data
│   └── model/                  # Model-ready datasets
│
├── scripts/                    # The data pipeline (see scripts/README.md)
│   ├── _helpers.R              # Shared scrape/fetch/manifest/parse utilities
│   ├── 00_scrape_fights.R      # Events → fights → enriched raw fight data
│   ├── 01_scrape_fighters.R    # Fighter metadata (height/reach/stance/dob)
│   ├── 02_clean_fight_data.R   # Clean + reshape to one row per fighter-fight
│   ├── 03_make_model_data.R    # Build striking + win-differential datasets
│   ├── 04_fit_models.R         # Fit + cache the Bayesian accuracy model
│   └── run_pipeline.R          # End-to-end runner
│
├── eda/                        # Exploratory data analysis
│   └── eda.Rmd
│
├── figs/                       # Generated figures
│
├── models/                     # Model code + saved fits
│   └── fits/                   # Saved brms/Stan fits (.rds)
│
├── reports/                    # Rendered reports
│   ├── midterm/                # Midterm checkpoint report
│   └── final/                  # Final report (the central document)
│
├── .gitignore
└── README.md                   # Project overview (this file)
```

