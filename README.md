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

An incremental pipeline scrapes [UFCStats](http://ufcstats.com) and
produces the cleaned fight and fighter datasets. Rerun it whenever new UFC
events occur — the scrapers are cache-first and only fetch what's new.

```r
# From the project root — refresh the data (scrape + clean):
source("scripts/run_pipeline.R")
```

```sh
Rscript scripts/run_pipeline.R               # scrape + clean
Rscript scripts/run_pipeline.R --no-scrape   # rebuild clean data from existing raw CSVs
```

The research-question-specific steps (building model datasets and fitting
the model) live under `scripts/striking_accuracy/` and are run directly,
separate from the core data pipeline.

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
├── scripts/                    # Pipeline + analysis scripts (see scripts/README.md)
│   ├── _helpers.R              # Shared scrape/fetch/manifest/parse utilities
│   ├── 00_scrape_fights.R      # Core pipeline: events → fights → enriched raw
│   ├── 01_scrape_fighters.R    # Core pipeline: fighter metadata
│   ├── 02_clean_fight_data.R   # Core pipeline: clean + reshape
│   ├── run_pipeline.R          # Runs the core data pipeline (00–02)
│   └── striking_accuracy/      # Research-question scripts (run separately)
│       ├── make_data.R         #   Build striking + win-differential datasets
│       └── fit_model.R         #   Fit + cache the Bayesian accuracy model
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

