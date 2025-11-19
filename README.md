# Project Scrapyard

Analyzing [UFC](http://www.ufcstats.com/statistics/events/completed) fight data.

## Project Structure

```
root/
├── cache/                      # Raw HTML files from scraping (ignored by Git)
│
├── data/                       # All datasets
│   ├── raw/                    # Untouched CSVs directly from scraping
│   ├── clean/                  # Outputs from scripts/data_cleaning.R
│   └── model/                  # Modeling-ready data (e.g. striking_df.rds)
│
├── scripts/                    # Data scraping and cleaning pipeline
│   ├── 01_fetch_event_cards.R
│   ├── 02_parse_event_cards.R
│   ├── 03_fetch_fight_pages.R
│   ├── 04_parse_fight_pages.R
│   ├── 05_join_event_metadata.R
│   ├── 06_data_cleaning.R
│   ├── 07_fetch_fighter_pages.R
│   └── 08_parse_fighter_pages.R
│   └── 09_make_striking_data.R
│
├── R/                          # Helper R scripts for modeling & visualization
│   ├── prep_accuracy_data.R    # Builds y/n + factor IDs for modeling
│   └── theme_plots.R           # (Optional) Custom ggplot theme for consistent figures
│
├── analysis/                   # EDA and Bayesian modeling notebooks (Quarto)
│   ├── eda.qmd                 # Exploratory data analysis
│   ├── model_accuracy_stage1.qmd  # Stage 1: priors + prior predictive checks
│   ├── model_accuracy_stage2.qmd  # Stage 2: model fit + posterior predictive checks
│   ├── model_accuracy_stage3.qmd  # Stage 3: LOO, sensitivity, summaries
│   └── model_accuracy_final.qmd   # Polished 10-page final report (render to PDF)
│
├── figs/                       # Generated figures
│   ├── eda/
│   ├── model_stage1/
│   ├── model_stage2/
│   ├── model_stage3/
│   └── final/                  # Curated figures used in final report/presentation
│
├── models/                     # Saved model fits (.rds) from brms/Stan (ignored by Git)
│
├── reports/                    # Rendered outputs
│   ├── midterm/                # Draft report for midterm checkpoint
│   └── final/                  # Final report PDF + slides
│
├── docs/                       # Presentation slides and related assets
│   └── slides/
│
├── .gitignore                  # Git ignore rules
├── README.md                   # Project overview (this file)
└── LICENSE / requirements.txt   # Optional metadata
```

