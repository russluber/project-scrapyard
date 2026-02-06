# Project Scrapyard

In this project, I investigate two central questions about fighter performance in the [UFC](http://www.ufcstats.com/statistics/events/completed):

1. Which in-fight performance differential metric is the most predictive of winning?

2. What is each fighter's *latent* probability of landing a significant strike, after accounting for gender, weight class, and fight-to-fight randomness?


## Project Structure

```
root/
├── cache/                      # Raw HTML files from scraping (ignored by Git)
│
├── data/                       # All datasets
│   ├── raw/                    # Untouched CSVs directly from scraping
│   ├── clean/                  # Outputs from scripts/data_cleaning.R
│   └── model/                  # Modeling-ready data
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
├── analysis/                   
│   └── eda.Rmd                 # Exploratory data analysis
│
├── figs/                       # Generated figures
│
├── models/fits                 # Saved model fits (.rds) from brms/Stan
│   ├── fit_prior_acc.rds
│   └── fit_acc_model.rds
│
├── reports/                    # Rendered outputs
│   ├── midterm/                # Draft report for midterm checkpoint
│   └── final/                  # Final report PDF
│
├── .gitignore                  # Git ignore rules
├── README.md                   # Project overview (this file)
└── LICENSE / requirements.txt  # Optional metadata
```

