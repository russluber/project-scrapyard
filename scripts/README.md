# Scripts Pipeline

The canonical scripts pipeline for this project is now:

1. `00_build_fight_data_raw_enriched.R`
2. `01_clean_fight_data.R`
3. `02_build_fighters_data_raw.R`

The older `01_` through `05_` scripts are legacy pipeline pieces kept for reference. Going forward, the intended fight-data workflow is `00_` followed by `01_`. The fighter metadata workflow is handled separately by `02_`.

## `00_build_fight_data_raw_enriched.R`

This script replaces the practical role of the old `01_` through `05_` sequence.

What it does:

- Fetches the UFCStats completed-events index
- Discovers all event card URLs and event IDs
- Updates event and fight manifests in `data/raw/`
- Fetches only missing or failed event pages into `cache/events/`
- Parses event pages into:
  - `data/raw/events_manifest.csv`
  - `data/raw/event_cards_parsed.csv`
  - `data/raw/event_fight_map.csv`
- Builds the fight queue directly from parsed event pages
- Fetches only missing or failed fight pages into `cache/fights/`
- Parses fight pages into `data/raw/fight_data_raw.csv`
- Joins fight rows to event metadata and writes `data/raw/fight_data_raw_enriched.csv`

## `01_clean_fight_data.R`

This script starts from `data/raw/fight_data_raw_enriched.csv` and converts the raw fight-level scrape into the cleaned analysis dataset.

What it does:

- Reads the canonical enriched raw fight data from `data/raw/`
- Standardizes dates, factor fields, weight classes, and method fields
- Converts end-of-fight and control-time clocks into usable numeric forms
- Splits the wide fight-level row into two fighter-centric rows:
  - one from fighter 1's perspective
  - one from fighter 2's perspective
- Derives analysis features such as:
  - volume strikes
  - strikes avoided
  - takedowns stuffed
  - control time in seconds
- Writes the cleaned output to `data/clean/fight_data.csv`

## `02_build_fighters_data_raw.R`

This script replaces the practical role of the old `07_fetch_fighter_pages.R` and `08_parse_fighter_pages.R` sequence.

It is independent from the fight-data pipeline above. It does not affect `data/clean/fight_data.csv` unless you explicitly join fighter metadata into the fight data later.

What it does:

- Scans the UFCStats A-Z fighter directory
- Discovers fighter profile URLs and fighter IDs
- Updates `data/raw/fighters_manifest.csv`
- Fetches only missing or failed fighter pages into `cache/fighters/`
- Parses only new or refetched fighter pages
- Writes fighter profile metadata to `data/raw/fighters_data_raw.csv`
- Logs parse issues to `data/raw/parse_errors_fighters.csv`

What kind of data it builds:

- fighter ID
- fighter profile URL
- display name
- height
- weight
- reach
- stance
- date of birth

## Recommended Run Order

For the canonical fight-data pipeline, from the project root:

```r
source("scripts/00_build_fight_data_raw_enriched.R")
source("scripts/01_clean_fight_data.R")
```

For fighter metadata only:

```r
source("scripts/02_build_fighters_data_raw.R")
```

Run these scripts individually in RStudio or in your usual R workflow.
