# Scripts Pipeline

The canonical pipeline for this project is now:

1. `00_build_raw_enriched_efficient.R`
2. `06_data_cleaning.R`

The older `01_` through `05_` scripts are legacy pipeline pieces kept for reference, but the intended workflow going forward is `00_` followed by `06_`.

## `00_build_raw_enriched_efficient.R`

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


## `06_data_cleaning.R`

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

## Recommended Run Order

From the project root:

```r
source("scripts/00_build_raw_enriched_efficient.R")
source("scripts/06_data_cleaning.R")
```

Or run them individually in RStudio.
