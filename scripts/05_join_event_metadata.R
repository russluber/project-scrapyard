# join_event_metadata_v3.R
suppressPackageStartupMessages({
  library(tidyverse)
  library(here)
  library(lubridate)
  library(readr)
})

# Inputs
FIGHTS_RAW   <- here("data/raw", "fight_data_raw.csv")
EVENT_MAP    <- here("data/raw", "event_fight_map.csv")
EVENT_CARDS  <- here("data/raw", "event_cards_parsed.csv")

# Outputs
FIGHTS_ENR            <- here("data/raw", "fight_data_raw_enriched.csv")
GAP_FIGHTS_NO_EVENT   <- here("data/raw", "join_gaps_fights_missing_event.csv")
GAP_EVENTMAP_EXTRA    <- here("data/raw", "join_gaps_eventmap_unmatched_fights.csv")
GAP_EVENTS_NO_FIGHTS  <- here("data/raw", "join_gaps_events_without_fights.csv")

# --- Load ---
df      <- read_csv(FIGHTS_RAW,  show_col_types = FALSE)
emap    <- read_csv(EVENT_MAP,   show_col_types = FALSE)
events  <- read_csv(EVENT_CARDS, show_col_types = FALSE)

# --- Normalize join keys to lowercase everywhere ---
df <- df %>% mutate(fight_id = tolower(fight_id))
emap <- emap %>% mutate(fight_id = tolower(fight_id), event_id = tolower(event_id))
events <- events %>% mutate(event_id = tolower(event_id))

# --- Build per-fight event info ---
event_info <- emap %>%
  distinct(fight_id, event_id) %>%
  left_join(
    events %>% select(event_id, event_title, event_date, event_location),
    by = "event_id"
  ) %>%
  distinct(fight_id, .keep_all = TRUE)

# Helper: safe fallback to an existing `date` column only if present
fallback_date <- if ("date" %in% names(df)) df$date else as.Date(NA)

# --- Merge onto fights ---
out <- df %>%
  left_join(event_info, by = "fight_id") %>%
  mutate(
    date       = coalesce(ymd(event_date), suppressWarnings(as.Date(fallback_date))),
    event_name = event_title,
    location   = event_location
  ) %>%
  select(-any_of(c("event_date", "event_title", "event_location")))

write_csv(out, FIGHTS_ENR)

# ---------------- Gap reports ----------------

# 1) Fights that didn't find event metadata
fights_no_event <- out %>%
  filter(is.na(event_name) | is.na(date) | is.na(location)) %>%
  select(fight_id, date, event_name, location)
if (nrow(fights_no_event) > 0) write_csv(fights_no_event, GAP_FIGHTS_NO_EVENT)

# 2) fight_ids present in event_fight_map but missing from fight_data_raw
emap_extra <- emap %>%
  distinct(fight_id, event_id) %>%
  anti_join(df %>% distinct(fight_id), by = "fight_id") %>%
  left_join(events %>% select(event_id, event_title, event_date, event_location), by = "event_id") %>%
  select(fight_id, event_id, event_title, event_date, event_location)
if (nrow(emap_extra) > 0) write_csv(emap_extra, GAP_EVENTMAP_EXTRA)

# 3) events that have zero fights mapped
events_no_fights <- events %>%
  anti_join(emap %>% distinct(event_id), by = "event_id") %>%
  select(event_id, event_title, event_date, event_location)
if (nrow(events_no_fights) > 0) write_csv(events_no_fights, GAP_EVENTS_NO_FIGHTS)

# --- Console log ---
cat("Rows in fights raw     :", nrow(df), "\n")
cat("Rows in fights enriched:", nrow(out), "\n")
cat("Wrote:", FIGHTS_ENR, "\n")
cat("Gaps — fights w/ no event:", nrow(fights_no_event), if (nrow(fights_no_event)>0) paste0(" -> ", GAP_FIGHTS_NO_EVENT) else "", "\n")
cat("Gaps — map-only fights  :", nrow(emap_extra),       if (nrow(emap_extra)>0)       paste0(" -> ", GAP_EVENTMAP_EXTRA)    else "", "\n")
cat("Gaps — events w/ no fights:", nrow(events_no_fights), if (nrow(events_no_fights)>0) paste0(" -> ", GAP_EVENTS_NO_FIGHTS) else "", "\n")
