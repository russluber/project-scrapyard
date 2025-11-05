# backfill_event_map.R  (run this as-is)

suppressPackageStartupMessages({
  library(tidyverse)
  library(here)
  library(readr)
  library(rvest)
  library(stringr)
})

EVENT_CACHE   <- here("cache", "events")
EVENT_MAP_CSV <- here("data",  "event_fight_map.csv")
EVENTS_PARSED <- here("data",  "event_cards_parsed.csv")  # for event_id
GAPS_CSV      <- here("data",  "join_gaps_fights_missing_event.csv")

# load existing artifacts
stopifnot(file.exists(EVENT_MAP_CSV), file.exists(EVENTS_PARSED), file.exists(GAPS_CSV))

emap    <- read_csv(EVENT_MAP_CSV, show_col_types = FALSE)
events  <- read_csv(EVENTS_PARSED, show_col_types = FALSE)
gaps    <- read_csv(GAPS_CSV,      show_col_types = FALSE)   # the 13 missing fights
need_ids <- unique(gaps$fight_id)

# helper: pull all fight_ids from one event HTML (very lenient)
extract_fight_ids_from_event <- function(event_path) {
  if (!file.exists(event_path)) return(tibble(fight_id = character()))
  txt <- tryCatch(readLines(event_path, warn = FALSE), error = function(e) character())
  if (!length(txt)) return(tibble(fight_id = character()))
  m <- str_match_all(txt, "(?i)fight-details/([0-9A-Fa-f]{16})")
  ids <- unique(tolower(unlist(map(m, ~ .x[,2]))))
  tibble(fight_id = ids)
}

# build a backfill map by scanning all cached event pages
event_files <- tibble(
  event_id  = str_remove(basename(list.files(EVENT_CACHE, pattern = "\\.html$", full.names = TRUE)), "\\.html$"),
  event_path = list.files(EVENT_CACHE, pattern = "\\.html$", full.names = TRUE)
)

backfill <- event_files %>%
  mutate(fights = map(event_path, extract_fight_ids_from_event)) %>%
  unnest(fights) %>%
  distinct(event_id, fight_id) %>%
  # only the fights we need to backfill
  semi_join(tibble(fight_id = need_ids), by = "fight_id") %>%
  # drop ones already in the current map (just in case)
  anti_join(emap %>% distinct(event_id, fight_id), by = c("event_id","fight_id"))

cat("Backfill pairs found:", nrow(backfill), "\n")

# if we found any, append to the map and de-dup
if (nrow(backfill) > 0) {
  emap2 <- bind_rows(emap, backfill) %>%
    mutate(
      fight_id = tolower(fight_id),
      event_id = tolower(event_id)
    ) %>%
    distinct(event_id, fight_id, .keep_all = TRUE)
  write_csv(emap2, EVENT_MAP_CSV)
  cat("Updated:", EVENT_MAP_CSV, "\n")
} else {
  cat("No new pairs to backfill.\n")
}
