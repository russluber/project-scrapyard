library(tidyverse)
library(here)
library(readr)
library(rvest)
library(stringr)

miss <- read_csv(here("data/join_gaps_fights_missing_event.csv"), show_col_types = FALSE)

fight_html_path <- function(fid) here("cache","fights", paste0(fid, ".html"))

extract_event_title <- function(fid){
  p <- fight_html_path(fid)
  if (!file.exists(p)) return(NA_character_)
  doc <- read_html(p)
  # Pull the “Event:” line from the details block
  lines <- doc %>%
    html_elements(".b-fight-details__text") %>%
    html_text2() %>% str_squish()
  # Grab the value after “Event:”
  out <- str_match(lines, "^Event:\\s*(.*)$")[,2]
  out[!is.na(out)][1] %||% NA_character_
}
 
`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

miss_evt <- miss %>%
  mutate(event_title_from_fight = map_chr(fight_id, extract_event_title))

# See where they cluster
miss_evt %>% count(event_title_from_fight, sort = TRUE)
# Peek a few ids from the top event
miss_evt %>% filter(event_title_from_fight == first(event_title_from_fight[order(event_title_from_fight)])) %>% head(10)
