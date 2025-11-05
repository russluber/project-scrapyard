# # parse_event_cards.R — parse cached event card HTMLs into metadata + fight map
# 
# suppressPackageStartupMessages({
#   library(rvest)
#   library(tidyverse)
#   library(here)
#   library(readr)
#   library(stringr)
#   library(lubridate)
# })
# 
# # ------------------------- Config -------------------------
# EVENT_MANIFEST <- here("data",  "events_manifest.csv")
# CACHE_DIR      <- here("cache", "events")
# OUT_EVENTS     <- here("data",  "event_cards_parsed.csv")
# OUT_MAP        <- here("data",  "event_fight_map.csv")
# 
# dir.create(dirname(OUT_EVENTS), recursive = TRUE, showWarnings = FALSE)
# 
# # ------------------------- Helpers ------------------------
# `%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b
# squish_na <- function(x) { x <- str_squish(x); na_if(x, "") }
# event_id_from_path <- function(p) sub("\\.html$", "", basename(p), ignore.case = TRUE)
# fight_id_from_url  <- function(u) sub(".*/fight-details/([0-9A-Fa-f]{16}).*", "\\1", u)
# abs_url <- function(link, base = "http://ufcstats.com") {
#   tryCatch(xml2::url_absolute(link, base = base), error = function(e) link)
# }
# 
# # Robust extractors for event page
# extract_event_title <- function(doc) {
#   # Try common title containers, then fallback to <title>
#   cands <- c(
#     html_text2(html_element(doc, ".b-content__title")),
#     html_text2(html_element(doc, ".b-content__title span")),
#     html_text2(html_element(doc, "h2")),
#     html_text2(html_element(doc, "title"))
#   )
#   squish_na(first(keep(cands, ~ !is.na(.x) && nzchar(.x))))
# }
# 
# extract_labeled_value <- function(doc, label) {
#   # Find an <li> whose text contains "Label:" and take the part after the colon
#   # This matches UFCStats event pages like: <li>Date: October 25, 2025</li>
#   x <- html_elements(doc, xpath = sprintf("//li[contains(normalize-space(.), '%s:')]", label)) |>
#     html_text2() |>
#     str_squish()
#   if (length(x) == 0) return(NA_character_)
#   val <- sub(sprintf("^\\s*%s\\s*:\\s*", label), "", x[1], ignore.case = TRUE)
#   val <- str_squish(val)
#   if (!nzchar(val)) NA_character_ else val
# }
# 
# extract_date <- function(doc) {
#   # Prefer "Date:" list item; fallback: try to spot a trailing "Month DD, YYYY" anywhere
#   d1 <- extract_labeled_value(doc, "Date")
#   if (!is.na(d1)) return(d1)
#   # Fallback: biggest/first block that has Month DD, YYYY
#   all_text <- html_text2(doc) %>% str_squish()
#   m <- str_match(all_text, "([A-Za-z]+\\s+\\d{1,2},\\s+\\d{4})")
#   m[,2] %||% NA_character_
# }
# 
# extract_location <- function(doc) {
#   # Typical: <li>Location: City, Region, Country</li>
#   extract_labeled_value(doc, "Location")
# }
# 
# extract_fight_urls <- function(doc) {
#   html_elements(doc, 'a[href*="fight-details/"]') |>
#     html_attr("href") |>
#     discard(is.na) |>
#     map_chr(abs_url) |>
#     unique()
# }
# 
# # ------------------------- Main ---------------------------
# if (!file.exists(EVENT_MANIFEST)) {
#   stop("Missing manifest: ", EVENT_MANIFEST, ". Run fetch_event_cards.R first.")
# }
# 
# man <- read_csv(EVENT_MANIFEST, show_col_types = FALSE)
# if (nrow(man) == 0) stop("Event manifest is empty.")
# 
# # Only parse files that exist
# man <- man |> filter(file.exists(path))
# if (nrow(man) == 0) stop("No cached event HTML files found in: ", CACHE_DIR)
# 
# # Resume support: read prior outputs if present
# prior_events <- if (file.exists(OUT_EVENTS)) read_csv(OUT_EVENTS, show_col_types = FALSE) else tibble()
# prior_map    <- if (file.exists(OUT_MAP))    read_csv(OUT_MAP,    show_col_types = FALSE) else tibble()
# 
# # Parse each event page
# parsed_events <- vector("list", nrow(man))
# parsed_maps   <- vector("list", nrow(man))
# 
# for (i in seq_len(nrow(man))) {
#   p <- man$path[i]
#   eid <- man$event_id[i]
#   eurl <- man$event_url[i]
#   
#   # Skip if already parsed (based on event_id) and files existed
#   if (nrow(prior_events) > 0 && eid %in% prior_events$event_id) {
#     # But still ensure the fight map includes all fights (in case we add more)
#     doc <- read_html(p)
#   } else {
#     doc <- read_html(p)
#   }
#   
#   title_raw <- extract_event_title(doc)
#   date_raw  <- extract_date(doc)
#   loc_raw   <- extract_location(doc)
#   
#   # Normalize date to ISO if parseable, keep original too
#   date_parsed <- suppressWarnings(mdy(date_raw))
#   date_iso    <- ifelse(is.na(date_parsed), NA_character_, as.character(date_parsed))
#   
#   # Build event row
#   parsed_events[[i]] <- tibble(
#     event_id       = eid,
#     event_url      = eurl,
#     event_title    = squish_na(title_raw),
#     event_date_txt = squish_na(date_raw),
#     event_date     = date_iso,
#     event_location = squish_na(loc_raw)
#   )
#   
#   # Build fight map rows
#   fights <- extract_fight_urls(doc)
#   if (length(fights) > 0) {
#     fids <- fight_id_from_url(fights)
#     keep <- !is.na(fids) & nzchar(fids)
#     parsed_maps[[i]] <- tibble(
#       event_id  = eid,
#       event_url = eurl,
#       fight_id  = fids[keep],
#       fight_url = fights[keep]
#     ) |> distinct(fight_id, .keep_all = TRUE)
#   } else {
#     parsed_maps[[i]] <- tibble(
#       event_id = eid, event_url = eurl,
#       fight_id = character(), fight_url = character()
#     )[0,]
#   }
# }
# 
# events_tbl <- list_rbind(parsed_events) |> distinct(event_id, .keep_all = TRUE)
# map_tbl    <- list_rbind(parsed_maps)   |> distinct(event_id, fight_id, .keep_all = TRUE)
# 
# # Merge with prior outputs (resume-friendly)
# if (nrow(prior_events) > 0) {
#   events_tbl <- bind_rows(prior_events, events_tbl) |> distinct(event_id, .keep_all = TRUE)
# }
# if (nrow(prior_map) > 0) {
#   map_tbl <- bind_rows(prior_map, map_tbl) |> distinct(event_id, fight_id, .keep_all = TRUE)
# }
# 
# # Write outputs
# write_csv(events_tbl, OUT_EVENTS)
# write_csv(map_tbl,   OUT_MAP)
# 
# message("Parsed events      : ", nrow(events_tbl))
# message("Parsed fight links : ", nrow(map_tbl))
# message("Events CSV         : ", OUT_EVENTS)
# message("Event–fight map    : ", OUT_MAP)


# parse_event_cards.R — parse cached event card HTMLs into metadata + fight map

suppressPackageStartupMessages({
  library(rvest)
  library(tidyverse)
  library(here)
  library(readr)
  library(stringr)
  library(lubridate)
})

# ------------------------- Config -------------------------
EVENT_MANIFEST <- here("data",  "events_manifest.csv")
CACHE_DIR      <- here("cache", "events")
OUT_EVENTS     <- here("data",  "event_cards_parsed.csv")
OUT_MAP        <- here("data",  "event_fight_map.csv")

dir.create(dirname(OUT_EVENTS), recursive = TRUE, showWarnings = FALSE)

# ------------------------- Helpers ------------------------
`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b
squish_na <- function(x) { x <- str_squish(x); na_if(x, "") }
event_id_from_path <- function(p) sub("\\.html$", "", basename(p), ignore.case = TRUE)
fight_id_from_url  <- function(u) sub(".*/fight-details/([0-9A-Fa-f]{16}).*", "\\1", u)
abs_url <- function(link, base = "http://ufcstats.com") {
  tryCatch(xml2::url_absolute(link, base = base), error = function(e) link)
}

extract_event_title <- function(doc) {
  cands <- c(
    html_text2(html_element(doc, ".b-content__title")),
    html_text2(html_element(doc, ".b-content__title span")),
    html_text2(html_element(doc, "h2")),
    html_text2(html_element(doc, "title"))
  )
  squish_na(first(keep(cands, ~ !is.na(.x) && nzchar(.x))))
}

extract_labeled_value <- function(doc, label) {
  x <- html_elements(doc, xpath = sprintf("//li[contains(normalize-space(.), '%s:')]", label)) |>
    html_text2() |>
    str_squish()
  if (length(x) == 0) return(NA_character_)
  val <- sub(sprintf("^\\s*%s\\s*:\\s*", label), "", x[1], ignore.case = TRUE)
  val <- str_squish(val)
  if (!nzchar(val)) NA_character_ else val
}

extract_date <- function(doc) {
  d1 <- extract_labeled_value(doc, "Date")
  if (!is.na(d1)) return(d1)
  all_text <- html_text2(doc) %>% str_squish()
  m <- str_match(all_text, "([A-Za-z]+\\s+\\d{1,2},\\s+\\d{4})")
  m[,2] %||% NA_character_
}

extract_location <- function(doc) {
  extract_labeled_value(doc, "Location")
}

extract_fight_urls <- function(doc) {
  html_elements(doc, 'a[href*="fight-details/"]') |>
    html_attr("href") |>
    discard(is.na) |>
    map_chr(abs_url) |>
    unique()
}

# ------------------------- Main ---------------------------
if (!file.exists(EVENT_MANIFEST)) {
  stop("Missing manifest: ", EVENT_MANIFEST, ". Run fetch_event_cards.R first.")
}

man <- read_csv(EVENT_MANIFEST, show_col_types = FALSE)
if (nrow(man) == 0) stop("Event manifest is empty.")
man <- man |> filter(file.exists(path))
if (nrow(man) == 0) stop("No cached event HTML files found in: ", CACHE_DIR)

# ---- Resume support: FORCE consistent column types on prior CSVs ----
prior_events <- if (file.exists(OUT_EVENTS)) {
  read_csv(
    OUT_EVENTS,
    col_types = cols(
      event_id       = col_character(),
      event_url      = col_character(),
      event_title    = col_character(),
      event_date_txt = col_character(),
      event_date     = col_character(),  # <- force character
      event_location = col_character(),
      .default       = col_guess()
    ),
    show_col_types = FALSE
  )
} else tibble()

prior_map <- if (file.exists(OUT_MAP)) {
  read_csv(
    OUT_MAP,
    col_types = cols(
      event_id  = col_character(),
      event_url = col_character(),
      fight_id  = col_character(),
      fight_url = col_character(),
      .default  = col_guess()
    ),
    show_col_types = FALSE
  )
} else tibble()

parsed_events <- vector("list", nrow(man))
parsed_maps   <- vector("list", nrow(man))

for (i in seq_len(nrow(man))) {
  p    <- man$path[i]
  eid  <- man$event_id[i]
  eurl <- man$event_url[i]
  
  doc <- read_html(p)
  
  title_raw <- extract_event_title(doc)
  date_raw  <- extract_date(doc)          # "Month DD, YYYY" or NA
  loc_raw   <- extract_location(doc)
  
  # Normalize to ISO string IF parseable, but KEEP as character
  date_parsed <- suppressWarnings(mdy(date_raw))
  date_iso    <- ifelse(is.na(date_parsed), NA_character_, as.character(date_parsed))
  
  parsed_events[[i]] <- tibble(
    event_id       = as.character(eid),
    event_url      = as.character(eurl),
    event_title    = squish_na(title_raw),
    event_date_txt = squish_na(date_raw),   # original text
    event_date     = date_iso,              # ISO string, still character
    event_location = squish_na(loc_raw)
  )
  
  fights <- extract_fight_urls(doc)
  if (length(fights) > 0) {
    fids <- fight_id_from_url(fights)
    keep <- !is.na(fids) & nzchar(fids)
    parsed_maps[[i]] <- tibble(
      event_id  = as.character(eid),
      event_url = as.character(eurl),
      fight_id  = as.character(fids[keep]),
      fight_url = as.character(fights[keep])
    ) |> distinct(event_id, fight_id, .keep_all = TRUE)
  } else {
    parsed_maps[[i]] <- tibble(
      event_id  = character(),
      event_url = character(),
      fight_id  = character(),
      fight_url = character()
    )[0,]
  }
}

events_tbl <- list_rbind(parsed_events) |> distinct(event_id, .keep_all = TRUE)
map_tbl    <- list_rbind(parsed_maps)   |> distinct(event_id, fight_id, .keep_all = TRUE)

# ---- Ensure current tables are also character (belt-and-suspenders) ----
events_tbl <- events_tbl %>%
  mutate(
    event_id       = as.character(event_id),
    event_url      = as.character(event_url),
    event_title    = as.character(event_title),
    event_date_txt = as.character(event_date_txt),
    event_date     = as.character(event_date),     # <- keep character here
    event_location = as.character(event_location)
  )

map_tbl <- map_tbl %>%
  mutate(
    event_id  = as.character(event_id),
    event_url = as.character(event_url),
    fight_id  = as.character(fight_id),
    fight_url = as.character(fight_url)
  )

# ---- Merge with prior outputs (resume-friendly) ----
if (nrow(prior_events) > 0) {
  events_tbl <- bind_rows(prior_events, events_tbl) |> distinct(event_id, .keep_all = TRUE)
}
if (nrow(prior_map) > 0) {
  map_tbl <- bind_rows(prior_map, map_tbl) |> distinct(event_id, fight_id, .keep_all = TRUE)
}

# ---- Write outputs ----
write_csv(events_tbl, OUT_EVENTS)
write_csv(map_tbl,   OUT_MAP)

message("Parsed events      : ", nrow(events_tbl))
message("Parsed fight links : ", nrow(map_tbl))
message("Events CSV         : ", OUT_EVENTS)
message("Event–fight map    : ", OUT_MAP)
