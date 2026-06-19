# scripts/00_scrape_fights.R
#
# Stage 0 of the pipeline: scrape UFCStats and build the enriched raw
# fight dataset.
#
# Flow:
#   1. Discover all completed events from the events index.
#   2. Fetch + parse event pages -> event metadata + event->fight map.
#   3. Fetch + parse each fight page -> per-fighter fight stats.
#   4. Join fights to their event metadata -> fight_data_raw_enriched.csv
#
# The whole stage is cache-first and incremental: rerunning it only
# fetches events/fights that are new or previously failed, and only
# re-parses pages that are new or were just refetched. To force a fully
# fresh scrape, delete cache/ (or set STALE_AFTER_DAYS to a finite value).
#
# Output: data/raw/fight_data_raw_enriched.csv

suppressPackageStartupMessages({
  library(here)
})
source(here::here("scripts", "_helpers.R"))

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
EVENTS_INDEX <- paste0(BASE_URL, "/statistics/events/completed?page=all")

EVENT_CACHE <- here("cache", "events")
FIGHT_CACHE <- here("cache", "fights")
OUT_DIR     <- here("data", "raw")

EVENT_MANIFEST <- file.path(OUT_DIR, "events_manifest.csv")
FIGHT_MANIFEST <- file.path(OUT_DIR, "fights_manifest.csv")
OUT_EVENTS     <- file.path(OUT_DIR, "event_cards_parsed.csv")
OUT_MAP        <- file.path(OUT_DIR, "event_fight_map.csv")
OUT_FIGHTS_RAW <- file.path(OUT_DIR, "fight_data_raw.csv")
OUT_ENRICHED   <- file.path(OUT_DIR, "fight_data_raw_enriched.csv")
ERR_EVENTS     <- file.path(OUT_DIR, "parse_errors_events.csv")
ERR_FIGHTS     <- file.path(OUT_DIR, "parse_errors_fights.csv")

# Re-fetch a successfully cached page only if older than this many days.
# Inf = never re-fetch (cache is authoritative); pages are only fetched
# when missing or previously failed.
STALE_AFTER_DAYS  <- Inf
MAX_EVENT_RETRIES <- 6
MAX_FIGHT_RETRIES <- 5

EVENT_UA <- "UFC stats research scraper (event cache)"
FIGHT_UA <- "UFC stats research scraper (fight cache)"

dir.create(EVENT_CACHE, recursive = TRUE, showWarnings = FALSE)
dir.create(FIGHT_CACHE, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# -------------------------------------------------------------------
# Event-page parsing
# -------------------------------------------------------------------
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
  x <- html_elements(doc, xpath = sprintf("//li[contains(normalize-space(.), '%s:')]", label)) %>%
    html_text2() %>%
    str_squish()
  if (length(x) == 0) return(NA_character_)
  val <- sub(sprintf("^\\s*%s\\s*:\\s*", label), "", x[1], ignore.case = TRUE)
  val <- str_squish(val)
  if (!nzchar(val)) NA_character_ else val
}

extract_event_date <- function(doc) {
  d1 <- extract_labeled_value(doc, "Date")
  if (!is.na(d1)) return(d1)
  all_text <- html_text2(doc) %>% str_squish()
  m <- str_match(all_text, "([A-Za-z]+\\s+\\d{1,2},\\s+\\d{4})")
  m[, 2] %||% NA_character_
}

extract_fight_urls <- function(doc) {
  html_elements(doc, 'a[href*="fight-details/"]') %>%
    html_attr("href") %>%
    discard(is.na) %>%
    map_chr(abs_url) %>%
    unique()
}

# Parse one cached event page into (event metadata row, fight-map rows).
parse_event_file <- function(event_id, event_url, path) {
  doc <- read_html(path)
  date_raw <- extract_event_date(doc)
  date_parsed <- suppressWarnings(mdy(date_raw))

  event_row <- tibble(
    event_id = as.character(event_id),
    event_url = as.character(event_url),
    event_title = squish_na(extract_event_title(doc)),
    event_date_txt = squish_na(date_raw),
    event_date = if (is.na(date_parsed)) NA_character_ else as.character(date_parsed),
    event_location = squish_na(extract_labeled_value(doc, "Location"))
  )

  fight_urls <- extract_fight_urls(doc)
  map_row <- if (length(fight_urls) > 0) {
    fight_ids <- id_from_url(fight_urls, "fight")
    keep <- !is.na(fight_ids) & nzchar(fight_ids)
    tibble(
      event_id = as.character(event_id),
      event_url = as.character(event_url),
      fight_id = as.character(fight_ids[keep]),
      fight_url = as.character(fight_urls[keep])
    ) %>%
      distinct(event_id, fight_id, .keep_all = TRUE)
  } else {
    tibble(event_id = character(), event_url = character(),
           fight_id = character(), fight_url = character())
  }

  list(event_row = event_row, map_row = map_row)
}

# -------------------------------------------------------------------
# Fight-page parsing
# -------------------------------------------------------------------

# Numeric stat columns that must never silently change type across runs.
STAT_COLS <- c(
  "fighter_1_KD", "fighter_2_KD",
  "fighter_1_Sig_Strike_Landed", "fighter_1_Sig_Strike_Attempts",
  "fighter_2_Sig_Strike_Landed", "fighter_2_Sig_Strike_Attempts",
  "fighter_1_Strike_Landed", "fighter_1_Strike_Attempts",
  "fighter_2_Strike_Landed", "fighter_2_Strike_Attempts",
  "fighter_1_TD_Landed", "fighter_1_TD_Attempts",
  "fighter_2_TD_Landed", "fighter_2_TD_Attempts",
  "fighter_1_Sub_Attempts", "fighter_2_Sub_Attempts",
  "fighter_1_Rev", "fighter_2_Rev",
  "fighter_1_Sig_Strike_Percent", "fighter_2_Sig_Strike_Percent",
  "fighter_1_TD_Percent", "fighter_2_TD_Percent"
)

# Clock-like columns kept as character in the raw layer (parsed to
# seconds later, in 02_clean_fight_data.R).
CLOCK_COLS <- c("time", "fighter_1_Ctrl", "fighter_2_Ctrl")

coerce_fight_schema <- function(df) {
  df %>%
    mutate(
      across(any_of(STAT_COLS), ~ {
        x <- as.character(.x)
        x <- str_replace_all(x, "—|–|--", "")  # em/en dash, double hyphen
        x <- na_if(str_squish(x), "")
        suppressWarnings(as.numeric(x))
      }),
      across(any_of(CLOCK_COLS), ~ na_if(str_squish(as.character(.x)), ""))
    )
}

# Parse one cached fight page into a one-row tibble of fight stats.
parse_one_fight <- function(path) {
  fight_id <- id_from_path(path)
  doc <- read_html(path)

  tbl_nodes <- html_nodes(doc, "table")
  if (length(tbl_nodes) < 1) stop("No <table> nodes found")

  tbl_list <- html_table(tbl_nodes[1], trim = TRUE, fill = TRUE)
  if (length(tbl_list) == 0 || nrow(tbl_list[[1]]) == 0) stop("First table empty")

  # Fighter ids from the first column's profile links (with fallbacks).
  fighter_anchors <- tbl_nodes[1] %>%
    html_element("tbody") %>%
    html_elements("tr td:nth-child(1) a.b-link[href*='fighter-details']")
  if (length(fighter_anchors) == 0) {
    fighter_anchors <- tbl_nodes[1] %>%
      html_elements("tr td:nth-child(1) a.b-link[href*='fighter-details']")
  }
  if (length(fighter_anchors) == 0) {
    fighter_anchors <- doc %>%
      html_elements(".b-fight-details__persons .b-fight-details__person-name a.b-fight-details__person-link")
  }
  fighter_hrefs <- fighter_anchors %>% html_attr("href") %>% str_squish() %>% head(2)
  fighter_ids <- map_chr(fighter_hrefs, function(x) {
    if (is.na(x) || !nzchar(x)) NA_character_ else sub(".*/fighter-details/([^/?#]+).*", "\\1", x)
  })
  fighter_ids <- c(fighter_ids, NA_character_, NA_character_)[1:2]

  # The summary table holds both fighters' values per stat, separated by
  # 2+ spaces. Reshape long -> split the two fighters -> wide, then split
  # the "landed of attempts" fields.
  summary_data <- bind_rows(tbl_list) %>%
    as_tibble() %>%
    rename(
      "Fighter" = 1, "KD" = 2, "Sig_Strike" = 3, "Sig_Strike_Percent" = 4,
      "Total_Strikes" = 5, "TD" = 6, "TD_Percent" = 7, "Sub_Attempts" = 8,
      "Rev" = 9, "Ctrl" = 10
    ) %>%
    pivot_longer(cols = everything(), names_to = "key", values_to = "value") %>%
    separate(value, into = c("fighter_1", "fighter_2"), sep = "\\s{2,}", extra = "merge", fill = "right") %>%
    mutate(across(everything(), squish_na)) %>%
    pivot_wider(names_from = key, values_from = c(fighter_1, fighter_2)) %>%
    separate(fighter_1_Sig_Strike, into = c("fighter_1_Sig_Strike_Landed", "fighter_1_Sig_Strike_Attempts"), sep = " of ", extra = "merge") %>%
    separate(fighter_2_Sig_Strike, into = c("fighter_2_Sig_Strike_Landed", "fighter_2_Sig_Strike_Attempts"), sep = " of ", extra = "merge") %>%
    separate(fighter_1_Total_Strikes, into = c("fighter_1_Strike_Landed", "fighter_1_Strike_Attempts"), sep = " of ", extra = "merge") %>%
    separate(fighter_2_Total_Strikes, into = c("fighter_2_Strike_Landed", "fighter_2_Strike_Attempts"), sep = " of ", extra = "merge") %>%
    separate(fighter_1_TD, into = c("fighter_1_TD_Landed", "fighter_1_TD_Attempts"), sep = " of ", extra = "merge") %>%
    separate(fighter_2_TD, into = c("fighter_2_TD_Landed", "fighter_2_TD_Attempts"), sep = " of ", extra = "merge") %>%
    mutate(
      across(contains("Percent"), ~ suppressWarnings(as.numeric(str_remove(.x, "%"))) * 0.01),
      across(-matches("(^fighter_1$|^fighter_2$|Fighter|Ctrl$)"), ~ suppressWarnings(as.numeric(.x)))
    )

  # W/L/D/NC status per fighter.
  statuses <- doc %>%
    html_elements(".b-fight-details__persons .b-fight-details__person .b-fight-details__person-status") %>%
    html_text2() %>% str_squish() %>% toupper()
  statuses <- c(statuses, NA_character_, NA_character_)[1:2]

  # Method / round / time / referee block.
  details_vals <- doc %>%
    html_nodes(xpath = '//*[contains(concat(" ", @class, " "), " b-fight-details__text ") and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//i') %>%
    html_text()
  fight_details <- tibble(value = details_vals) %>%
    mutate(value = str_squish(value)) %>%
    separate(value, into = c("feature", "value"), sep = ":", extra = "merge") %>%
    mutate(value = str_trim(value)) %>%
    replace_na(list(value = "")) %>%
    filter(value != "") %>%
    pivot_wider(names_from = feature, values_from = value) %>%
    rename_with(~ str_replace_all(.x, "\\s|/", "_") %>% tolower()) %>%
    rename(round_finished = round)

  # Weight-class title line (text after the last dash).
  title_text <- doc %>%
    html_nodes(".b-fight-details__fight-title") %>%
    html_text() %>% str_replace_all("\n", "") %>% str_trim()

  ids <- tibble(
    fight_id = fight_id,
    fighter_1_id = fighter_ids[1],
    fighter_2_id = fighter_ids[2],
    source_path = path
  )

  out <- bind_cols(
    summary_data,
    ids,
    fight_details %>%
      bind_cols(tibble(
        fighter_1_res = if (statuses[1] %in% c("W", "L", "D", "NC")) statuses[1] else NA_character_,
        fighter_2_res = if (statuses[2] %in% c("W", "L", "D", "NC")) statuses[2] else NA_character_,
        weight_class = title_text
      )) %>%
      mutate(
        weight_class = str_squish(weight_class),
        weight_class = str_replace(weight_class, ".*-\\s*", "")
      )
  ) %>%
    as_tibble()

  coerce_fight_schema(out)
}

# Parse a vector of fight paths, capturing per-file errors instead of
# aborting the whole run.
parse_fights_safely <- function(paths) {
  safe_parse <- safely(parse_one_fight, otherwise = NULL)
  res <- map(paths, safe_parse)

  parsed <- compact(map(res, "result"))
  errs <- tibble(
    path = paths,
    fight_id = id_from_path(paths),
    error = map_chr(map(res, "error"), ~ if (is.null(.x)) NA_character_ else conditionMessage(.x))
  ) %>%
    filter(!is.na(error))

  parsed_tbl <- if (length(parsed) > 0) {
    bind_rows(parsed) %>%
      mutate(round_finished = suppressWarnings(as.integer(round_finished))) %>%
      distinct() %>%
      coerce_fight_schema()
  } else {
    tibble()
  }

  list(parsed = parsed_tbl, errors = errs)
}

# ===================================================================
# 1) Discover all event cards from the completed-events index
# ===================================================================
message("Discovering event cards from UFCStats index...")

cards <- read_html(EVENTS_INDEX) %>%
  html_elements("a.b-link_style_black") %>%
  html_attr("href") %>%
  discard(is.na) %>%
  map_chr(abs_url) %>%
  keep(~ grepl("/event-details/[0-9A-Fa-f]{16}", .x, perl = TRUE)) %>%
  unique()

if (length(cards) == 0) stop("No event card links found at: ", EVENTS_INDEX)

event_manifest_old <- read_csv_if_exists(EVENT_MANIFEST) %>% coerce_manifest("event_id")
event_manifest_new <- tibble(event_id = id_from_url(cards, "event"), url = cards) %>%
  filter(!is.na(event_id), nzchar(event_id)) %>%
  distinct(event_id, .keep_all = TRUE) %>%
  mutate(path = file.path(EVENT_CACHE, paste0(event_id, ".html")))
event_manifest_new <- bind_cols(event_manifest_new, seed_status_cols(nrow(event_manifest_new)))

event_manifest <- merge_manifests(event_manifest_old, event_manifest_new, "event_id") %>%
  coerce_manifest("event_id") %>%
  mutate(needs_fetch = manifest_needs_fetch(path, fetch_ok, fetched_at, STALE_AFTER_DAYS))

need_events <- event_manifest %>% filter(needs_fetch)
message("Event cards indexed : ", nrow(event_manifest))
message("Event cards to fetch: ", nrow(need_events))

if (nrow(need_events) > 0) {
  event_fetches <- fetch_sequential(
    need_events, "url", "path", EVENT_UA, MAX_EVENT_RETRIES,
    id_col = "event_id", log_label = "event"
  ) %>%
    bind_cols(need_events %>% select(event_id))
  event_manifest <- event_manifest %>% select(-needs_fetch) %>%
    apply_fetch_results(event_fetches, "event_id")
} else {
  event_manifest <- event_manifest %>% select(-needs_fetch)
}

write_csv(event_manifest, EVENT_MANIFEST)

# ===================================================================
# 2) Parse cached event cards -> event metadata + fight map
# ===================================================================
message("Parsing cached event cards...")

cached_events <- event_manifest %>% filter(file.exists(path)) %>% distinct(event_id, .keep_all = TRUE)

event_parse_res <- map(seq_len(nrow(cached_events)), function(i) {
  tryCatch(
    parse_event_file(cached_events$event_id[i], cached_events$url[i], cached_events$path[i]),
    error = function(e) list(
      event_row = NULL, map_row = NULL,
      error = tibble(event_id = cached_events$event_id[i], url = cached_events$url[i],
                     path = cached_events$path[i], error = conditionMessage(e))
    )
  )
})

event_errors <- compact(map(event_parse_res, "error"))
if (length(event_errors) > 0) {
  write_csv(bind_rows(event_errors) %>% distinct(event_id, .keep_all = TRUE), ERR_EVENTS)
} else if (file.exists(ERR_EVENTS)) {
  file.remove(ERR_EVENTS)
}

events_tbl <- compact(map(event_parse_res, "event_row")) %>% bind_rows() %>% distinct(event_id, .keep_all = TRUE)
map_tbl    <- compact(map(event_parse_res, "map_row"))   %>% bind_rows() %>% distinct(event_id, fight_id, .keep_all = TRUE)

write_csv(events_tbl, OUT_EVENTS)
write_csv(map_tbl, OUT_MAP)

event_manifest <- mark_parsed(event_manifest, events_tbl$event_id, "event_id")
write_csv(event_manifest, EVENT_MANIFEST)

message("Parsed events      : ", nrow(events_tbl))
message("Parsed fight links : ", nrow(map_tbl))

# ===================================================================
# 3) Build/update fight manifest, then fetch missing fight pages
# ===================================================================
fight_manifest_old <- read_csv_if_exists(FIGHT_MANIFEST) %>% coerce_manifest("fight_id", extra = "event_id")
fight_manifest_new <- map_tbl %>%
  transmute(fight_id = as.character(fight_id), url = as.character(fight_url),
            event_id = as.character(event_id),
            path = file.path(FIGHT_CACHE, paste0(fight_id, ".html"))) %>%
  distinct(fight_id, .keep_all = TRUE)
fight_manifest_new <- bind_cols(fight_manifest_new, seed_status_cols(nrow(fight_manifest_new)))

fight_manifest <- merge_manifests(fight_manifest_old, fight_manifest_new, "fight_id") %>%
  coerce_manifest("fight_id", extra = "event_id") %>%
  mutate(needs_fetch = manifest_needs_fetch(path, fetch_ok, fetched_at, STALE_AFTER_DAYS))

need_fights <- fight_manifest %>% filter(needs_fetch)
message("Fight pages indexed : ", nrow(fight_manifest))
message("Fight pages to fetch: ", nrow(need_fights))

if (nrow(need_fights) > 0) {
  fight_fetches <- fetch_in_batches(need_fights, "url", "path", FIGHT_UA, MAX_FIGHT_RETRIES) %>%
    bind_cols(need_fights %>% select(fight_id))
  fight_manifest <- fight_manifest %>% select(-needs_fetch) %>%
    apply_fetch_results(fight_fetches, "fight_id")
} else {
  fight_fetches <- tibble(fight_id = character())
  fight_manifest <- fight_manifest %>% select(-needs_fetch)
}

write_csv(fight_manifest, FIGHT_MANIFEST)

# ===================================================================
# 4) Parse new/refetched fight pages -> raw fight data
# ===================================================================
existing_fight_raw <- read_csv_if_exists(OUT_FIGHTS_RAW) %>% coerce_fight_schema()
already_parsed <- if ("fight_id" %in% names(existing_fight_raw)) unique(existing_fight_raw$fight_id) else character()
refetched_ids <- if (nrow(fight_fetches) > 0) fight_fetches$fight_id[fight_fetches$fetch_ok %in% TRUE] else character()

fight_parse_queue <- fight_manifest %>%
  filter(file.exists(path)) %>%
  filter(!(fight_id %in% already_parsed) | fight_id %in% refetched_ids)

message("Fight pages to parse: ", nrow(fight_parse_queue))

fight_parsed <- parse_fights_safely(fight_parse_queue$path)

if (nrow(fight_parsed$errors) > 0) {
  out_errs <- bind_rows(read_csv_if_exists(ERR_FIGHTS), fight_parsed$errors) %>%
    distinct(fight_id, .keep_all = TRUE)
  write_csv(out_errs, ERR_FIGHTS)
}

new_fight_raw <- fight_parsed$parsed
fight_raw_all <- if (nrow(existing_fight_raw) > 0 && nrow(new_fight_raw) > 0) {
  bind_rows(new_fight_raw, existing_fight_raw) %>% distinct(fight_id, .keep_all = TRUE) %>% coerce_fight_schema()
} else if (nrow(new_fight_raw) > 0) {
  new_fight_raw %>% coerce_fight_schema()
} else {
  existing_fight_raw %>% coerce_fight_schema()
}

write_csv(fight_raw_all, OUT_FIGHTS_RAW)
fight_manifest <- mark_parsed(fight_manifest, new_fight_raw[["fight_id"]], "fight_id")
write_csv(fight_manifest, FIGHT_MANIFEST)

# ===================================================================
# 5) Enrich raw fights with event metadata (date, name, location)
# ===================================================================
event_info <- map_tbl %>%
  distinct(fight_id, event_id) %>%
  left_join(events_tbl %>% select(event_id, event_title, event_date, event_location), by = "event_id") %>%
  distinct(fight_id, .keep_all = TRUE)

fallback_date <- if ("date" %in% names(fight_raw_all)) fight_raw_all$date else as.Date(NA)

fight_enriched <- if (nrow(fight_raw_all) == 0 || !"fight_id" %in% names(fight_raw_all)) {
  tibble()
} else {
  fight_raw_all %>%
    mutate(fight_id = tolower(fight_id)) %>%
    left_join(event_info %>% mutate(fight_id = tolower(fight_id), event_id = tolower(event_id)), by = "fight_id") %>%
    mutate(
      date = coalesce(ymd(event_date), suppressWarnings(as.Date(fallback_date))),
      event_name = event_title,
      location = event_location
    ) %>%
    select(-any_of(c("event_date", "event_title", "event_location"))) %>%
    distinct(fight_id, .keep_all = TRUE)
}

write_csv(fight_enriched, OUT_ENRICHED)

message("Done (00_scrape_fights).")
message("  Fight raw      : ", OUT_FIGHTS_RAW)
message("  Fight enriched : ", OUT_ENRICHED)
