suppressPackageStartupMessages({
  library(rvest)
  library(tidyverse)
  library(here)
  library(httr2)
  library(readr)
  library(stringr)
  library(lubridate)
  library(purrr)
  library(furrr)
  library(parallelly)
})

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
BASE_URL       <- "http://ufcstats.com"
EVENTS_INDEX   <- paste0(BASE_URL, "/statistics/events/completed?page=all")

EVENT_CACHE    <- here("cache", "events")
FIGHT_CACHE    <- here("cache", "fights")
OUT_DIR        <- here("data", "raw")

EVENT_MANIFEST <- file.path(OUT_DIR, "events_manifest.csv")
FIGHT_MANIFEST <- file.path(OUT_DIR, "fights_manifest.csv")
OUT_EVENTS     <- file.path(OUT_DIR, "event_cards_parsed.csv")
OUT_MAP        <- file.path(OUT_DIR, "event_fight_map.csv")
OUT_FIGHTS_RAW <- file.path(OUT_DIR, "fight_data_raw.csv")
OUT_ENRICHED   <- file.path(OUT_DIR, "fight_data_raw_enriched.csv")
ERR_EVENTS     <- file.path(OUT_DIR, "parse_errors_events.csv")
ERR_FIGHTS     <- file.path(OUT_DIR, "parse_errors_fights.csv")

EVENT_BATCH_LIMIT  <- Inf
FIGHT_BATCH_LIMIT  <- Inf
EVENT_DELAY_SEC    <- c(0.5, 1.25)
FIGHT_BATCH_PAUSE  <- c(0.5, 1.25)
MAX_EVENT_RETRIES  <- 6
MAX_FIGHT_RETRIES  <- 5
STALE_AFTER_DAYS   <- Inf
POLITE_WORKERS     <- min(max(1, parallelly::availableCores() - 1), 4)

EVENT_UA <- "UFC stats research scraper (canonical event cache)"
FIGHT_UA <- "UFC stats research scraper (canonical fight cache)"

dir.create(EVENT_CACHE, recursive = TRUE, showWarnings = FALSE)
dir.create(FIGHT_CACHE, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

abs_url <- function(link, base = BASE_URL) {
  tryCatch(xml2::url_absolute(link, base = base), error = function(e) link)
}

event_id_from_url <- function(u) sub(".*/event-details/([0-9A-Fa-f]{16}).*", "\\1", u)
fight_id_from_url <- function(u) sub(".*/fight-details/([0-9A-Fa-f]{16}).*", "\\1", u)
fight_id_from_path <- function(path) sub("\\.html$", "", basename(path), ignore.case = TRUE)

save_bin <- function(raw, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  writeBin(raw, path)
}

now_chr <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")

read_csv_if_exists <- function(path, col_types = NULL) {
  if (!file.exists(path)) return(tibble())
  if (is.null(col_types)) {
    read_csv(path, show_col_types = FALSE)
  } else {
    read_csv(path, col_types = col_types, show_col_types = FALSE)
  }
}

ensure_cols <- function(df, defaults) {
  for (nm in names(defaults)) {
    if (!nm %in% names(df)) df[[nm]] <- defaults[[nm]]
  }
  df %>% select(any_of(names(defaults)), everything())
}

manifest_needs_fetch <- function(path, fetch_ok, fetched_at, stale_after_days = Inf) {
  fetch_ok <- dplyr::coalesce(as.logical(fetch_ok), FALSE)
  path_missing <- !file.exists(path)
  if (!is.finite(stale_after_days)) {
    return(path_missing | !fetch_ok)
  }
  fetched_time <- suppressWarnings(ymd_hms(fetched_at, tz = Sys.timezone()))
  stale <- is.na(fetched_time) | fetched_time < (Sys.time() - days(stale_after_days))
  path_missing | !fetch_ok | stale
}

merge_manifests <- function(old, new, key) {
  if (nrow(old) == 0) return(new)
  old %>%
    full_join(new, by = key, suffix = c("_old", "")) %>%
    mutate(
      across(
        ends_with("_old"),
        ~ .x
      )
    ) %>%
    {
      out <- .
      old_cols <- names(out)[endsWith(names(out), "_old")]
      for (old_col in old_cols) {
        base_col <- sub("_old$", "", old_col)
        if (base_col %in% names(out)) {
          out[[base_col]] <- dplyr::coalesce(out[[base_col]], out[[old_col]])
        } else {
          names(out)[names(out) == old_col] <- base_col
        }
      }
      out %>% select(-any_of(old_cols))
    } %>%
    distinct(.data[[key]], .keep_all = TRUE)
}

coerce_event_manifest <- function(df) {
  defaults <- list(
    event_id = character(),
    event_url = character(),
    path = character(),
    last_seen_at = character(),
    fetched_at = character(),
    fetch_ok = logical(),
    status_code = numeric(),
    content_type = character(),
    bytes = numeric(),
    error_msg = character(),
    parsed_at = character()
  )
  ensure_cols(df, defaults) %>%
    mutate(
      across(c(event_id, event_url, path, last_seen_at, fetched_at, content_type, error_msg, parsed_at), as.character),
      fetch_ok = as.logical(fetch_ok),
      status_code = suppressWarnings(as.numeric(status_code)),
      bytes = suppressWarnings(as.numeric(bytes))
    ) %>%
    distinct(event_id, .keep_all = TRUE)
}

coerce_fight_manifest <- function(df) {
  defaults <- list(
    fight_id = character(),
    fight_url = character(),
    event_id = character(),
    path = character(),
    last_seen_at = character(),
    fetched_at = character(),
    fetch_ok = logical(),
    status_code = numeric(),
    content_type = character(),
    bytes = numeric(),
    error_msg = character(),
    parsed_at = character()
  )
  ensure_cols(df, defaults) %>%
    mutate(
      across(c(fight_id, fight_url, event_id, path, last_seen_at, fetched_at, content_type, error_msg, parsed_at), as.character),
      fetch_ok = as.logical(fetch_ok),
      status_code = suppressWarnings(as.numeric(status_code)),
      bytes = suppressWarnings(as.numeric(bytes))
    ) %>%
    distinct(fight_id, .keep_all = TRUE)
}

request_with_retry <- function(url, user_agent, max_tries) {
  request(url) %>%
    req_user_agent(user_agent) %>%
    req_timeout(30) %>%
    req_retry(
      max_tries = max_tries,
      is_transient = \(resp) resp_status(resp) %in% c(429, 500, 502, 503, 504),
      backoff = ~ 1 * 2^tries
    )
}

fetch_one_page <- function(url, path, user_agent, max_tries) {
  status <- NA_real_
  ctype <- NA_character_
  bytes <- NA_real_
  err <- NA_character_
  ok <- FALSE

  tryCatch({
    resp <- request_with_retry(url, user_agent, max_tries) %>% req_perform()
    status <- resp_status(resp)
    ctype <- resp_content_type(resp)

    if (status == 200 && grepl("text/html", ctype, fixed = TRUE)) {
      raw <- resp_body_raw(resp)
      save_bin(raw, path)
      bytes <- length(raw)
      ok <- TRUE
    } else {
      err <- sprintf("status=%s ctype=%s", status, ctype %||% NA_character_)
    }
  }, error = function(e) {
    err <<- conditionMessage(e)
  })

  tibble(
    fetched_at = now_chr(),
    fetch_ok = ok,
    status_code = status,
    content_type = ctype,
    bytes = bytes,
    error_msg = err
  )
}

fetch_in_batches <- function(df, url_col, path_col, user_agent, max_tries, batch_pause, workers) {
  if (nrow(df) == 0) return(tibble())

  old_plan <- future::plan()
  on.exit(future::plan(old_plan), add = TRUE)
  future::plan(future::multisession, workers = workers)

  batches <- split(seq_len(nrow(df)), ceiling(seq_len(nrow(df)) / workers))
  results <- vector("list", length(batches))

  for (i in seq_along(batches)) {
    idx <- batches[[i]]
    chunk <- df[idx, , drop = FALSE]

    results[[i]] <- furrr::future_map2_dfr(
      chunk[[url_col]],
      chunk[[path_col]],
      function(u, p) {
        Sys.sleep(runif(1, 0.10, 0.35))
        fetch_one_page(u, p, user_agent = user_agent, max_tries = max_tries)
      },
      .options = furrr::furrr_options(seed = TRUE)
    )

    if (i < length(batches)) {
      Sys.sleep(runif(1, batch_pause[1], batch_pause[2]))
    }
  }

  bind_rows(results)
}

squish_na <- function(x) {
  x <- str_squish(x)
  na_if(x, "")
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

extract_event_location <- function(doc) {
  extract_labeled_value(doc, "Location")
}

extract_fight_urls <- function(doc) {
  html_elements(doc, 'a[href*="fight-details/"]') %>%
    html_attr("href") %>%
    discard(is.na) %>%
    map_chr(abs_url) %>%
    unique()
}

parse_event_file <- function(event_id, event_url, path) {
  doc <- read_html(path)
  title_raw <- extract_event_title(doc)
  date_raw <- extract_event_date(doc)
  loc_raw <- extract_event_location(doc)
  date_parsed <- suppressWarnings(mdy(date_raw))
  date_iso <- ifelse(is.na(date_parsed), NA_character_, as.character(date_parsed))

  event_row <- tibble(
    event_id = as.character(event_id),
    event_url = as.character(event_url),
    event_title = squish_na(title_raw),
    event_date_txt = squish_na(date_raw),
    event_date = date_iso,
    event_location = squish_na(loc_raw)
  )

  fight_urls <- extract_fight_urls(doc)
  map_row <- if (length(fight_urls) > 0) {
    fight_ids <- fight_id_from_url(fight_urls)
    keep <- !is.na(fight_ids) & nzchar(fight_ids)
    tibble(
      event_id = as.character(event_id),
      event_url = as.character(event_url),
      fight_id = as.character(fight_ids[keep]),
      fight_url = as.character(fight_urls[keep])
    ) %>%
      distinct(event_id, fight_id, .keep_all = TRUE)
  } else {
    tibble(
      event_id = character(),
      event_url = character(),
      fight_id = character(),
      fight_url = character()
    )[0, ]
  }

  list(event_row = event_row, map_row = map_row)
}

# Numeric stat columns that must never flip types
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

CLOCK_COLS <- c("time", "fighter_1_Ctrl", "fighter_2_Ctrl")

coerce_stats <- function(df) {
  df %>%
    mutate(
      across(any_of(STAT_COLS), ~ {
        x <- as.character(.x)
        x <- str_replace_all(x, "â€”|â€“|--", "")
        x <- str_squish(x)
        x <- na_if(x, "")
        suppressWarnings(as.numeric(x))
      })
    )
}

coerce_clocks <- function(df) {
  df %>%
    mutate(
      across(any_of(CLOCK_COLS), ~ {
        x <- as.character(.x)
        x <- str_squish(x)
        x <- na_if(x, "")
        x
      })
    )
}

coerce_fight_schema <- function(df) {
  df %>%
    coerce_stats() %>%
    coerce_clocks()
}

parse_one_fight <- function(path) {
  fight_id <- fight_id_from_path(path)
  doc <- read_html(path)

  tbl_nodes <- html_nodes(doc, "table")
  if (length(tbl_nodes) < 1) stop("No <table> nodes found")

  tbl_list <- html_table(tbl_nodes[1], trim = TRUE, fill = TRUE)
  if (length(tbl_list) == 0 || nrow(tbl_list[[1]]) == 0) stop("First table empty")

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

  fighter_hrefs <- fighter_anchors %>%
    html_attr("href") %>%
    str_squish() %>%
    head(2)

  fighter_ids <- map_chr(
    fighter_hrefs,
    function(x) {
      if (is.na(x) || !nzchar(x)) {
        NA_character_
      } else {
        sub(".*/fighter-details/([^/?#]+).*", "\\1", x)
      }
    }
  )
  fighter_ids <- c(fighter_ids, NA_character_, NA_character_)[1:2]

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

  statuses <- doc %>%
    html_elements(".b-fight-details__persons .b-fight-details__person .b-fight-details__person-status") %>%
    html_text2() %>%
    str_squish() %>%
    toupper()
  statuses <- c(statuses, NA_character_, NA_character_)[1:2]

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

  title_text <- doc %>%
    html_nodes(".b-fight-details__fight-title") %>%
    html_text() %>%
    str_replace_all("\n", "") %>%
    str_trim()

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

parse_fights_safely <- function(paths) {
  safe_parse <- safely(parse_one_fight, otherwise = NULL)
  res <- map(paths, safe_parse)

  parsed <- compact(map(res, "result"))
  errs <- tibble(
    path = paths,
    fight_id = fight_id_from_path(paths),
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

# -------------------------------------------------------------------
# 1) Discover all event cards from the completed events index
# -------------------------------------------------------------------
message("Discovering event cards from UFCStats index...")

cards <- read_html(EVENTS_INDEX) %>%
  html_elements("a.b-link_style_black") %>%
  html_attr("href") %>%
  discard(is.na) %>%
  map_chr(abs_url) %>%
  keep(~ grepl("/event-details/[0-9A-Fa-f]{16}", .x, perl = TRUE)) %>%
  unique()

if (!is.finite(EVENT_BATCH_LIMIT)) EVENT_BATCH_LIMIT <- length(cards)
cards <- head(cards, EVENT_BATCH_LIMIT)

if (length(cards) == 0) {
  stop("No event card links found at: ", EVENTS_INDEX)
}

event_manifest_old <- read_csv_if_exists(EVENT_MANIFEST) %>% coerce_event_manifest()
event_manifest_new <- tibble(
  event_id = event_id_from_url(cards),
  event_url = cards,
  path = file.path(EVENT_CACHE, paste0(event_id, ".html")),
  last_seen_at = now_chr(),
  fetched_at = NA_character_,
  fetch_ok = NA,
  status_code = NA_real_,
  content_type = NA_character_,
  bytes = NA_real_,
  error_msg = NA_character_,
  parsed_at = NA_character_
) %>%
  filter(!is.na(event_id), nzchar(event_id)) %>%
  distinct(event_id, .keep_all = TRUE)

event_manifest <- merge_manifests(event_manifest_old, event_manifest_new, "event_id") %>%
  coerce_event_manifest()

event_manifest <- event_manifest %>%
  mutate(needs_fetch = manifest_needs_fetch(path, fetch_ok, fetched_at, stale_after_days = STALE_AFTER_DAYS))

need_events <- event_manifest %>%
  filter(needs_fetch)

message("Event cards indexed : ", nrow(event_manifest))
message("Event cards to fetch: ", nrow(need_events))

if (nrow(need_events) > 0) {
  event_fetches <- vector("list", nrow(need_events))

  for (i in seq_len(nrow(need_events))) {
    Sys.sleep(runif(1, EVENT_DELAY_SEC[1], EVENT_DELAY_SEC[2]))
    event_fetches[[i]] <- fetch_one_page(
      need_events$event_url[i],
      need_events$path[i],
      user_agent = EVENT_UA,
      max_tries = MAX_EVENT_RETRIES
    )
    message(sprintf("[%d/%d] event %s fetched=%s", i, nrow(need_events), need_events$event_id[i], if (event_fetches[[i]]$fetch_ok[1]) "yes" else "no"))
  }

  event_fetches <- bind_rows(event_fetches) %>%
    bind_cols(need_events %>% select(event_id))

  event_manifest <- event_manifest %>%
    select(-needs_fetch) %>%
    left_join(event_fetches, by = "event_id", suffix = c("", "_new")) %>%
    mutate(
      fetched_at = coalesce(fetched_at_new, fetched_at),
      fetch_ok = coalesce(fetch_ok_new, fetch_ok),
      status_code = coalesce(status_code_new, status_code),
      content_type = coalesce(content_type_new, content_type),
      bytes = coalesce(bytes_new, bytes),
      error_msg = coalesce(error_msg_new, error_msg)
    ) %>%
    select(-ends_with("_new"))
} else {
  event_manifest <- event_manifest %>% select(-needs_fetch)
}

write_csv(event_manifest, EVENT_MANIFEST)

# -------------------------------------------------------------------
# 2) Parse cached event cards into event metadata and fight map
# -------------------------------------------------------------------
message("Parsing cached event cards...")

cached_events <- event_manifest %>%
  filter(file.exists(path)) %>%
  distinct(event_id, .keep_all = TRUE)

event_parse_res <- map(
  seq_len(nrow(cached_events)),
  function(i) {
    tryCatch(
      parse_event_file(
        event_id = cached_events$event_id[i],
        event_url = cached_events$event_url[i],
        path = cached_events$path[i]
      ),
      error = function(e) {
        list(
          event_row = NULL,
          map_row = NULL,
          error = tibble(
            event_id = cached_events$event_id[i],
            event_url = cached_events$event_url[i],
            path = cached_events$path[i],
            error = conditionMessage(e)
          )
        )
      }
    )
  }
)

event_errors <- compact(map(event_parse_res, "error"))
if (length(event_errors) > 0) {
  write_csv(bind_rows(event_errors) %>% distinct(event_id, .keep_all = TRUE), ERR_EVENTS)
} else if (file.exists(ERR_EVENTS)) {
  file.remove(ERR_EVENTS)
}

events_tbl <- compact(map(event_parse_res, "event_row")) %>%
  bind_rows() %>%
  distinct(event_id, .keep_all = TRUE)

map_tbl <- compact(map(event_parse_res, "map_row")) %>%
  bind_rows() %>%
  distinct(event_id, fight_id, .keep_all = TRUE)

write_csv(events_tbl, OUT_EVENTS)
write_csv(map_tbl, OUT_MAP)

event_manifest <- event_manifest %>%
  left_join(events_tbl %>% transmute(event_id, parsed_at_new = now_chr()), by = "event_id") %>%
  mutate(parsed_at = coalesce(parsed_at_new, parsed_at)) %>%
  select(-parsed_at_new)

write_csv(event_manifest, EVENT_MANIFEST)

message("Parsed events      : ", nrow(events_tbl))
message("Parsed fight links : ", nrow(map_tbl))

# -------------------------------------------------------------------
# 3) Build or update fight manifest from parsed event cards
# -------------------------------------------------------------------
fight_manifest_old <- read_csv_if_exists(FIGHT_MANIFEST) %>% coerce_fight_manifest()
fight_manifest_new <- map_tbl %>%
  transmute(
    fight_id = as.character(fight_id),
    fight_url = as.character(fight_url),
    event_id = as.character(event_id),
    path = file.path(FIGHT_CACHE, paste0(fight_id, ".html")),
    last_seen_at = now_chr(),
    fetched_at = NA_character_,
    fetch_ok = NA,
    status_code = NA_real_,
    content_type = NA_character_,
    bytes = NA_real_,
    error_msg = NA_character_,
    parsed_at = NA_character_
  ) %>%
  distinct(fight_id, .keep_all = TRUE)

fight_manifest <- merge_manifests(fight_manifest_old, fight_manifest_new, "fight_id") %>%
  coerce_fight_manifest() %>%
  mutate(needs_fetch = manifest_needs_fetch(path, fetch_ok, fetched_at, stale_after_days = STALE_AFTER_DAYS))

need_fights <- fight_manifest %>%
  filter(needs_fetch)

if (!is.finite(FIGHT_BATCH_LIMIT)) FIGHT_BATCH_LIMIT <- nrow(need_fights)
need_fights <- head(need_fights, FIGHT_BATCH_LIMIT)

message("Fight pages indexed : ", nrow(fight_manifest))
message("Fight pages to fetch: ", nrow(need_fights))

if (nrow(need_fights) > 0) {
  fight_fetches <- fetch_in_batches(
    need_fights,
    url_col = "fight_url",
    path_col = "path",
    user_agent = FIGHT_UA,
    max_tries = MAX_FIGHT_RETRIES,
    batch_pause = FIGHT_BATCH_PAUSE,
    workers = POLITE_WORKERS
  ) %>%
    bind_cols(need_fights %>% select(fight_id))

  fight_manifest <- fight_manifest %>%
    select(-needs_fetch) %>%
    left_join(fight_fetches, by = "fight_id", suffix = c("", "_new")) %>%
    mutate(
      fetched_at = coalesce(fetched_at_new, fetched_at),
      fetch_ok = coalesce(fetch_ok_new, fetch_ok),
      status_code = coalesce(status_code_new, status_code),
      content_type = coalesce(content_type_new, content_type),
      bytes = coalesce(bytes_new, bytes),
      error_msg = coalesce(error_msg_new, error_msg)
    ) %>%
    select(-ends_with("_new"))
} else {
  fight_fetches <- tibble(fight_id = character())
  fight_manifest <- fight_manifest %>% select(-needs_fetch)
}

write_csv(fight_manifest, FIGHT_MANIFEST)

# -------------------------------------------------------------------
# 4) Parse new or refreshed fight pages into raw fight data
# -------------------------------------------------------------------
existing_fight_raw <- read_csv_if_exists(OUT_FIGHTS_RAW) %>% coerce_fight_schema()
already_parsed_fights <- if ("fight_id" %in% names(existing_fight_raw)) unique(existing_fight_raw$fight_id) else character()
refetched_ids <- if (nrow(fight_fetches) > 0) fight_fetches$fight_id[fight_fetches$fetch_ok %in% TRUE] else character()

fight_parse_queue <- fight_manifest %>%
  filter(file.exists(path)) %>%
  filter(!(fight_id %in% already_parsed_fights) | fight_id %in% refetched_ids)

message("Fight pages to parse: ", nrow(fight_parse_queue))

fight_parsed <- parse_fights_safely(fight_parse_queue$path)

if (nrow(fight_parsed$errors) > 0) {
  prior_errs <- read_csv_if_exists(ERR_FIGHTS)
  out_errs <- bind_rows(prior_errs, fight_parsed$errors) %>%
    distinct(fight_id, .keep_all = TRUE)
  write_csv(out_errs, ERR_FIGHTS)
}

new_fight_raw <- fight_parsed$parsed
fight_raw_all <- if (nrow(existing_fight_raw) > 0 && nrow(new_fight_raw) > 0) {
  bind_rows(new_fight_raw, existing_fight_raw) %>%
    distinct(fight_id, .keep_all = TRUE) %>%
    coerce_fight_schema()
} else if (nrow(new_fight_raw) > 0) {
  new_fight_raw %>% coerce_fight_schema()
} else {
  existing_fight_raw %>% coerce_fight_schema()
}

write_csv(fight_raw_all, OUT_FIGHTS_RAW)

fight_manifest <- fight_manifest %>%
  left_join(
    tibble(fight_id = unique(new_fight_raw[["fight_id"]] %||% character()), parsed_at_new = now_chr()),
    by = "fight_id"
  ) %>%
  mutate(parsed_at = coalesce(parsed_at_new, parsed_at)) %>%
  select(-parsed_at_new)

write_csv(fight_manifest, FIGHT_MANIFEST)

# -------------------------------------------------------------------
# 5) Rebuild enriched raw fight data from raw fights + current event map
# -------------------------------------------------------------------
event_info <- map_tbl %>%
  distinct(fight_id, event_id) %>%
  left_join(
    events_tbl %>% select(event_id, event_title, event_date, event_location),
    by = "event_id"
  ) %>%
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

message("Done.")
message("Events manifest   : ", EVENT_MANIFEST)
message("Fights manifest   : ", FIGHT_MANIFEST)
message("Event metadata    : ", OUT_EVENTS)
message("Event-fight map   : ", OUT_MAP)
message("Fight raw         : ", OUT_FIGHTS_RAW)
message("Fight enriched    : ", OUT_ENRICHED)
