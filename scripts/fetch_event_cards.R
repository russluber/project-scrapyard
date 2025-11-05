# fetch_event_cards.R — cache all UFC event card pages (sequential + polite)

suppressPackageStartupMessages({
  library(rvest)
  library(tidyverse)
  library(here)
  library(httr2)
  library(readr)
})

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
EVENTS_INDEX <- "http://ufcstats.com/statistics/events/completed?page=all"

CACHE_DIR    <- here("cache", "events")
MANIFEST     <- here("data",  "events_manifest.csv")

# knobs
BATCH_LIMIT  <- Inf           # max event cards to fetch this run
DELAY_SEC    <- c(0.5, 1.5)   # polite jitter per request
COFFEE_EVERY <- 75            # longer pause every N pages
COFFEE_WAIT  <- c(5, 12)      # seconds for the longer pause

dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(MANIFEST), recursive = TRUE, showWarnings = FALSE)

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
abs_url <- function(link, base = "http://ufcstats.com") {
  tryCatch(xml2::url_absolute(link, base = base), error = function(e) link)
}
event_id_from_url <- function(u) sub(".*/event-details/([0-9A-Fa-f]{16}).*", "\\1", u)
save_bin <- function(raw, path) { dir.create(dirname(path), TRUE, TRUE); writeBin(raw, path) }

# -------------------------------------------------------------------
# 1) Discover event card URLs from the index
# -------------------------------------------------------------------
cards <- read_html(EVENTS_INDEX) |>
  html_elements("a.b-link_style_black") |>
  html_attr("href") |>
  discard(is.na) |>
  map_chr(abs_url) |>
  keep(~ grepl("/event-details/[0-9A-Fa-f]{16}", .x, perl = TRUE)) |>
  unique()

if (!is.finite(BATCH_LIMIT)) BATCH_LIMIT <- length(cards)
cards <- head(cards, BATCH_LIMIT)

if (length(cards) == 0) {
  stop("No event card links found at: ", EVENTS_INDEX)
}

# -------------------------------------------------------------------
# 2) Build or update manifest
# -------------------------------------------------------------------
df <- tibble(
  event_url = cards,
  event_id  = event_id_from_url(cards),
  path      = file.path(CACHE_DIR, paste0(event_id, ".html"))
) |>
  filter(!is.na(event_id), nzchar(event_id)) |>
  distinct(event_id, .keep_all = TRUE)

if (file.exists(MANIFEST)) {
  old <- read_csv(MANIFEST, show_col_types = FALSE)
  df  <- bind_rows(old, df) |> distinct(event_id, .keep_all = TRUE)
}
write_csv(df, MANIFEST)

# -------------------------------------------------------------------
# 3) Fetch any missing event pages (sequential, with retries)
# -------------------------------------------------------------------
need <- df |> filter(!file.exists(path))
message("Need to fetch: ", nrow(need), " event cards")

for (i in seq_len(nrow(need))) {
  u <- need$event_url[i]; p <- need$path[i]
  
  # polite jitter
  Sys.sleep(runif(1, DELAY_SEC[1], DELAY_SEC[2]))
  
  # occasional longer pause
  if (i %% COFFEE_EVERY == 0L) {
    pause <- runif(1, COFFEE_WAIT[1], COFFEE_WAIT[2])
    message(sprintf("Coffee break (~%.1fs)…", pause))
    Sys.sleep(pause)
  }
  
  req <- request(u) |>
    req_user_agent("UFC stats research scraper (event cards, cache)") |>
    req_timeout(30) |>
    req_retry(
      max_tries = 6,
      is_transient = \(resp) resp_status(resp) %in% c(429, 500, 502, 503, 504),
      backoff = ~ 1 * 2^tries  # 1,2,4,8,16,32
    )
  
  ok <- FALSE; status <- NA_integer_; ctype <- NA_character_
  
  try({
    resp   <- req_perform(req)
    status <- resp_status(resp)
    ctype  <- resp_content_type(resp)
    
    if (status == 200 && grepl("text/html", ctype, fixed = TRUE)) {
      raw <- resp_body_raw(resp)
      save_bin(raw, p)
      ok <- TRUE
    } else if (status %in% c(429, 503)) {
      extra <- runif(1, 10, 20)
      message(sprintf("Server busy (status=%s). Extra sleep ~%.1fs.", status, extra))
      Sys.sleep(extra)
    }
  }, silent = TRUE)
  
  message(sprintf("[%d/%d] %s  status=%s  saved=%s",
                  i, nrow(need), basename(p), status, if (ok) "yes" else "no"))
}

message("Done. Cached event cards in: ", CACHE_DIR, "\nManifest: ", MANIFEST)
