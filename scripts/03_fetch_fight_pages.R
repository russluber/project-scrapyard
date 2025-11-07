# fetch_fight_pages - sequential + extra politeness

suppressPackageStartupMessages({
  library(rvest)
  library(tidyverse)
  library(here)
  library(httr2)
  library(readr)
})

EVENTS_URL  <- "http://ufcstats.com/statistics/events/completed?page=all"
CACHE_DIR   <- here("cache", "fights")
MANIFEST    <- here("data/raw", "fights_manifest.csv")

# knobs: shrink if you just want to test quickly
MAX_CARDS    <- Inf        # number of event cards to scan (Inf = all)
BATCH_LIMIT  <- Inf       # max fights to fetch this run (Inf = all)

# Politeness settings
DELAY_SEC    <- c(0.5, 1.5)   # per-request jitter; ~0.8–2 req/s avg
COFFEE_EVERY <- 50            # take a longer break every N pages
COFFEE_WAIT  <- c(5, 10)      # seconds for the longer break

dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(MANIFEST), recursive = TRUE, showWarnings = FALSE)

# helpers
abs_url <- function(link, base = "http://ufcstats.com") {
  tryCatch(xml2::url_absolute(link, base = base), error = function(e) link)
}
fight_id_from_url <- function(u) sub(".*/fight-details/([0-9A-Fa-f]{16}).*", "\\1", u)
save_bin <- function(raw, path) { dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE); writeBin(raw, path) }

# 1) get event card URLs
cards <- read_html(EVENTS_URL) |>
  html_nodes(".b-link_style_black") |>
  html_attr("href") |>
  discard(is.na) |>
  map_chr(abs_url)

if (!is.finite(MAX_CARDS)) MAX_CARDS <- length(cards)
cards <- head(cards, MAX_CARDS)

# 2) collect fight links from cards
get_fights <- function(card_url) {
  tryCatch({
    read_html(card_url) |>
      html_nodes("a") |>
      html_attr("href") |>
      discard(is.na) |>
      map_chr(abs_url) |>
      keep(~ grepl("/fight-details/[0-9A-Fa-f]{16}", .x))
  }, error = function(e) character())
}

fight_urls <- flatten_chr(map(cards, get_fights)) |> unique()

if (!is.finite(BATCH_LIMIT)) BATCH_LIMIT <- length(fight_urls)
fight_urls <- head(fight_urls, BATCH_LIMIT)

# 3) make a simple manifest tibble
df <- tibble(
  fights   = fight_urls,
  fight_id = fight_id_from_url(fight_urls),
  path     = file.path(CACHE_DIR, paste0(fight_id, ".html"))
) |>
  filter(!is.na(fight_id), nzchar(fight_id)) |>
  distinct(fight_id, .keep_all = TRUE)

# write or update manifest (very simple)
if (file.exists(MANIFEST)) {
  old <- read_csv(MANIFEST, show_col_types = FALSE)
  df  <- old |> bind_rows(df) |> distinct(fight_id, .keep_all = TRUE)
}
write_csv(df, MANIFEST)

# 4) fetch any missing files sequentially (extra polite)
need <- df |> filter(!file.exists(path))
message("Need to fetch: ", nrow(need), " fights")

for (i in seq_len(nrow(need))) {
  u <- need$fights[i]; p <- need$path[i]
  
  # polite jitter before each request
  Sys.sleep(runif(1, DELAY_SEC[1], DELAY_SEC[2]))
  
  # take a longer break every COFFEE_EVERY pages
  if (i %% COFFEE_EVERY == 0L) {
    pause <- runif(1, COFFEE_WAIT[1], COFFEE_WAIT[2])
    message(sprintf("Coffee break: sleeping ~%.1fs before continuing…", pause))
    Sys.sleep(pause)
  }
  
  req <- request(u) |>
    req_user_agent("UFC stats research scraper (cache fetch, sequential)") |>
    # optional: add a contact so site admins can reach you if needed
    # req_headers(`From` = "youremail@example.com") |>
    req_timeout(30) |>
    req_retry(
      max_tries = 6,
      is_transient = \(resp) resp_status(resp) %in% c(429, 500, 502, 503, 504),
      backoff = ~ 1 * 2^tries   # 1, 2, 4, 8, 16, 32 seconds
    )
  
  ok <- FALSE; status <- NA_integer_; ctype <- NA_character_; bytes <- NA_integer_
  
  try({
    resp   <- req_perform(req)
    status <- resp_status(resp)
    ctype  <- resp_content_type(resp)
    
    if (status == 200 && grepl("text/html", ctype, fixed = TRUE)) {
      raw <- resp_body_raw(resp)
      save_bin(raw, p)
      bytes <- length(raw)
      ok <- TRUE
    } else if (status %in% c(429, 503)) {
      # be extra courteous on busy signals even after retries
      extra <- runif(1, 10, 20)
      message(sprintf("Server busy (status=%s). Extra sleep ~%.1fs.", status, extra))
      Sys.sleep(extra)
    }
  }, silent = TRUE)
  
  message(sprintf("[%d/%d] %s  status=%s  saved=%s",
                  i, nrow(need), basename(p), status, if (ok) "yes" else "no"))
}

message("Done. Cached files live in: ", CACHE_DIR, "\nManifest: ", MANIFEST)
