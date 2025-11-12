# fetch_fighter_pages.R  — sequential, polite cache-first fetcher for fighter pages
# Discovers fighter-detail URLs from A–Z master pages and caches raw HTML locally.
# Output: data/raw/fighters_manifest.csv mapping fighter_id → url → local path

suppressPackageStartupMessages({
  library(rvest)
  library(tidyverse)
  library(here)
  library(httr2)
  library(readr)
})

# --------------------------- Config -----------------------------------
BASE          <- "http://ufcstats.com"
MASTER_URL    <- function(letter) paste0(BASE, "/statistics/fighters?char=", letter, "&page=all")

CACHE_DIR     <- here("cache", "fighters")
MANIFEST      <- here("data", "raw", "fighters_manifest.csv")

# knobs (shrink for quick tests)
LETTERS_TO_SCRAPE <- letters          # a..z
MAX_LETTERS       <- Inf              # Inf = all letters; else subset head(LETTERS_TO_SCRAPE, MAX_LETTERS)
BATCH_LIMIT       <- Inf              # Inf = fetch all missing; else stop after N fighters

# politeness
DELAY_SEC    <- c(0.5, 1.5)           # per-request jitter (~0.8–2 req/s avg)
COFFEE_EVERY <- 50                    # longer break every N requests
COFFEE_WAIT  <- c(5, 10)              # seconds for longer break

# request behavior
TIMEOUT_SEC  <- 30
RETRY_STAT   <- c(429, 500, 502, 503, 504)

# ensure dirs
dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(MANIFEST), recursive = TRUE, showWarnings = FALSE)

# --------------------------- Helpers ----------------------------------
abs_url <- function(link, base = BASE) {
  tryCatch(xml2::url_absolute(link, base = base), error = function(e) link)
}

fighter_id_from_url <- function(u) sub(".*/fighter-details/([0-9A-Fa-f]{16}).*", "\\1", u)

save_bin <- function(raw, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  writeBin(raw, path)
}

# Extract fighter-detail links from one master page (a given letter)
get_fighters_from_letter <- function(letter) {
  url <- MASTER_URL(letter)
  # be resilient to occasional network flakiness
  doc <- try(read_html(url), silent = TRUE)
  if (inherits(doc, "try-error")) return(tibble(letter = character(), fighter_url = character()))
  
  # Any of the first 3 columns usually link to /fighter-details/{id}
  links <- doc |>
    html_elements("table.b-statistics__table tbody tr.b-statistics__table-row td:nth-child(-n+3) a[href]") |>
    html_attr("href") |>
    discard(is.na) |>
    map_chr(abs_url) |>
    keep(~ grepl("/fighter-details/[0-9A-Fa-f]{16}", .x))
  
  tibble(letter = letter, fighter_url = unique(links))
}

looks_like_html <- function(ct) is.character(ct) && any(grepl("text/html", ct, fixed = TRUE))

# --------------------- 1) Discover fighter URLs -----------------------
letters_vec <- LETTERS_TO_SCRAPE
if (!is.finite(MAX_LETTERS)) MAX_LETTERS <- length(letters_vec)
letters_vec <- head(letters_vec, MAX_LETTERS)

message("Scanning fighter master pages for letters: ", paste(letters_vec, collapse = ", "))

fighters_tbl <- map_dfr(letters_vec, get_fighters_from_letter) |>
  mutate(fighter_id = fighter_id_from_url(fighter_url)) |>
  filter(!is.na(fighter_id), nzchar(fighter_id)) |>
  distinct(fighter_id, .keep_all = TRUE) |>
  mutate(path = file.path(CACHE_DIR, paste0(fighter_id, ".html")))

if (nrow(fighters_tbl) == 0) {
  stop("No fighter URLs discovered from master pages.")
}

# --------------------- 2) Build / update manifest ---------------------
man_new <- fighters_tbl |>
  transmute(
    fighter_id,
    fighter_url,
    path,
    letter
  )

if (file.exists(MANIFEST)) {
  man_old <- read_csv(MANIFEST, show_col_types = FALSE)
  man_all <- bind_rows(man_old, man_new) |>
    arrange(fighter_id) |>
    distinct(fighter_id, .keep_all = TRUE)
} else {
  man_all <- man_new
}

write_csv(man_all, MANIFEST)
message("Manifest written: ", MANIFEST, " (", nrow(man_all), " fighters indexed)")

# --------------------- 3) Fetch missing files -------------------------
need <- man_all |>
  filter(!file.exists(path))

if (!is.finite(BATCH_LIMIT)) BATCH_LIMIT <- nrow(need)
need <- head(need, BATCH_LIMIT)

message("Need to fetch: ", nrow(need), " fighters")

for (i in seq_len(nrow(need))) {
  u <- need$fighter_url[i]
  p <- need$path[i]
  
  # per-request polite jitter
  Sys.sleep(runif(1, DELAY_SEC[1], DELAY_SEC[2]))
  
  # periodic longer break
  if (i %% COFFEE_EVERY == 0L) {
    pause <- runif(1, COFFEE_WAIT[1], COFFEE_WAIT[2])
    message(sprintf("Coffee break: sleeping ~%.1fs before continuing…", pause))
    Sys.sleep(pause)
  }
  
  req <- request(u) |>
    req_user_agent("UFC stats research scraper (fighters cache, sequential)") |>
    req_timeout(TIMEOUT_SEC) |>
    req_retry(
      max_tries = 6,
      is_transient = \(resp) resp_status(resp) %in% RETRY_STAT,
      backoff = ~ 1 * 2^tries  # 1,2,4,8,16,32 sec
    )
  
  ok <- FALSE; status <- NA_integer_; ctype <- NA_character_; bytes <- NA_integer_
  
  try({
    resp   <- req_perform(req)
    status <- resp_status(resp)
    ctype  <- resp_content_type(resp)
    
    if (status == 200 && looks_like_html(ctype)) {
      raw <- resp_body_raw(resp)
      save_bin(raw, p)
      bytes <- length(raw)
      ok <- TRUE
    } else if (status %in% c(429, 503)) {
      extra <- runif(1, 10, 20)
      message(sprintf("Server busy (status=%s). Extra sleep ~%.1fs.", status, extra))
      Sys.sleep(extra)
    }
  }, silent = TRUE)
  
  message(sprintf("[%d/%d] %s  status=%s  saved=%s",
                  i, nrow(need), paste0(basename(p)), status, if (ok) "yes" else "no"))
}

message("Done. Cached fighter HTML lives in: ", CACHE_DIR, "\nManifest: ", MANIFEST)
