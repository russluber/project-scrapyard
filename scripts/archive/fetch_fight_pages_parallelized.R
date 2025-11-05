# fetch_fight_pages.R
suppressPackageStartupMessages({
  library(rvest)
  library(tidyverse)
  library(here)
  library(httr2)
  library(furrr)
  library(parallelly)
})

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
CACHE_DIR   <- here("cache", "fights")
MANIFEST    <- here("data", "fights_manifest.csv")
EVENTS_URL  <- "http://ufcstats.com/statistics/events/completed?page=all"

# Run-time flags
DRY_RUN        <- TRUE              # if TRUE: only run preflight; no downloads
PREFLIGHT_N    <- 5                 # sample size for link inspection
BATCH_LIMIT    <- NA_integer_       # set to a number (e.g., 50) to test small batch
MAX_WORKERS    <- max(1, availableCores() - 1)
POLITE_WORKERS <- min(MAX_WORKERS, 4)   # do not exceed 4 for fetches

dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(MANIFEST), recursive = TRUE, showWarnings = FALSE)

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

fight_id_from_url <- function(u) sub(".*/fight-details/([0-9A-Fa-f]{16}).*", "\\1", u)

save_bin <- function(raw, path) {
  dir.create(dirname(path), TRUE, TRUE)
  writeBin(raw, path)
}

# Normalize possibly relative links to absolute
abs_url <- function(link, base = "http://ufcstats.com") {
  # xml2::url_absolute handles absolute inputs too
  tryCatch(xml2::url_absolute(link, base = base), error = function(e) link)
}

is_fight_url <- function(u) {
  is.character(u) && str_detect(u, "/fight-details/[0-9A-Fa-f]{16}")
}

peek_chr <- function(x, n = 5) {
  x <- x[!is.na(x) & nzchar(x)]
  unique(head(x, n))
}

# --------------- Parsers (network-bound; used only in preflight/manifest) -----
scrape_cards <- function(link) {
  read_html(link) |>
    html_nodes(".b-link_style_black") |>
    html_attr("href") |>
    (\(x) tibble(cards = x))()
}

scrape_fights <- function(link) {
  read_html(link) |>
    html_nodes("a") |>
    html_attr("href") |>
    (\(x) tibble(fights = x))() |>
    filter(str_detect(fights, "fight-details"))
}

# -------------------------------------------------------------------
# Preflight
# -------------------------------------------------------------------
preflight_check <- function(events_url, n = 5) {
  message("Preflight: fetching events page and inspecting selectors")
  
  # 1) cards
  cards <- tryCatch(scrape_cards(events_url), error = function(e) tibble(cards = character()))
  if (nrow(cards) == 0) stop("Preflight: no card links found at events page")
  
  cards$cards <- abs_url(cards$cards)
  message("Example cards [", min(n, nrow(cards)), "]:\n  ",
          paste0(peek_chr(cards$cards, n), collapse = "\n  "))
  
  # 2) fights from first card
  one_card <- cards$cards[[1]]
  fights_tbl <- tryCatch(scrape_fights(one_card), error = function(e) tibble(fights = character()))
  fights_tbl$fights <- abs_url(fights_tbl$fights)
  
  if (nrow(fights_tbl) == 0) stop("Preflight: no fight links found on first card")
  good_mask <- vapply(fights_tbl$fights, is_fight_url, logical(1))
  if (!any(good_mask)) stop("Preflight: found links but none look like fight-details URLs")
  
  message("Example fights [", min(n, sum(good_mask)), "]:\n  ",
          paste0(peek_chr(fights_tbl$fights[good_mask], n), collapse = "\n  "))
  
  # 3) quick count across first n cards
  total_fights <- cards$cards |>
    head(n) |>
    map_dfr(function(cu) {
      out <- tryCatch(scrape_fights(cu), error = function(e) tibble(fights = character()))
      out$fights <- abs_url(out$fights)
      out
    }) |>
    distinct(fights) |>
    nrow()
  
  message("Preflight: distinct fights found in first ", n, " cards = ", total_fights)
  invisible(list(cards = cards, sample_fights = fights_tbl))
}

# -------------------------------------------------------------------
# 1) Preflight
# -------------------------------------------------------------------
pf <- preflight_check(EVENTS_URL, n = PREFLIGHT_N)
if (isTRUE(DRY_RUN)) {
  message("Dry run is TRUE. Stopping after preflight.")
  quit(save = "no")
}

# -------------------------------------------------------------------
# 2) Build or update manifest of fight URLs
# -------------------------------------------------------------------
message("Building manifest…")

cards <- scrape_cards(EVENTS_URL) |>
  mutate(cards = abs_url(cards))

fights <- cards$cards |>
  map_dfr(function(c) tryCatch(scrape_fights(c), error = function(e) tibble(fights = character()))) |>
  mutate(
    fights   = abs_url(fights),
    fight_id = fight_id_from_url(fights)
  ) |>
  filter(!is.na(fight_id), nzchar(fight_id)) |>
  distinct(fight_id, .keep_all = TRUE) |>
  mutate(path = file.path(CACHE_DIR, paste0(fight_id, ".html")))

manifest <- if (file.exists(MANIFEST)) {
  read_csv(MANIFEST, show_col_types = FALSE) |>
    bind_rows(fights) |>
    distinct(fight_id, .keep_all = TRUE)
} else {
  fights
}

write_csv(manifest, MANIFEST)

# -------------------------------------------------------------------
# 3) Download missing HTMLs in parallel (retry + polite UA + jitter)
# -------------------------------------------------------------------
need <- manifest |>
  filter(!file.exists(path))

if (!is.na(BATCH_LIMIT)) {
  message("Batch limit active: taking first ", BATCH_LIMIT, " fights for this run")
  need <- need |> slice_head(n = BATCH_LIMIT)
}

if (nrow(need) == 0) {
  message("All fights already cached.")
  quit(save = "no")
}

message("Downloading ", nrow(need), " fights…")
plan(multisession, workers = POLITE_WORKERS)

results <- future_map2(
  need$fights, need$path,
  function(u, p) {
    # polite jitter to avoid thundering herd
    Sys.sleep(runif(1, 0.10, 0.30))
    
    req <- request(u) |>
      req_user_agent("UFC stats research scraper (cache fetch)") |>
      req_timeout(20) |>
      req_retry(max_tries = 5, backoff = ~ 0.5 * 2^tries)  # 0.5,1,2,4,8
    
    resp <- NULL
    ok <- FALSE
    bytes <- NA_integer_
    ctype <- NA_character_
    status <- NA_integer_
    etag <- NA_character_
    lm <- NA_character_
    err <- NA_character_
    
    try({
      resp   <- req_perform(req)
      status <- resp_status(resp)
      ctype  <- resp_content_type(resp)
      etag   <- resp_headers(resp)[["etag"]]
      lm     <- resp_headers(resp)[["last-modified"]]
      
      if (status == 200 && grepl("text/html", ctype, fixed = TRUE)) {
        raw <- resp_body_raw(resp)
        save_bin(raw, p)
        bytes <- length(raw)
        ok <- TRUE
      } else {
        warning(sprintf("Skipped (status/content): %s [status=%s, ctype=%s]",
                        u, status, ctype))
        err <- sprintf("status=%s ctype=%s", status, ctype)
      }
    }, silent = TRUE)
    
    tibble(
      fights = u,
      path = p,
      fetched_at = as.character(Sys.time()),
      status_code = status,
      content_type = ctype,
      etag = etag %||% NA_character_,
      last_modified = lm %||% NA_character_,
      bytes = bytes,
      ok = ok,
      error_msg = if (!ok) (err %||% "request failed or non-html") else NA_character_
    )
  }
)

results <- dplyr::bind_rows(results)

# merge back into manifest and save
manifest <- manifest |>
  left_join(
    results |>
      select(fights, fetched_at, status_code, content_type, etag, last_modified, bytes, ok, error_msg),
    by = "fights"
  ) |>
  mutate(fetched_at = coalesce(fetched_at, as.character(NA)))

write_csv(manifest, MANIFEST)

message("Done.")
