# scripts/_helpers.R
#
# Shared utilities for the UFCStats scraping + parsing pipeline.
# Sourced by 00_scrape_fights.R and 01_scrape_fighters.R.
#
# Everything here is generic: HTTP fetching with retries, a cache-first
# manifest system that makes the scrapers idempotent and resumable, and
# small HTML/string helpers. Page-specific parsing lives in the scripts
# that source this file.

suppressPackageStartupMessages({
  library(rvest)
  library(dplyr)
  library(tibble)
  library(tidyr)
  library(readr)
  library(stringr)
  library(purrr)
  library(lubridate)
  library(httr2)
  library(furrr)
  library(future)
  library(parallelly)
  library(xml2)
})

# -------------------------------------------------------------------
# Shared config
# -------------------------------------------------------------------
BASE_URL <- "http://ufcstats.com"

# Bounded, polite concurrency for batch fetches: cores - 1, capped at 4.
POLITE_WORKERS <- min(max(1, parallelly::availableCores() - 1), 4)

# -------------------------------------------------------------------
# Tiny helpers
# -------------------------------------------------------------------

# Null/empty coalescing: return `a` unless it is NULL/zero-length.
`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

# Resolve a possibly-relative href against the site base.
abs_url <- function(link, base = BASE_URL) {
  tryCatch(xml2::url_absolute(link, base = base), error = function(e) link)
}

# Pull the 16-hex id out of a UFCStats detail URL of a given kind
# (e.g. "event", "fight", "fighter").
id_from_url <- function(u, kind) {
  pattern <- sprintf(".*/%s-details/([0-9A-Fa-f]{16}).*", kind)
  sub(pattern, "\\1", u)
}

# Recover an id from a cached file path like "cache/fights/<id>.html".
id_from_path <- function(path) sub("\\.html$", "", basename(path), ignore.case = TRUE)

# Write raw bytes to disk, creating the parent dir if needed.
save_bin <- function(raw, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  writeBin(raw, path)
}

# Current timestamp as a stable character string (for manifests).
now_chr <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")

# Squish whitespace and turn empty strings into NA.
squish_na <- function(x) {
  x <- str_squish(x)
  na_if(x, "")
}

# Read a CSV if it exists, otherwise an empty tibble.
read_csv_if_exists <- function(path, col_types = NULL) {
  if (!file.exists(path)) return(tibble())
  if (is.null(col_types)) {
    read_csv(path, show_col_types = FALSE)
  } else {
    read_csv(path, col_types = col_types, show_col_types = FALSE)
  }
}

# Ensure a data frame has all columns named in `defaults`, in front.
ensure_cols <- function(df, defaults) {
  for (nm in names(defaults)) {
    if (!nm %in% names(df)) df[[nm]] <- defaults[[nm]]
  }
  df %>% select(any_of(names(defaults)), everything())
}

# -------------------------------------------------------------------
# Manifest machinery
#
# A "manifest" is one row per page (event / fight / fighter) tracking
# its URL, cache path, and fetch/parse status. The manifest is what
# makes the pipeline incremental: on a rerun we only fetch pages that
# are missing or previously failed, and only re-parse pages that are
# new or were just refetched.
# -------------------------------------------------------------------

# Decide which cached pages still need fetching.
# A page needs fetching if its file is missing, its last fetch failed,
# or (when a finite staleness window is set) it was fetched too long ago.
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

# Merge an old manifest with a freshly-discovered one on `key`,
# preferring new (non-NA) values but falling back to prior state.
merge_manifests <- function(old, new, key) {
  if (nrow(old) == 0) return(new)
  old %>%
    full_join(new, by = key, suffix = c("_old", "")) %>%
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

# Coerce a manifest to a stable schema. `key` is the id column name;
# every manifest shares the same fetch-status columns.
#
# Backward compatibility: older manifests stored the page URL in a
# kind-specific column (event_url / fight_url / fighter_url). Rename any
# such column to the generic `url` so previously-cached manifests merge
# cleanly and don't trigger a full re-fetch.
coerce_manifest <- function(df, key, extra = character()) {
  if (nrow(df) > 0 && !"url" %in% names(df)) {
    legacy_url <- intersect(c("event_url", "fight_url", "fighter_url"), names(df))
    if (length(legacy_url) >= 1) df <- dplyr::rename(df, url = !!legacy_url[1])
  }

  defaults <- list(
    url = character(),
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
  # The id column and any caller-specific extras come first.
  id_defaults <- setNames(rep(list(character()), length(c(key, extra))), c(key, extra))
  defaults <- c(id_defaults, defaults)

  ensure_cols(df, defaults) %>%
    mutate(
      across(any_of(c(key, extra, "url", "path", "last_seen_at", "fetched_at",
                      "content_type", "error_msg", "parsed_at")), as.character),
      fetch_ok = as.logical(fetch_ok),
      status_code = suppressWarnings(as.numeric(status_code)),
      bytes = suppressWarnings(as.numeric(bytes))
    ) %>%
    distinct(.data[[key]], .keep_all = TRUE)
}

# Seed fetch-status columns (NA, unfetched) for `n` rows. Used when
# building a freshly-discovered manifest before any fetching happens.
seed_status_cols <- function(n) {
  tibble(
    last_seen_at = rep(now_chr(), n),
    fetched_at = rep(NA_character_, n),
    fetch_ok = rep(NA, n),
    status_code = rep(NA_real_, n),
    content_type = rep(NA_character_, n),
    bytes = rep(NA_real_, n),
    error_msg = rep(NA_character_, n),
    parsed_at = rep(NA_character_, n)
  )
}

# Fold a batch of fetch results back into the manifest, by `key`.
apply_fetch_results <- function(manifest, fetches, key) {
  manifest %>%
    left_join(fetches, by = key, suffix = c("", "_new")) %>%
    mutate(
      fetched_at   = coalesce(fetched_at_new, fetched_at),
      fetch_ok     = coalesce(fetch_ok_new, fetch_ok),
      status_code  = coalesce(status_code_new, status_code),
      content_type = coalesce(content_type_new, content_type),
      bytes        = coalesce(bytes_new, bytes),
      error_msg    = coalesce(error_msg_new, error_msg)
    ) %>%
    select(-ends_with("_new"))
}

# Stamp parsed_at = now for the given ids.
mark_parsed <- function(manifest, ids, key) {
  ids <- unique(ids %||% character())
  manifest %>%
    left_join(tibble(!!key := ids, parsed_at_new = now_chr()), by = key) %>%
    mutate(parsed_at = coalesce(parsed_at_new, parsed_at)) %>%
    select(-parsed_at_new)
}

# -------------------------------------------------------------------
# HTTP fetching
# -------------------------------------------------------------------

# Build a request with a polite UA, timeout, and retry/backoff on
# transient server errors.
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

# Fetch one page to `path`. Returns a one-row tibble of fetch status.
# Only writes the file on a 200 text/html response.
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

# Fetch many pages sequentially with polite per-request jitter.
# Use for small batches (e.g. event index pages).
fetch_sequential <- function(df, url_col, path_col, user_agent, max_tries,
                             delay = c(0.5, 1.25), id_col = NULL, log_label = "page") {
  if (nrow(df) == 0) return(tibble())
  results <- vector("list", nrow(df))
  for (i in seq_len(nrow(df))) {
    Sys.sleep(runif(1, delay[1], delay[2]))
    results[[i]] <- fetch_one_page(df[[url_col]][i], df[[path_col]][i],
                                   user_agent = user_agent, max_tries = max_tries)
    tag <- if (!is.null(id_col)) df[[id_col]][i] else basename(df[[path_col]][i])
    message(sprintf("[%d/%d] %s %s fetched=%s", i, nrow(df), log_label, tag,
                    if (results[[i]]$fetch_ok[1]) "yes" else "no"))
  }
  bind_rows(results)
}

# Fetch many pages with bounded concurrency, in worker-sized batches,
# pausing between batches. Use for large batches (fights, fighters).
fetch_in_batches <- function(df, url_col, path_col, user_agent, max_tries,
                             batch_pause = c(0.5, 1.25), workers = POLITE_WORKERS) {
  if (nrow(df) == 0) return(tibble())

  old_plan <- future::plan()
  on.exit(future::plan(old_plan), add = TRUE)
  future::plan(future::multisession, workers = workers)

  batches <- split(seq_len(nrow(df)), ceiling(seq_len(nrow(df)) / workers))
  results <- vector("list", length(batches))

  for (i in seq_along(batches)) {
    chunk <- df[batches[[i]], , drop = FALSE]
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
