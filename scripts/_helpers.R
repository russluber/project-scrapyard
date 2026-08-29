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
  library(xml2)
  # chromote is loaded lazily in browser_session() so that the parsing-only
  # helpers can be sourced without it installed.
})

# -------------------------------------------------------------------
# Shared config
# -------------------------------------------------------------------
BASE_URL <- "http://ufcstats.com"

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
# Fetching (headless browser)
#
# ufcstats.com serves a JavaScript anti-bot challenge ("Checking your
# browser...") to plain HTTP clients, so a static fetch (httr2/rvest)
# returns a challenge stub instead of the page. We drive a headless
# Chromium browser (via chromote) so the challenge JS runs, then read
# the rendered DOM. Parsing downstream is unchanged.
#
# A single browser session is reused across all pages (one session per
# page would be far too slow), so fetching is sequential. The session is
# created with browser_session() and must be closed by the caller.
# -------------------------------------------------------------------

# Locate a Chromium-based browser. Honor CHROMOTE_CHROME if set, else try
# common Chrome/Edge install paths on Windows.
find_chromium <- function() {
  env <- Sys.getenv("CHROMOTE_CHROME", "")
  if (nzchar(env) && file.exists(env)) return(env)

  candidates <- c(
    file.path(Sys.getenv("ProgramFiles"), "Google/Chrome/Application/chrome.exe"),
    file.path(Sys.getenv("ProgramFiles(x86)"), "Google/Chrome/Application/chrome.exe"),
    file.path(Sys.getenv("LOCALAPPDATA"), "Google/Chrome/Application/chrome.exe"),
    file.path(Sys.getenv("ProgramFiles"), "Microsoft/Edge/Application/msedge.exe"),
    file.path(Sys.getenv("ProgramFiles(x86)"), "Microsoft/Edge/Application/msedge.exe")
  )
  hit <- candidates[file.exists(candidates)]
  if (length(hit) > 0) return(hit[1])
  ""  # let chromote try its own default discovery
}

# Start a headless browser session for scraping. Returns a ChromoteSession.
browser_session <- function() {
  if (!requireNamespace("chromote", quietly = TRUE)) {
    stop("The 'chromote' package is required to scrape ufcstats.com ",
         "(it serves a JavaScript anti-bot challenge). Install it with ",
         "install.packages('chromote').")
  }
  browser_path <- find_chromium()
  if (nzchar(browser_path)) Sys.setenv(CHROMOTE_CHROME = browser_path)
  if (identical(tolower(basename(browser_path)), "msedge.exe")) {
    # Edge can relaunch itself through the Windows compatibility layer,
    # which detaches chromote from the process and its DevTools port.
    chromote::set_chrome_args(unique(c(
      chromote::get_chrome_args(),
      "--edge-skip-compat-layer-relaunch"
    )))
  }
  message("Headless browser: ", if (nzchar(browser_path)) browser_path else "(chromote default)")
  chromote::ChromoteSession$new()
}

# True if rendered HTML still looks like the anti-bot challenge stub.
is_challenge_html <- function(html) {
  is.null(html) || !nzchar(html) ||
    grepl("Checking your browser|requires JavaScript", html, ignore.case = TRUE)
}

# Navigate `session` to `url` and return rendered HTML once the challenge
# clears, or NULL on timeout. Polls the DOM rather than sleeping a fixed
# amount, so it's as fast as the challenge allows.
render_page <- function(session, url, max_wait = 30, poll = 1.0) {
  session$Page$navigate(url)
  try(session$Page$loadEventFired(timeout = max_wait * 1000), silent = TRUE)

  deadline <- Sys.time() + max_wait
  repeat {
    html <- tryCatch({
      root <- session$DOM$getDocument(depth = -1)$root$nodeId
      session$DOM$getOuterHTML(nodeId = root)$outerHTML
    }, error = function(e) NULL)

    if (!is_challenge_html(html)) return(html)
    if (Sys.time() > deadline) return(NULL)
    Sys.sleep(poll)
  }
}

# Fetch one page through the shared browser `session`, saving rendered
# HTML to `path`. Returns a one-row tibble of fetch status, matching the
# manifest schema. `max_tries` retries navigation if the challenge sticks.
fetch_one_page <- function(url, path, session, max_tries = 3, max_wait = 30) {
  bytes <- NA_real_
  err <- NA_character_
  ok <- FALSE

  for (attempt in seq_len(max_tries)) {
    html <- tryCatch(render_page(session, url, max_wait = max_wait),
                     error = function(e) { err <<- conditionMessage(e); NULL })
    if (!is.null(html) && !is_challenge_html(html)) {
      save_bin(charToRaw(enc2utf8(html)), path)
      bytes <- nchar(html, type = "bytes")
      ok <- TRUE
      err <- NA_character_
      break
    }
    err <- err %||% "challenge not cleared"
    if (attempt < max_tries) Sys.sleep(2 * attempt)  # backoff before retry
  }

  tibble(
    fetched_at = now_chr(),
    fetch_ok = ok,
    status_code = if (ok) 200 else NA_real_,
    content_type = if (ok) "text/html" else NA_character_,
    bytes = bytes,
    error_msg = err
  )
}

# Fetch many pages sequentially through one browser `session`, with polite
# per-request jitter. Used for every batch (large and small), since the
# browser session can't be shared across parallel workers.
fetch_sequential <- function(df, url_col, path_col, session, max_tries = 3,
                             delay = c(0.5, 1.25), id_col = NULL, log_label = "page") {
  if (nrow(df) == 0) return(tibble())
  results <- vector("list", nrow(df))
  for (i in seq_len(nrow(df))) {
    Sys.sleep(runif(1, delay[1], delay[2]))
    results[[i]] <- fetch_one_page(df[[url_col]][i], df[[path_col]][i],
                                   session = session, max_tries = max_tries)
    tag <- if (!is.null(id_col)) df[[id_col]][i] else basename(df[[path_col]][i])
    message(sprintf("[%d/%d] %s %s fetched=%s", i, nrow(df), log_label, tag,
                    if (results[[i]]$fetch_ok[1]) "yes" else "no"))
  }
  bind_rows(results)
}
