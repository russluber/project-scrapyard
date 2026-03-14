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
BASE_URL    <- "http://ufcstats.com"
MASTER_URL  <- function(letter) paste0(BASE_URL, "/statistics/fighters?char=", letter, "&page=all")

CACHE_DIR   <- here("cache", "fighters")
OUT_DIR     <- here("data", "raw")
MANIFEST    <- file.path(OUT_DIR, "fighters_manifest.csv")
OUT_CSV     <- file.path(OUT_DIR, "fighters_data_raw.csv")
ERR_CSV     <- file.path(OUT_DIR, "parse_errors_fighters.csv")

LETTERS_TO_SCRAPE <- letters
MAX_LETTERS       <- Inf
FIGHTER_BATCH_LIMIT <- Inf
STALE_AFTER_DAYS  <- Inf
MAX_FETCH_RETRIES <- 5
POLITE_WORKERS    <- min(max(1, parallelly::availableCores() - 1), 4)
BATCH_PAUSE_SEC   <- c(0.5, 1.25)

USER_AGENT <- "UFC stats research scraper (fighter metadata canonical cache)"

dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

abs_url <- function(link, base = BASE_URL) {
  tryCatch(xml2::url_absolute(link, base = base), error = function(e) link)
}

fighter_id_from_url <- function(u) sub(".*/fighter-details/([0-9A-Fa-f]{16}).*", "\\1", u)

save_bin <- function(raw, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  writeBin(raw, path)
}

now_chr <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")

read_csv_if_exists <- function(path) {
  if (!file.exists(path)) return(tibble())
  read_csv(path, show_col_types = FALSE)
}

ensure_cols <- function(df, defaults) {
  for (nm in names(defaults)) {
    if (!nm %in% names(df)) df[[nm]] <- defaults[[nm]]
  }
  df %>% select(any_of(names(defaults)), everything())
}

coerce_manifest <- function(df) {
  defaults <- list(
    fighter_id = character(),
    fighter_url = character(),
    path = character(),
    letter = character(),
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
      across(c(fighter_id, fighter_url, path, letter, last_seen_at, fetched_at, content_type, error_msg, parsed_at), as.character),
      fetch_ok = as.logical(fetch_ok),
      status_code = suppressWarnings(as.numeric(status_code)),
      bytes = suppressWarnings(as.numeric(bytes))
    ) %>%
    distinct(fighter_id, .keep_all = TRUE)
}

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

get_fighters_from_letter <- function(letter) {
  doc <- try(read_html(MASTER_URL(letter)), silent = TRUE)
  if (inherits(doc, "try-error")) {
    return(tibble(letter = character(), fighter_url = character()))
  }

  links <- doc %>%
    html_elements("table.b-statistics__table tbody tr.b-statistics__table-row td:nth-child(-n+3) a[href]") %>%
    html_attr("href") %>%
    discard(is.na) %>%
    map_chr(abs_url) %>%
    keep(~ grepl("/fighter-details/[0-9A-Fa-f]{16}", .x))

  tibble(letter = letter, fighter_url = unique(links))
}

parse_height_in <- function(x) {
  x <- tolower(str_squish(as.character(x)))
  x[x == "" | x == "--"] <- NA_character_
  m <- str_match(x, "^\\s*([0-9]+)\\s*(?:'|ft)\\s*([0-9]+)?")
  feet <- suppressWarnings(as.numeric(m[, 2]))
  inch <- suppressWarnings(as.numeric(m[, 3]))
  inch[is.na(inch) & !is.na(feet)] <- 0
  out <- feet * 12 + inch
  out[is.na(feet)] <- NA_real_
  out
}

parse_weight_lb <- function(x) {
  x <- tolower(str_squish(as.character(x)))
  x[x == "" | x == "--"] <- NA_character_
  suppressWarnings(as.numeric(str_remove(x, "\\s*lb?s?\\.?$")))
}

parse_reach_in <- function(x) {
  x <- tolower(str_squish(as.character(x)))
  x[x == "" | x == "--"] <- NA_character_
  suppressWarnings(as.numeric(str_remove(x, '"$')))
}

parse_dob <- function(x) {
  x <- str_squish(as.character(x))
  x[x == "" | x == "--"] <- NA_character_
  x <- str_replace_all(x, "\\.", "")
  suppressWarnings(mdy(x))
}

extract_info_map <- function(doc) {
  sel <- paste(
    "div.b-list__info-box.b-list__info-box_style_small-width ul.b-list__box-list > li,",
    "div.b-list__info-box.b-list__info_style_small-width ul.b-list__box-list > li"
  )
  lis <- html_elements(doc, sel)
  if (length(lis) == 0) {
    lis <- html_elements(doc, "div.b-list__info-box ul.b-list__box-list > li")
  }
  if (length(lis) == 0) return(list())

  labels <- lis %>% html_element("i") %>% html_text2()
  raw_li <- lis %>% html_text2()

  labels <- coalesce(labels, str_extract(raw_li, "^[^:]+"))
  labels <- labels %>% str_remove(":") %>% str_squish()
  values <- raw_li %>% str_replace("^.*?:\\s*", "") %>% str_squish()

  out <- as.list(values)
  names(out) <- toupper(labels)
  out
}

extract_name <- function(doc) {
  nm <- html_element(doc, ".b-content__title .b-content__title-highlight") %||%
    html_element(doc, "h2.b-content__title") %||%
    html_element(doc, ".b-content__title h2")
  nm_txt <- if (length(nm)) html_text2(nm) else NA_character_
  str_squish(nm_txt)
}

empty_fighter_row <- function(fighter_id, details_url) {
  tibble(
    fighter_id = fighter_id,
    details_url = details_url,
    name = NA_character_,
    height_in = NA_real_,
    weight_lb = NA_real_,
    reach_in = NA_real_,
    stance = NA_character_,
    dob = as.Date(NA)
  )
}

parse_fighter_file <- function(fighter_id, details_url, path) {
  if (!file.exists(path)) return(empty_fighter_row(fighter_id, details_url))

  doc <- try(read_html(path), silent = TRUE)
  if (inherits(doc, "try-error")) return(empty_fighter_row(fighter_id, details_url))

  info <- extract_info_map(doc)
  height_in <- parse_height_in(info[["HEIGHT"]] %||% NA_character_)
  weight_lb <- parse_weight_lb(info[["WEIGHT"]] %||% NA_character_)
  reach_in  <- parse_reach_in(info[["REACH"]] %||% NA_character_)
  stance    <- info[["STANCE"]] %||% NA_character_
  stance    <- ifelse(is.na(stance) | stance == "--" | stance == "", NA_character_, str_squish(stance))
  dob       <- parse_dob(info[["DOB"]] %||% NA_character_)
  name      <- extract_name(doc)

  tibble(
    fighter_id = fighter_id,
    details_url = details_url,
    name = name,
    height_in = height_in,
    weight_lb = weight_lb,
    reach_in = reach_in,
    stance = stance,
    dob = dob
  )
}

parse_fighters_safely <- function(df) {
  safe_parse <- safely(
    function(fighter_id, details_url, path) parse_fighter_file(fighter_id, details_url, path),
    otherwise = NULL
  )

  res <- pmap(
    df %>%
      transmute(
        fighter_id = fighter_id,
        details_url = fighter_url,
        path = path
      ),
    safe_parse
  )

  parsed <- compact(map(res, "result"))
  errs <- tibble(
    fighter_id = df$fighter_id,
    details_url = df$fighter_url,
    path = df$path,
    error = map_chr(map(res, "error"), ~ if (is.null(.x)) NA_character_ else conditionMessage(.x))
  ) %>%
    filter(!is.na(error))

  parsed_tbl <- if (length(parsed) > 0) {
    bind_rows(parsed) %>%
      arrange(fighter_id) %>%
      distinct(fighter_id, .keep_all = TRUE)
  } else {
    tibble()
  }

  list(parsed = parsed_tbl, errors = errs)
}

coerce_fighter_output <- function(df) {
  defaults <- list(
    fighter_id = character(),
    details_url = character(),
    name = character(),
    height_in = numeric(),
    weight_lb = numeric(),
    reach_in = numeric(),
    stance = character(),
    dob = as.Date(character())
  )

  ensure_cols(df, defaults) %>%
    mutate(
      across(c(fighter_id, details_url, name, stance), as.character),
      height_in = suppressWarnings(as.numeric(height_in)),
      weight_lb = suppressWarnings(as.numeric(weight_lb)),
      reach_in = suppressWarnings(as.numeric(reach_in)),
      dob = suppressWarnings(as.Date(dob))
    ) %>%
    arrange(fighter_id) %>%
    distinct(fighter_id, .keep_all = TRUE)
}

# -------------------------------------------------------------------
# 1) Discover all fighter profile URLs from A-Z master pages
# -------------------------------------------------------------------
letters_vec <- LETTERS_TO_SCRAPE
if (!is.finite(MAX_LETTERS)) MAX_LETTERS <- length(letters_vec)
letters_vec <- head(letters_vec, MAX_LETTERS)

message("Scanning fighter master pages for letters: ", paste(letters_vec, collapse = ", "))

fighters_tbl <- map_dfr(letters_vec, get_fighters_from_letter) %>%
  mutate(
    fighter_id = fighter_id_from_url(fighter_url),
    path = file.path(CACHE_DIR, paste0(fighter_id, ".html"))
  ) %>%
  filter(!is.na(fighter_id), nzchar(fighter_id)) %>%
  distinct(fighter_id, .keep_all = TRUE)

if (nrow(fighters_tbl) == 0) {
  stop("No fighter URLs discovered from master pages.")
}

# -------------------------------------------------------------------
# 2) Build or update manifest
# -------------------------------------------------------------------
manifest_old <- read_csv_if_exists(MANIFEST) %>% coerce_manifest()
manifest_new <- fighters_tbl %>%
  transmute(
    fighter_id = as.character(fighter_id),
    fighter_url = as.character(fighter_url),
    path = as.character(path),
    letter = as.character(letter),
    last_seen_at = now_chr(),
    fetched_at = NA_character_,
    fetch_ok = NA,
    status_code = NA_real_,
    content_type = NA_character_,
    bytes = NA_real_,
    error_msg = NA_character_,
    parsed_at = NA_character_
  )

manifest <- merge_manifests(manifest_old, manifest_new, "fighter_id") %>%
  coerce_manifest() %>%
  mutate(needs_fetch = manifest_needs_fetch(path, fetch_ok, fetched_at, stale_after_days = STALE_AFTER_DAYS))

need <- manifest %>%
  filter(needs_fetch)

if (!is.finite(FIGHTER_BATCH_LIMIT)) FIGHTER_BATCH_LIMIT <- nrow(need)
need <- head(need, FIGHTER_BATCH_LIMIT)

message("Fighters indexed : ", nrow(manifest))
message("Need to fetch    : ", nrow(need))

# -------------------------------------------------------------------
# 3) Fetch missing or failed fighter pages with bounded concurrency
# -------------------------------------------------------------------
if (nrow(need) > 0) {
  fetches <- fetch_in_batches(
    need,
    url_col = "fighter_url",
    path_col = "path",
    user_agent = USER_AGENT,
    max_tries = MAX_FETCH_RETRIES,
    batch_pause = BATCH_PAUSE_SEC,
    workers = POLITE_WORKERS
  ) %>%
    bind_cols(need %>% select(fighter_id))

  manifest <- manifest %>%
    select(-needs_fetch) %>%
    left_join(fetches, by = "fighter_id", suffix = c("", "_new")) %>%
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
  fetches <- tibble(fighter_id = character())
  manifest <- manifest %>% select(-needs_fetch)
}

write_csv(manifest, MANIFEST)

# -------------------------------------------------------------------
# 4) Parse only new or refetched fighter pages
# -------------------------------------------------------------------
existing_out <- read_csv_if_exists(OUT_CSV) %>% coerce_fighter_output()
already_parsed <- if ("fighter_id" %in% names(existing_out)) unique(existing_out$fighter_id) else character()
refetched_ids <- if (nrow(fetches) > 0) fetches$fighter_id[fetches$fetch_ok %in% TRUE] else character()

parse_queue <- manifest %>%
  filter(file.exists(path)) %>%
  filter(!(fighter_id %in% already_parsed) | fighter_id %in% refetched_ids) %>%
  distinct(fighter_id, .keep_all = TRUE)

message("Need to parse    : ", nrow(parse_queue))

parsed_res <- parse_fighters_safely(parse_queue)

if (nrow(parsed_res$errors) > 0) {
  prior_errs <- read_csv_if_exists(ERR_CSV)
  out_errs <- bind_rows(prior_errs, parsed_res$errors) %>%
    distinct(fighter_id, .keep_all = TRUE)
  write_csv(out_errs, ERR_CSV)
} else if (file.exists(ERR_CSV)) {
  file.remove(ERR_CSV)
}

new_rows <- parsed_res$parsed
out <- if (nrow(existing_out) > 0 && nrow(new_rows) > 0) {
  bind_rows(new_rows, existing_out) %>%
    distinct(fighter_id, .keep_all = TRUE) %>%
    coerce_fighter_output()
} else if (nrow(new_rows) > 0) {
  new_rows %>% coerce_fighter_output()
} else {
  existing_out %>% coerce_fighter_output()
}

write_csv(out, OUT_CSV)

manifest <- manifest %>%
  left_join(
    tibble(fighter_id = unique(new_rows[["fighter_id"]] %||% character()), parsed_at_new = now_chr()),
    by = "fighter_id"
  ) %>%
  mutate(parsed_at = coalesce(parsed_at_new, parsed_at)) %>%
  select(-parsed_at_new)

write_csv(manifest, MANIFEST)

message("Done.")
message("Manifest written : ", MANIFEST)
message("Output written   : ", OUT_CSV)
if (file.exists(ERR_CSV)) message("Parse errors     : ", ERR_CSV)
