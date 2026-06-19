# scripts/01_scrape_fighters.R
#
# Stage 1 of the pipeline: scrape fighter profile metadata from the
# UFCStats A-Z fighter directory.
#
# This stage is independent of the fight scrape (00). It does not modify
# the fight data; it produces a standalone fighter-attributes table that
# downstream analyses can join on fighter_id.
#
# Like the fight scrape, it is cache-first and incremental: reruns only
# fetch fighter pages that are new or previously failed, and only
# re-parse pages that are new or were just refetched.
#
# Output: data/raw/fighters_data_raw.csv
#   fighter_id, details_url, name, height_in, weight_lb, reach_in, stance, dob

suppressPackageStartupMessages({
  library(here)
})
source(here::here("scripts", "_helpers.R"))

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
MASTER_URL <- function(letter) paste0(BASE_URL, "/statistics/fighters?char=", letter, "&page=all")

CACHE_DIR <- here("cache", "fighters")
OUT_DIR   <- here("data", "raw")
MANIFEST  <- file.path(OUT_DIR, "fighters_manifest.csv")
OUT_CSV   <- file.path(OUT_DIR, "fighters_data_raw.csv")
ERR_CSV   <- file.path(OUT_DIR, "parse_errors_fighters.csv")

LETTERS_TO_SCRAPE <- letters
STALE_AFTER_DAYS  <- Inf
MAX_FETCH_RETRIES <- 5

USER_AGENT <- "UFC stats research scraper (fighter metadata cache)"

dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# -------------------------------------------------------------------
# Fighter directory discovery
# -------------------------------------------------------------------

# Collect fighter-detail URLs from one letter's directory page.
get_fighters_from_letter <- function(letter) {
  doc <- try(read_html(MASTER_URL(letter)), silent = TRUE)
  if (inherits(doc, "try-error")) {
    return(tibble(letter = character(), url = character()))
  }
  links <- doc %>%
    html_elements("table.b-statistics__table tbody tr.b-statistics__table-row td:nth-child(-n+3) a[href]") %>%
    html_attr("href") %>%
    discard(is.na) %>%
    map_chr(abs_url) %>%
    keep(~ grepl("/fighter-details/[0-9A-Fa-f]{16}", .x))
  tibble(letter = letter, url = unique(links))
}

# -------------------------------------------------------------------
# Fighter-profile field parsers
# -------------------------------------------------------------------

# "6' 1\"" / "6'1" / "6 ft 1" -> total inches.
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

# "155 lbs." -> 155.
parse_weight_lb <- function(x) {
  x <- tolower(str_squish(as.character(x)))
  x[x == "" | x == "--"] <- NA_character_
  suppressWarnings(as.numeric(str_remove(x, "\\s*lb?s?\\.?$")))
}

# "76\"" -> 76.
parse_reach_in <- function(x) {
  x <- tolower(str_squish(as.character(x)))
  x[x == "" | x == "--"] <- NA_character_
  suppressWarnings(as.numeric(str_remove(x, '"$')))
}

# "Nov. 15, 1990" -> Date.
parse_dob <- function(x) {
  x <- str_squish(as.character(x))
  x[x == "" | x == "--"] <- NA_character_
  x <- str_replace_all(x, "\\.", "")
  suppressWarnings(mdy(x))
}

# Pull the label:value info list from a fighter profile into a named list
# keyed by upper-cased label (HEIGHT, WEIGHT, REACH, STANCE, DOB, ...).
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
  str_squish(if (length(nm)) html_text2(nm) else NA_character_)
}

empty_fighter_row <- function(fighter_id, details_url) {
  tibble(
    fighter_id = fighter_id, details_url = details_url, name = NA_character_,
    height_in = NA_real_, weight_lb = NA_real_, reach_in = NA_real_,
    stance = NA_character_, dob = as.Date(NA)
  )
}

# Parse one cached fighter profile into a one-row tibble.
parse_fighter_file <- function(fighter_id, details_url, path) {
  if (!file.exists(path)) return(empty_fighter_row(fighter_id, details_url))
  doc <- try(read_html(path), silent = TRUE)
  if (inherits(doc, "try-error")) return(empty_fighter_row(fighter_id, details_url))

  info <- extract_info_map(doc)
  stance <- info[["STANCE"]] %||% NA_character_
  stance <- ifelse(is.na(stance) | stance %in% c("--", ""), NA_character_, str_squish(stance))

  tibble(
    fighter_id = fighter_id,
    details_url = details_url,
    name = extract_name(doc),
    height_in = parse_height_in(info[["HEIGHT"]] %||% NA_character_),
    weight_lb = parse_weight_lb(info[["WEIGHT"]] %||% NA_character_),
    reach_in  = parse_reach_in(info[["REACH"]] %||% NA_character_),
    stance    = stance,
    dob       = parse_dob(info[["DOB"]] %||% NA_character_)
  )
}

# Parse a queue of fighter rows, capturing per-file errors.
parse_fighters_safely <- function(df) {
  safe_parse <- safely(parse_fighter_file, otherwise = NULL)
  res <- pmap(
    df %>% transmute(fighter_id, details_url = url, path),
    safe_parse
  )
  parsed <- compact(map(res, "result"))
  errs <- tibble(
    fighter_id = df$fighter_id, details_url = df$url, path = df$path,
    error = map_chr(map(res, "error"), ~ if (is.null(.x)) NA_character_ else conditionMessage(.x))
  ) %>%
    filter(!is.na(error))

  parsed_tbl <- if (length(parsed) > 0) {
    bind_rows(parsed) %>% arrange(fighter_id) %>% distinct(fighter_id, .keep_all = TRUE)
  } else {
    tibble()
  }
  list(parsed = parsed_tbl, errors = errs)
}

# Stable schema for the fighter output table.
coerce_fighter_output <- function(df) {
  defaults <- list(
    fighter_id = character(), details_url = character(), name = character(),
    height_in = numeric(), weight_lb = numeric(), reach_in = numeric(),
    stance = character(), dob = as.Date(character())
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

# ===================================================================
# 1) Discover fighter profile URLs from the A-Z directory
# ===================================================================
message("Scanning fighter directory pages (a-z)...")

fighters_tbl <- map_dfr(LETTERS_TO_SCRAPE, get_fighters_from_letter) %>%
  mutate(fighter_id = id_from_url(url, "fighter"),
         path = file.path(CACHE_DIR, paste0(fighter_id, ".html"))) %>%
  filter(!is.na(fighter_id), nzchar(fighter_id)) %>%
  distinct(fighter_id, .keep_all = TRUE)

if (nrow(fighters_tbl) == 0) stop("No fighter URLs discovered from directory pages.")

# ===================================================================
# 2) Build/update manifest
# ===================================================================
manifest_old <- read_csv_if_exists(MANIFEST) %>% coerce_manifest("fighter_id", extra = "letter")
manifest_new <- fighters_tbl %>%
  transmute(fighter_id = as.character(fighter_id), url = as.character(url),
            path = as.character(path), letter = as.character(letter))
manifest_new <- bind_cols(manifest_new, seed_status_cols(nrow(manifest_new)))

manifest <- merge_manifests(manifest_old, manifest_new, "fighter_id") %>%
  coerce_manifest("fighter_id", extra = "letter") %>%
  mutate(needs_fetch = manifest_needs_fetch(path, fetch_ok, fetched_at, STALE_AFTER_DAYS))

need <- manifest %>% filter(needs_fetch)
message("Fighters indexed : ", nrow(manifest))
message("Need to fetch    : ", nrow(need))

# ===================================================================
# 3) Fetch missing/failed fighter pages (bounded concurrency)
# ===================================================================
if (nrow(need) > 0) {
  fetches <- fetch_in_batches(need, "url", "path", USER_AGENT, MAX_FETCH_RETRIES) %>%
    bind_cols(need %>% select(fighter_id))
  manifest <- manifest %>% select(-needs_fetch) %>%
    apply_fetch_results(fetches, "fighter_id")
} else {
  fetches <- tibble(fighter_id = character())
  manifest <- manifest %>% select(-needs_fetch)
}

write_csv(manifest, MANIFEST)

# ===================================================================
# 4) Parse new/refetched fighter pages
# ===================================================================
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
  out_errs <- bind_rows(read_csv_if_exists(ERR_CSV), parsed_res$errors) %>%
    distinct(fighter_id, .keep_all = TRUE)
  write_csv(out_errs, ERR_CSV)
} else if (file.exists(ERR_CSV)) {
  file.remove(ERR_CSV)
}

new_rows <- parsed_res$parsed
out <- if (nrow(existing_out) > 0 && nrow(new_rows) > 0) {
  bind_rows(new_rows, existing_out) %>% distinct(fighter_id, .keep_all = TRUE) %>% coerce_fighter_output()
} else if (nrow(new_rows) > 0) {
  new_rows %>% coerce_fighter_output()
} else {
  existing_out %>% coerce_fighter_output()
}

write_csv(out, OUT_CSV)
manifest <- mark_parsed(manifest, new_rows[["fighter_id"]], "fighter_id")
write_csv(manifest, MANIFEST)

message("Done (01_scrape_fighters).")
message("  Fighters table : ", OUT_CSV, " (", nrow(out), " fighters)")
