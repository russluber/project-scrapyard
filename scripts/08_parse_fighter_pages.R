# parse_fighter_pages.R — build data/fighters_data.csv from cached fighter HTML
# Inputs:
#   - data/raw/fighters_manifest.csv (fighter_id, fighter_url, path)
#   - cache/fighters/{fighter_id}.html
# Output:
#   - data/fighters_data.csv with columns:
#       fighter_id, details_url, name, height_in, weight_lb, reach_in, stance, dob

suppressPackageStartupMessages({
  library(rvest)
  library(tidyverse)
  library(lubridate)
  library(stringr)
  library(here)
  library(progress)
})

# ----------------------------- Config ---------------------------------
MANIFEST_PATH <- here("data", "raw", "fighters_manifest.csv")
OUT_DIR       <- here("data", "raw")
OUT_CSV       <- here("data", "raw", "fighters_data_raw.csv")

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ----------------------------- Helpers --------------------------------
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

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
  x <- str_replace_all(x, "\\.", "")  # "Nov. 15, 1990" -> "Nov 15, 1990"
  suppressWarnings(lubridate::mdy(x))
}

# Extract label-value pairs from the profile info list
extract_info_map <- function(doc) {
  # Try precise selectors first, then broaden
  sel <- paste(
    "div.b-list__info-box.b-list__info-box_style_small-width ul.b-list__box-list > li,",
    "div.b-list__info-box.b-list__info_style_small-width ul.b-list__box-list > li"
  )
  lis <- html_elements(doc, sel)
  if (length(lis) == 0) {
    lis <- html_elements(doc, "div.b-list__info-box ul.b-list__box-list > li")
  }
  if (length(lis) == 0) return(list())
  
  labels <- lis |> html_element("i") |> html_text2()
  raw_li <- lis |> html_text2()
  
  labels <- coalesce(labels, str_extract(raw_li, "^[^:]+"))
  labels <- labels |> str_remove(":") |> str_squish()
  values <- raw_li  |> str_replace("^.*?:\\s*", "") |> str_squish()
  
  # Build a named list (case-insensitive keys normalized to upper)
  lbl_up <- toupper(labels)
  out <- as.list(values)
  names(out) <- lbl_up
  out
}

# Extract display name from header
extract_name <- function(doc) {
  nm <- html_element(doc, ".b-content__title .b-content__title-highlight") %||%
    html_element(doc, "h2.b-content__title") %||%
    html_element(doc, ".b-content__title h2")
  nm_txt <- if (length(nm)) html_text2(nm) else NA_character_
  str_squish(nm_txt)
}

# Parse one fighter file -> tibble row
parse_fighter_file <- function(fighter_id, details_url, path) {
  if (!file.exists(path)) {
    return(tibble(
      fighter_id = fighter_id,
      details_url = details_url,
      name = NA_character_,
      height_in = NA_real_,
      weight_lb = NA_real_,
      reach_in  = NA_real_,
      stance    = NA_character_,
      dob       = as.Date(NA)
    ))
  }
  
  doc <- try(read_html(path), silent = TRUE)
  if (inherits(doc, "try-error")) {
    return(tibble(
      fighter_id = fighter_id,
      details_url = details_url,
      name = NA_character_,
      height_in = NA_real_,
      weight_lb = NA_real_,
      reach_in  = NA_real_,
      stance    = NA_character_,
      dob       = as.Date(NA)
    ))
  }
  
  info <- extract_info_map(doc)
  
  height_in <- parse_height_in(info[["HEIGHT"]] %||% NA_character_)
  weight_lb <- parse_weight_lb(info[["WEIGHT"]] %||% NA_character_)
  reach_in  <- parse_reach_in(info[["REACH"]] %||% NA_character_)
  stance    <- info[["STANCE"]] %||% NA_character_
  stance    <- ifelse(is.na(stance) | stance == "--" | stance == "", NA_character_, str_squish(stance))
  dob       <- parse_dob(info[["DOB"]] %||% NA_character_)
  
  name <- extract_name(doc)
  
  tibble(
    fighter_id = fighter_id,
    details_url = details_url,
    name = name,
    height_in = height_in,
    weight_lb = weight_lb,
    reach_in  = reach_in,
    stance    = stance,
    dob       = dob
  )
}

# ----------------------------- Run ------------------------------------
if (!file.exists(MANIFEST_PATH)) {
  stop("Manifest not found: ", MANIFEST_PATH)
}

man <- readr::read_csv(MANIFEST_PATH, show_col_types = FALSE) |>
  transmute(
    fighter_id = fighter_id,
    details_url = fighter_url,
    path = path
  ) |>
  filter(!is.na(fighter_id), nzchar(fighter_id)) |>
  distinct(fighter_id, .keep_all = TRUE)

if (nrow(man) == 0) {
  stop("Manifest has no fighter rows.")
}

pb <- progress_bar$new(total = nrow(man), clear = FALSE,
                       format = "[:bar] :current/:total :fighter_id")
rows <- vector("list", nrow(man))

for (i in seq_len(nrow(man))) {
  pb$tick(tokens = list(fighter_id = man$fighter_id[i]))
  rows[[i]] <- parse_fighter_file(man$fighter_id[i], man$details_url[i], man$path[i])
}

out <- bind_rows(rows) |>
  arrange(fighter_id)

readr::write_csv(out, OUT_CSV)
message("Wrote ", nrow(out), " rows to ", OUT_CSV)
