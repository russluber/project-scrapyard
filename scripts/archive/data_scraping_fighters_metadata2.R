# ufc_fighters_metadata_split.R
# Build two CSVs:
#   - data/fighters_static.csv  (stable facts: id, name, physicals, stance, DOB)
#   - data/fighters_status.csv  (changing facts: W/L/D, belt, scraped_at)
# DOB is fetched from individual fighter pages; we only fetch where DOB is missing.

library(rvest)
library(tidyverse)
library(lubridate)
library(progress)
library(stringr)
library(here)

# ----------------------------- Config ---------------------------------
BASE <- "http://www.ufcstats.com"
MASTER_URL <- function(letter) paste0(BASE, "/statistics/fighters?char=", letter, "&page=all")
LETTERS_TO_SCRAPE <- letters
DOB_RATE_DELAY <- 0.4         # polite delay between fighter-page fetches (seconds)
RETRY <- 3                    # retry attempts for network reads

OUT_DIR       <- here("data")
STATIC_PATH   <- here("data", "fighters_static_metadata.csv")
STATUS_PATH   <- here("data", "fighters_dynamic_metadata.csv")

# ----------------------------- Parsers --------------------------------
parse_height_in <- function(x) {
  if (is.na(x) || x == "--") return(NA_real_)
  x <- str_squish(tolower(x))
  m <- str_match(x, "([0-9]+)\\s*(?:'|ft)\\s*([0-9]+)?")
  if (is.na(m[1,1])) return(NA_real_)
  feet <- as.numeric(m[1,2])
  inch <- suppressWarnings(as.numeric(m[1,3])); inch <- ifelse(is.na(inch), 0, inch)
  feet * 12 + inch
}

parse_weight_lb <- function(x) {
  if (is.na(x) || x == "--") return(NA_real_)
  x <- str_squish(tolower(x))
  as.numeric(str_remove(x, "\\s*lbs\\.?$"))
}

parse_reach_in <- function(x) {
  if (is.na(x) || x == "--") return(NA_real_)
  x <- str_squish(tolower(x))
  as.numeric(str_remove(x, '"$'))
}

parse_dob <- function(x) {
  if (is.na(x) || x == "--") return(as.Date(NA))
  suppressWarnings(mdy(x))
}

extract_fighter_id <- function(url) {
  url <- tolower(url %||% "")
  url <- str_remove(url, "#.*$")  # fragment
  url <- str_remove(url, "\\?.*$")# query
  m <- str_match(url, "/fighter-details/([0-9a-f]+)/?$")
  id <- m[,2]
  ifelse(is.na(id), NA_character_, id)
}

safe_read_html <- function(url, retry = RETRY, quiet = FALSE) {
  for (i in seq_len(retry)) {
    out <- try(read_html(url), silent = TRUE)
    if (!inherits(out, "try-error")) return(out)
    if (!quiet) message("  read_html failed (", i, "/", retry, "): ", url)
    Sys.sleep(0.8 + runif(1, 0, 0.5))
  }
  stop("Failed to read: ", url)
}

# ----------------------- Master-page scraping -------------------------
scrape_master_letter <- function(letter = "a") {
  url <- MASTER_URL(letter)
  pg  <- safe_read_html(url, quiet = TRUE)
  
  rows <- html_elements(pg, "table.b-statistics__table tbody tr.b-statistics__table-row")
  if (length(rows) == 0) return(tibble())
  
  purrr::map_dfr(rows, function(tr) {
    tds <- html_elements(tr, "td.b-statistics__table-col")
    if (length(tds) < 10) return(tibble())   # guard against odd rows
    
    # any of the first 3 cells should link to the fighter-details page; take the first found
    link <- html_elements(tr, "td:nth-child(-n+3) a[href*='fighter-details']") |>
      html_attr("href") |>
      unique() |>
      purrr::pluck(1, .default = NA_character_)
    
    belt_img <- if (length(tds) >= 11) length(html_elements(tds[[11]], "img")) > 0 else NA
    
    tibble(
      first       = html_text2(tds[[1]]),
      last        = html_text2(tds[[2]]),
      nickname    = html_text2(tds[[3]]),
      height_raw  = html_text2(tds[[4]]),
      weight_raw  = html_text2(tds[[5]]),
      reach_raw   = html_text2(tds[[6]]),
      stance_raw  = html_text2(tds[[7]]),
      w           = readr::parse_integer(html_text2(tds[[8]])),
      l           = readr::parse_integer(html_text2(tds[[9]])),
      d           = readr::parse_integer(html_text2(tds[[10]])),
      belt        = belt_img,
      details_url = link,
      fighter_id  = extract_fighter_id(link),
      letter      = letter
    )
  }) |>
    mutate(
      nickname   = na_if(nickname, "--"),
      stance     = na_if(stance_raw, "--"),
      height_in  = parse_height_in(height_raw),
      weight_lb  = parse_weight_lb(weight_raw),
      reach_in   = parse_reach_in(reach_raw)
    ) |>
    select(-ends_with("_raw"))
}

# ----------------------- DOB from fighter page ------------------------
fetch_dob_from_page <- function(details_url) {
  if (is.na(details_url) || details_url == "") return(as.Date(NA))
  
  Sys.sleep(DOB_RATE_DELAY + runif(1, 0, 0.15))
  doc <- try(safe_read_html(details_url, quiet = TRUE), silent = TRUE)
  if (inherits(doc, "try-error")) return(as.Date(NA))
  
  # Precise selectors (two class spellings), then broad fallback
  sel <- paste(
    "div.b-list__info-box.b-list__info-box_style_small-width ul.b-list__box-list > li,",
    "div.b-list__info-box.b-list__info_style_small-width ul.b-list__box-list > li"
  )
  lis <- html_elements(doc, sel)
  if (length(lis) == 0) {
    lis <- html_elements(doc, "div.b-list__info-box ul.b-list__box-list > li")
    if (length(lis) == 0) return(as.Date(NA))
  }
  
  # Primary parse path: use <i> for labels when present
  labels <- lis |> html_element("i") |> html_text2()
  raw_li  <- lis |> html_text2()
  labels  <- dplyr::coalesce(labels, stringr::str_extract(raw_li, "^[^:]+"))
  labels  <- labels |> stringr::str_remove(":") |> stringr::str_squish()
  values  <- raw_li  |> stringr::str_replace("^.*?:\\s*", "") |> stringr::str_squish()
  
  kv <- tibble::tibble(label = labels, value = values)
  
  dob_txt <- kv %>%
    dplyr::filter(stringr::str_to_upper(label) == "DOB") %>%
    dplyr::pull(value) %>%
    dplyr::first()
  
  # XPath fallback: directly find the LI whose <i> contains 'DOB'
  if (is.na(dob_txt) || is.null(dob_txt)) {
    li_dob <- html_elements(doc, xpath =
                              "//div[contains(@class,'b-list__info-box')][contains(@class,'small-width')]//ul[contains(@class,'b-list__box-list')]/li[
         i and contains(translate(normalize-space(i), 'dob', 'DOB'), 'DOB')
       ]"
    )
    if (length(li_dob) == 0) {
      li_dob <- html_elements(doc, xpath =
                                "//div[contains(@class,'b-list__info-box')]//ul[contains(@class,'b-list__box-list')]/li[
           i and contains(translate(normalize-space(i), 'dob', 'DOB'), 'DOB')
         ]"
      )
    }
    if (length(li_dob) > 0) {
      dob_txt <- html_text2(li_dob[[1]]) %>%
        stringr::str_replace("^.*?:\\s*", "") %>%
        stringr::str_squish()
    }
  }
  
  # Normalize month abbreviations with periods (e.g., "Nov. 15, 1990")
  if (!is.na(dob_txt) && !is.null(dob_txt)) {
    dob_txt <- stringr::str_replace_all(dob_txt, "\\.", "")
  }
  
  parse_dob(dob_txt)
}

# ----------------------------- Build ----------------------------------
build_master_all <- function(letters_vec = LETTERS_TO_SCRAPE) {
  message("Scraping master lists A→Z ...")
  pb <- progress_bar$new(total = length(letters_vec), clear = FALSE,
                         format = "[:bar] :current/:total :letter")
  master <- map_dfr(letters_vec, function(L) {
    pb$tick(tokens = list(letter = L))
    try(scrape_master_letter(L), silent = TRUE) %>% suppressWarnings()
  }) %>% distinct(fighter_id, .keep_all = TRUE)
  
  # Drop rows lacking a usable fighter_id
  master %>% filter(!is.na(fighter_id) & fighter_id != "")
}

# Incremental DOB fetch: reuse existing static CSV if present
fill_missing_dob <- function(master) {
  # Bring forward prior DOBs (if any)
  if (file.exists(STATIC_PATH)) {
    prev_static <- readr::read_csv(STATIC_PATH, show_col_types = FALSE) %>%
      select(fighter_id, dob_prev = dob)
    master <- master %>% left_join(prev_static, by = "fighter_id") %>%
      mutate(dob = dob_prev) %>%
      select(-dob_prev)
  } else {
    master$dob <- as.Date(NA)
  }
  
  # Fetch only for rows still missing DOB
  idx <- which(is.na(master$dob))
  if (length(idx) > 0) {
    message("Fetching DOB from fighter pages for ", length(idx), " fighters ...")
    pb2 <- progress_bar$new(total = length(idx), clear = FALSE,
                            format = "[:bar] :current/:total dob")
    for (k in seq_along(idx)) {
      i <- idx[k]
      pb2$tick()
      master$dob[i] <- fetch_dob_from_page(master$details_url[i])
    }
  } else {
    message("No DOB fetch needed; all present from previous run.")
  }
  master
}

# Split into static/status frames
split_static_status <- function(master_with_dob) {
  static_tbl <- master_with_dob %>%
    transmute(
      fighter_id,
      details_url,
      first,
      last,
      nickname,
      height_in,
      weight_lb,
      reach_in,
      stance,
      dob
    ) %>%
    arrange(last, first, fighter_id)
  
  status_tbl <- master_with_dob %>%
    transmute(
      fighter_id,
      w, l, d,
      belt = coalesce(belt, FALSE),
      scraped_at = Sys.time()
    ) %>%
    arrange(fighter_id)
  
  list(static = static_tbl, status = status_tbl)
}

# ----------------------------- Run ------------------------------------
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

master <- build_master_all()
master <- fill_missing_dob(master)

out <- split_static_status(master)

readr::write_csv(out$static, STATIC_PATH)
readr::write_csv(out$status, STATUS_PATH)

message("Wrote ", nrow(out$static), " rows to ", STATIC_PATH)
message("Wrote ", nrow(out$status), " rows to ", STATUS_PATH)

# --------------------------- Usage notes -------------------------------
# - fighters_static.csv: stable facts; compute age at analysis time via:
#     fighters <- readr::read_csv(STATIC_PATH, show_col_types = FALSE) %>%
#       mutate(age_years = if_else(!is.na(dob),
#                                  as.numeric(difftime(Sys.Date(), dob, "days"))/365.25,
#                                  NA_real_))
# - fighters_status.csv: snapshot of W/L/D and belt with a scraped_at timestamp.
#   Re-run this script when you want a refreshed snapshot.
