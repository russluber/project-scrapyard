# ufc_fighters_metadata.R
# Build a fighters_metadata.csv from UFCStats master pages + fighter pages (for DOB)

library(rvest)
library(tidyverse)
library(lubridate)
library(progress)
library(stringr)
library(here)

# ----------------------------- Config ---------------------------------
BASE <- "http://www.ufcstats.com"
MASTER_URL <- function(letter) paste0(BASE, "/statistics/fighters?char=", letter, "&page=all")
LETTERS_TO_SCRAPE <- letters  # c(letters, "other") if you later discover a non-alpha bucket
DOB_RATE_DELAY <- 0.4         # polite delay between DOB page fetches (seconds)
RETRY <- 3                    # retry attempts for network reads

# ----------------------------- Parsers --------------------------------
parse_height_in <- function(x) {
  if (is.na(x) || x == "--") return(NA_real_)
  x <- str_squish(tolower(x))
  # "6' 1"", 6'1", 6 ft 1 in
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
  url <- str_remove(url, "#.*$")       # fragment
  url <- str_remove(url, "\\?.*$")     # query
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
    if (length(tds) < 10) return(tibble())   # skip header/odd rows
    
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
    # normalize textual placeholders
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

# fetch_dob_from_page <- function(details_url) {
#   if (is.na(details_url) || details_url == "") return(as.Date(NA))
#   # polite delay
#   Sys.sleep(DOB_RATE_DELAY + runif(1, 0, 0.15))
#   doc <- try(safe_read_html(details_url, quiet = TRUE), silent = TRUE)
#   if (inherits(doc, "try-error")) return(as.Date(NA))
#   
#   # The small info box with Height/Weight/Reach/STANCE/DOB
#   lis <- html_elements(doc, ".b-list__info-box_style_small-width li")
#   if (length(lis) == 0) return(as.Date(NA))
#   
#   labels <- lis |> html_element("i") |> html_text2() |> str_remove(":") |> str_trim()
#   values <- lis |> html_text2() |> str_remove(".*:\\s*") |> str_squish()
#   kv <- tibble(label = labels, value = values)
#   
#   dob_txt <- kv |> filter(str_to_upper(label) == "DOB") |> pull(value) |> dplyr::first()
#   parse_dob(dob_txt)
# }


# fetch_dob_from_page <- function(details_url) {
#   if (is.na(details_url) || details_url == "") return(as.Date(NA))
#   
#   Sys.sleep(DOB_RATE_DELAY + runif(1, 0, 0.15))
#   doc <- try(safe_read_html(details_url, quiet = TRUE), silent = TRUE)
#   if (inherits(doc, "try-error")) return(as.Date(NA))
#   
#   # 1) Prefer the exact small-width box, accepting both class spellings
#   sel <- paste(
#     "div.b-list__info-box.b-list__info-box_style_small-width ul.b-list__box-list > li,",
#     "div.b-list__info-box.b-list__info_style_small-width ul.b-list__box-list > li"
#   )
#   lis <- html_elements(doc, sel)
#   
#   # 2) Fallback: any info-box's ul > li if the above yields nothing
#   if (length(lis) == 0) {
#     lis <- html_elements(doc, "div.b-list__info-box ul.b-list__box-list > li")
#     if (length(lis) == 0) return(as.Date(NA))
#   }
#   
#   labels <- lis |> html_element("i") |> html_text2() |> str_remove(":") |> str_squish()
#   values <- lis |> html_text2() |> str_replace("^.*?:\\s*", "") |> str_squish()
#   
#   kv <- tibble(label = labels, value = values)
#   
#   dob_txt <- kv |>
#     filter(str_to_upper(label) == "DOB") |>
#     pull(value) |>
#     dplyr::first()
#   
#   parse_dob(dob_txt)
# }


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
  # If any labels are NA (missing <i>), synthesize them by taking text up to colon
  raw_li  <- lis |> html_text2()
  labels  <- dplyr::coalesce(labels, stringr::str_extract(raw_li, "^[^:]+"))
  labels  <- labels |> stringr::str_remove(":") |> stringr::str_squish()
  
  # Values: strip everything through the first colon
  values  <- raw_li |> stringr::str_replace("^.*?:\\s*", "") |> stringr::str_squish()
  
  kv <- tibble::tibble(label = labels, value = values)
  
  # Try to get DOB row case-insensitively
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
      dob_txt <- html_text2(li_dob[[1]]) %>% stringr::str_replace("^.*?:\\s*", "") %>% stringr::str_squish()
    }
  }
  
  # Normalize month abbreviations with periods (e.g., "Nov. 15, 1990")
  if (!is.na(dob_txt) && !is.null(dob_txt)) {
    dob_txt <- stringr::str_replace_all(dob_txt, "\\.", "")
  }
  
  parse_dob(dob_txt)
}



# ----------------------------- Driver ---------------------------------
build_fighters_metadata <- function(letters_vec = LETTERS_TO_SCRAPE) {
  message("Scraping master lists A→Z ...")
  pb <- progress_bar$new(total = length(letters_vec), clear = FALSE, format = "[:bar] :current/:total :letter")
  master <- map_dfr(letters_vec, function(L) {
    pb$tick(tokens = list(letter = L))
    try(scrape_master_letter(L), silent = TRUE) %>% suppressWarnings()
  }) %>% distinct(fighter_id, .keep_all = TRUE)
  
  message("\nFetched ", nrow(master), " unique fighters from master pages.")
  
  # Pull DOBs per fighter
  message("Fetching DOB from fighter pages ...")
  pb2 <- progress_bar$new(total = nrow(master), clear = FALSE, format = "[:bar] :current/:total dob")
  dob_vec <- vector("list", nrow(master))
  for (i in seq_len(nrow(master))) {
    pb2$tick()
    dob_vec[[i]] <- fetch_dob_from_page(master$details_url[i])
  }
  master$dob <- as.Date(unlist(dob_vec), origin = "1970-01-01")
  
  # Compute age
  master <- master %>%
    mutate(age_years = if_else(!is.na(dob),
                               as.numeric(difftime(Sys.Date(), dob, units = "days")) / 365.25,
                               NA_real_))
  
  # Tidy columns and return
  master %>%
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
      w, l, d,
      belt = coalesce(belt, FALSE),
      dob,
      age_years = round(age_years, 2)
    )
}

# ----------------------------- Run ------------------------------------
# Create folder and write CSV
ensure_dir <- function(path) {
  dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
  path
}

out <- build_fighters_metadata()
csv_path <- ensure_dir("data/fighters_metadata.csv")
readr::write_csv(out, csv_path)
message("Wrote ", nrow(out), " rows to ", csv_path)
