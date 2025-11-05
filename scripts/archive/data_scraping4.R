# data_scraping
library(rvest)
library(tidyverse)
library(progress)
library(here)

SLEEP_DELAY <- 0.25  # be polite to the site

# ----------------------------- Scraping Functions -----------------------------

scrape_cards <- function(link){
  link %>%
    read_html() %>%
    html_nodes(".b-link_style_black") %>%
    html_attr("href") %>%
    tibble(cards = .)
}

scrape_dates <- function(link){
  link %>%
    read_html() %>%
    html_nodes(".b-list__box-list-item:nth-child(1)") %>%
    html_text() %>%
    tibble(fight_date = .) %>%
    separate(fight_date, into = c("key", "value"), sep = ":") %>%
    select(date = value) %>%
    mutate(
      date = str_replace_all(date, "\n", ""),
      date = str_trim(date)
      # date = lubridate::mdy(date)  # optional: parse to Date
    )
}

scrape_fights <- function(link){
  link %>%
    read_html() %>%
    html_nodes("a") %>%
    html_attr("href") %>%
    tibble(fights = .) %>%
    filter(str_detect(fights, "fight-details"))
}

scrape_fight_summary_data <- function(link){
  link <- read_html(link)
  
  # Grab the first table
  table_nodes <- html_nodes(link, "table")
  if (length(table_nodes) < 1) stop("No tables found")
  
  tbl_list <- html_table(table_nodes[1], trim = TRUE, fill = TRUE)
  if (length(tbl_list) == 0) stop("First table empty")
  
  # ---- extract fighter hrefs and IDs from the first table (scoped to col 1) ----
  fighter_anchors <- table_nodes[1] %>%
    html_element("tbody") %>%
    html_elements("tr td:nth-child(1) a.b-link[href*='fighter-details']")
  
  # fallback if <tbody> wasn’t parsed explicitly
  if (length(fighter_anchors) == 0) {
    fighter_anchors <- table_nodes[1] %>%
      html_elements("tr td:nth-child(1) a.b-link[href*='fighter-details']")
  }
  
  if (length(fighter_anchors) == 0) {
    fighter_anchors <- link %>%
      html_elements(".b-fight-details__persons .b-fight-details__person-name a.b-fight-details__person-link")
  }
  
  # fighter_hrefs <- fighter_anchors %>% html_attr("href") %>% head(2)
  
  fighter_hrefs <- fighter_anchors %>%
    html_attr("href") %>%
    stringr::str_squish() %>%
    head(2)
  
  fighter_ids <- fighter_hrefs %>%
    purrr::map_chr(~ {
      if (is.na(.x) || length(.x) == 0) return(NA_character_)
      sub(".*/fighter-details/([^/?#]+).*", "\\1", .x)
    })
  
  # ensure length 2 (pad with NAs if a page is weird)
  fighter_ids <- c(fighter_ids, rep(NA_character_, 2 - length(fighter_ids)))[1:2]
  
  fighter_1_id <- fighter_ids[1]
  fighter_2_id <- fighter_ids[2]
  
  # ---------------------------------------------------------------
  
  summary_data <- bind_rows(tbl_list) %>%
    as_tibble() %>%
    rename(
      "Fighter" = 1, "KD" = 2, "Sig_Strike" = 3, "Sig_Strike_Percent" = 4,
      "Total_Strikes" = 5, "TD" = 6, "TD_Percent" = 7, "Sub_Attempts" = 8,
      "Rev" = 9, "Ctrl" = 10
    ) %>%
    pivot_longer(cols = everything(), names_to = "key", values_to = "value") %>%
    separate(value, into = c("fighter_1", "fighter_2"),
             sep = "\\s{2,}", extra = "merge", fill = "right") %>%
    mutate(across(everything(), stringr::str_squish)) %>%
    mutate(across(everything(), ~ na_if(.x, ""))) %>%
    pivot_wider(names_from = key, values_from = c(fighter_1, fighter_2)) %>%
    separate(fighter_1_Sig_Strike,
             into = c("fighter_1_Sig_Strike_Landed", "fighter_1_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_Sig_Strike,
             into = c("fighter_2_Sig_Strike_Landed", "fighter_2_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_1_Total_Strikes,
             into = c("fighter_1_Strike_Landed", "fighter_1_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_Total_Strikes,
             into = c("fighter_2_Strike_Landed", "fighter_2_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_1_TD,
             into = c("fighter_1_TD_Landed", "fighter_1_TD_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_TD,
             into = c("fighter_2_TD_Landed", "fighter_2_TD_Attempts"),
             sep = " of ", extra = "merge") %>%
    mutate(across(contains("Percent"),
                  ~ as.numeric(str_remove(.x, "%")) * 0.01)) %>%
    mutate(across(-matches("(^fighter_1$|^fighter_2$|Fighter|Ctrl$)"),
                  ~ suppressWarnings(as.numeric(.x))))
  
  # # outcome parsing
  # res_text <- link %>%
  #   html_node(".b-fight-details__persons") %>%
  #   html_text() %>%
  #   stringr::str_squish()
  # 
  # fighter_1_res <- dplyr::case_when(
  #   stringr::str_detect(res_text, stringr::regex("\\bNC\\b|No\\s*Contest|Overturned", ignore_case = TRUE)) ~ "NC",
  #   stringr::str_detect(res_text, stringr::regex("\\bD\\b|Majority\\s*Draw|Split\\s*Draw|Unanimous\\s*Draw|\\bDraw\\b", ignore_case = TRUE)) ~ "D",
  #   stringr::str_detect(res_text, "\\bW\\b") ~ "W",
  #   stringr::str_detect(res_text, "\\bL\\b") ~ "L",
  #   TRUE ~ NA_character_
  # )
  # 
  # fighter_2_res <- dplyr::case_when(
  #   fighter_1_res %in% c("NC", "D") ~ fighter_1_res,
  #   fighter_1_res == "W" ~ "L",
  #   fighter_1_res == "L" ~ "W",
  #   TRUE ~ NA_character_
  # )
  
  # --- outcome parsing (robust: read per-fighter status badges) ---
  # badges show one of: W / L / D / NC
  statuses <- link %>%
    html_elements(".b-fight-details__persons .b-fight-details__person .b-fight-details__person-status") %>%
    html_text2() %>%
    stringr::str_squish() %>%
    toupper()
  
  # pad to length 2 in case the page is odd
  statuses <- c(statuses, rep(NA_character_, 2 - length(statuses)))[1:2]
  
  fighter_1_res <- if (statuses[1] %in% c("W","L","D","NC")) statuses[1] else NA_character_
  fighter_2_res <- if (statuses[2] %in% c("W","L","D","NC")) statuses[2] else NA_character_
  
  
  fight_details_text <- link %>%
    html_nodes(xpath = '//*[contains(concat(" ", @class, " "), " b-fight-details__text ") and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//i') %>%
    html_text()
  
  fight_details <- tibble(value = fight_details_text) %>%
    mutate(value = str_squish(value)) %>%
    separate(value, into = c("feature", "value"), sep = ":", extra = "merge") %>%
    mutate(value = str_trim(value)) %>%
    replace_na(list(value = "")) %>%
    filter(value != "") %>%
    pivot_wider(names_from = feature, values_from = value) %>%
    rename_with(~ str_replace_all(.x, "\\s|/", "_") %>% tolower()) %>%
    rename(round_finished = round)
  
  title_text <- link %>%
    html_nodes(".b-fight-details__fight-title") %>%
    html_text() %>%
    str_replace_all("\n", "") %>%
    str_trim()
  
  fight_details <- fight_details %>%
    bind_cols(tibble(
      fighter_1_res = fighter_1_res,
      fighter_2_res = fighter_2_res
    )) %>%
    bind_cols(tibble(weight_class = title_text)) %>%
    mutate(
      weight_class = str_squish(weight_class),
      weight_class = str_replace(weight_class, ".*-\\s*", "")
    )
  
  id_cols <- tibble(
    fighter_1_id = fighter_1_id,
    fighter_2_id = fighter_2_id
  )
  # ---------------------------------------------------------------
  
  out <- dplyr::bind_cols(summary_data, id_cols, fight_details)
  
  if (exists("pb", inherits = FALSE)) pb$tick()
  Sys.sleep(SLEEP_DELAY)
  
  tibble::as_tibble(out)
}


# ----------------------------- Data Scraping -----------------------------

out_path <- here("data/fight_data_raw.csv")

# Prior file (or stub)
if (file.exists(out_path)) {
  fight_data_raw <- readr::read_csv(out_path, show_col_types = FALSE)
} else {
  fight_data_raw <- tibble(cards = character())
}

# Helpers for safely() NULL -> empty tibble
empty_cards  <- tibble(cards  = character())
empty_dates  <- tibble(date   = character())
empty_fights <- tibble(fights = character())

# Prepare wrappers
safe_cards  <- purrr::safely(scrape_cards)
safe_dates  <- purrr::safely(scrape_dates)
safe_fights <- purrr::safely(scrape_fights)

# Start table
UFC <- tibble(UFC_Page = "http://ufcstats.com/statistics/events/completed?page=all")

# cards (safe)
UFC$cards_try       <- purrr::map(UFC$UFC_Page, function(x) safe_cards(x))
UFC$cards_result    <- purrr::map(UFC$cards_try,  "result")
UFC$cards_error     <- purrr::map(UFC$cards_try,  "error")
UFC$cards_error_msg <- purrr::map_chr(UFC$cards_error, function(e) if (is.null(e)) NA_character_ else conditionMessage(e))
UFC$cards           <- purrr::map(UFC$cards_result, function(x) if (is.null(x)) empty_cards else x)
UFC                <- tidyr::unnest(UFC, cols = cards)
UFC                <- dplyr::distinct(UFC, cards, .keep_all = TRUE)

# de-dup previously processed cards if present
cards_done <- fight_data_raw %>% dplyr::select(any_of("cards")) %>% dplyr::distinct()
if ("cards" %in% names(cards_done)) {
  UFC <- dplyr::anti_join(UFC, cards_done, by = "cards")
}

# dates (safe)
UFC$dates_try       <- purrr::map(UFC$cards, function(x) safe_dates(x))
UFC$dates_result    <- purrr::map(UFC$dates_try, "result")
UFC$dates_error     <- purrr::map(UFC$dates_try, "error")
UFC$dates_error_msg <- purrr::map_chr(UFC$dates_error, function(e) if (is.null(e)) NA_character_ else conditionMessage(e))
UFC$dates           <- purrr::map(UFC$dates_result, function(x) if (is.null(x)) empty_dates else x)
UFC                <- tidyr::unnest(UFC, cols = dates)

# fights (safe)
UFC$fights_try       <- purrr::map(UFC$cards, function(x) safe_fights(x))
UFC$fights_result    <- purrr::map(UFC$fights_try, "result")
UFC$fights_error     <- purrr::map(UFC$fights_try, "error")
UFC$fights_error_msg <- purrr::map_chr(UFC$fights_error, function(e) if (is.null(e)) NA_character_ else conditionMessage(e))
UFC$fights           <- purrr::map(UFC$fights_result, function(x) if (is.null(x)) empty_fights else x)
UFC                 <- tidyr::unnest(UFC, cols = fights)

# write early-stage error log (only rows with at least one non-NA error msg)
error_log <- UFC %>%
  dplyr::transmute(cards, cards_error_msg, dates_error_msg, fights_error_msg)
err_mask <- !is.na(error_log$cards_error_msg) | !is.na(error_log$dates_error_msg) | !is.na(error_log$fights_error_msg)
if (any(err_mask)) {
  readr::write_csv(error_log[err_mask, , drop = FALSE], here("data/scrape_errors_cards_dates_fights.csv"))
}

# nothing to do?
if (nrow(UFC) == 0L) {
  message("No new cards to scrape.")
  readr::write_csv(fight_data_raw, out_path)
} else {
  pb <- progress_bar$new(
    total  = nrow(UFC),
    format = "  downloading [:bar] ETA: :eta :current/:total"
  )
  
  # fight pages (safe)
  safe_fight <- purrr::safely(scrape_fight_summary_data)
  UFC$fight_try <- purrr::map(UFC$fights, function(x) safe_fight(x))
  UFC$result    <- purrr::map(UFC$fight_try, "result")
  UFC$error     <- purrr::map(UFC$fight_try, "error")
  
  keep_mask <- purrr::map_lgl(UFC$error, is.null)
  UFC_ok    <- UFC[keep_mask, , drop = FALSE]
  
  # optional: write fight-level errors
  fight_err <- UFC[!keep_mask, c("cards", "fights"), drop = FALSE]
  if (nrow(fight_err) > 0) {
    fight_err$error_msg <- purrr::map_chr(UFC$error[!keep_mask], function(e) if (is.null(e)) NA_character_ else conditionMessage(e))
    readr::write_csv(fight_err, here("data/scrape_errors_fight_pages.csv"))
  }
  
  UFC_Data <- tidyr::unnest(UFC_ok, cols = result)
  
  new_rows <- UFC_Data %>%
    mutate(round_finished = suppressWarnings(as.integer(round_finished))) %>%
    distinct()
  
  # append or create
  if (file.exists(out_path)) {
    if ("time" %in% names(fight_data_raw)) fight_data_raw$time <- as.character(fight_data_raw$time)
    if ("time" %in% names(new_rows))       new_rows$time       <- as.character(new_rows$time)
    fight_data_raw <- dplyr::bind_rows(fight_data_raw, new_rows)
  } else {
    fight_data_raw <- new_rows
  }
  
  readr::write_csv(fight_data_raw, out_path)
}
