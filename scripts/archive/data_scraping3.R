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
  
  # --- Guard: summary table presence/shape ---
  table_nodes <- link %>% html_nodes("table")
  if (length(table_nodes) < 1) stop("No tables found")
  
  tbl_list <- table_nodes[1] %>% html_table(trim = TRUE, fill = TRUE)
  if (length(tbl_list) == 0) stop("First table empty")
  
  summary_data <- bind_rows(tbl_list) %>%
    as_tibble() %>%
    rename(
      "Fighter" = 1, "KD" = 2, "Sig_Strike" = 3, "Sig_Strike_Percent" = 4,
      "Total_Strikes" = 5, "TD" = 6, "TD_Percent" = 7, "Sub_Attempts" = 8,
      "Rev" = 9, "Ctrl" = 10
    ) %>%
    # pivot_longer replaces deprecated gather()
    pivot_longer(cols = everything(), names_to = "key", values_to = "value") %>%
    # split two fighters by flexible whitespace
    separate(value, into = c("fighter_1", "fighter_2"),
             sep = "\\s{2,}", extra = "merge", fill = "right") %>%
    mutate(across(everything(), stringr::str_squish)) %>%
    # replace empty strings with NA so downstream separates don't misbehave
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
  
  # --- Fight details + outcome parsing (NC/Draw/W/L aware) ---
  res_text <- link %>%
    html_node(".b-fight-details__persons") %>%
    html_text() %>%
    stringr::str_squish()
  
  fighter_1_res <- dplyr::case_when(
    stringr::str_detect(res_text, stringr::regex("\\bNC\\b|No\\s*Contest|Overturned", ignore_case = TRUE)) ~ "NC",
    stringr::str_detect(res_text, stringr::regex("\\bD\\b|Majority\\s*Draw|Split\\s*Draw|Unanimous\\s*Draw|\\bDraw\\b", ignore_case = TRUE)) ~ "D",
    stringr::str_detect(res_text, "\\bW\\b") ~ "W",
    stringr::str_detect(res_text, "\\bL\\b") ~ "L",
    TRUE ~ NA_character_
  )
  
  fighter_2_res <- dplyr::case_when(
    fighter_1_res %in% c("NC", "D") ~ fighter_1_res,
    fighter_1_res == "W" ~ "L",
    fighter_1_res == "L" ~ "W",
    TRUE ~ NA_character_
  )
  
  fight_details <- link %>%
    html_nodes(xpath = '//*[contains(concat(" ", @class, " "), " b-fight-details__text ") and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//i') %>%
    html_text() %>%
    tibble(value = .) %>%
    mutate(value = str_squish(value)) %>%
    separate(value, into = c("feature", "value"), sep = ":", extra = "merge") %>%
    mutate(value = str_trim(value)) %>%
    replace_na(list(value = "")) %>%
    filter(value != "") %>%
    pivot_wider(names_from = feature, values_from = value) %>%
    rename_with(~ str_replace_all(.x, "\\s|/", "_") %>% tolower()) %>%
    rename(round_finished = round) %>%
    bind_cols(
      tibble(
        fighter_1_res = fighter_1_res,
        fighter_2_res = fighter_2_res
      )
    ) %>%
    bind_cols(
      link %>%
        html_nodes(".b-fight-details__fight-title") %>%
        html_text() %>%
        str_replace_all("\n", "") %>%
        str_trim() %>%
        tibble(weight_class = .)
    ) %>%
    mutate(
      weight_class = str_squish(weight_class),
      # keep tail after dash if present (Helps isolate "Welterweight" from "Event - Welterweight")
      weight_class = str_replace(weight_class, ".*-\\s*", "")
    )
  
  # combine safely for tibbles
  out <- dplyr::bind_cols(summary_data, fight_details)
  
  # progress tick (relies on global pb during batch runs)
  if (exists("pb", inherits = FALSE)) pb$tick()
  Sys.sleep(SLEEP_DELAY)
  
  tibble::as_tibble(out)
}

# ----------------------------- Data Scraping -----------------------------

out_path <- here("data/fight_data_raw.csv")

# If fight_data_raw.csv doesn't exist yet, pretend we've scraped nothing
fight_data_raw <- if (file.exists(out_path)) {
  readr::read_csv(out_path, show_col_types = FALSE)
} else {
  tibble(cards = character())   # enough for anti_join below
}

UFC <- tibble(UFC_Page = "http://ufcstats.com/statistics/events/completed?page=all") %>%
  mutate(cards  = purrr::map(UFC_Page, scrape_cards)) %>%
  tidyr::unnest(cards) %>%
  mutate(dates  = purrr::map(cards, scrape_dates))  %>% tidyr::unnest(dates) %>%
  mutate(fights = purrr::map(cards, scrape_fights)) %>% tidyr::unnest(fights)

# Guard anti_join: only if 'cards' exists in prior file
cards_done <- fight_data_raw %>% select(any_of("cards")) %>% distinct()
if ("cards" %in% names(cards_done)) {
  UFC <- UFC %>% anti_join(cards_done, by = "cards")
}

# If no new cards, bail early
if (nrow(UFC) == 0L) {
  message("No new cards to scrape.")
  readr::write_csv(fight_data_raw, out_path)
} else {
  pb <- progress_bar$new(
    total  = nrow(UFC),
    format = "  downloading [:bar] ETA: :eta :current/:total"
  )
  
  # Wrap with safely() and unpack result/error correctly
  UFC_Data <- UFC %>%
    mutate(fight_data = purrr::map(fights, ~ purrr::safely(scrape_fight_summary_data)(.x))) %>%
    tidyr::unnest_wider(fight_data) %>%                      # -> columns: result, error
    dplyr::filter(purrr::map_lgl(error, is.null)) %>%
    dplyr::select(-error) %>%
    tidyr::unnest(result)
  
  new_rows <- UFC_Data %>%
    mutate(round_finished = suppressWarnings(as.integer(round_finished))) %>%
    distinct()
  
  # Append or create
  if (file.exists(out_path)) {
    # only coerce 'time' if present (older files might not have it)
    fight_data_raw <- fight_data_raw %>% mutate(across(any_of("time"), as.character))
    new_rows       <- new_rows       %>% mutate(across(any_of("time"), as.character))
    fight_data_raw <- dplyr::bind_rows(fight_data_raw, new_rows)
  } else {
    fight_data_raw <- new_rows
  }
  
  readr::write_csv(fight_data_raw, out_path)
}
