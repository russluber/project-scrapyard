# data_scraping_neat
library(rvest)
library(tidyverse)
library(progress)
library(here)
library(purrr)
library(tidyr)
library(dplyr)
library(stringr)
library(readr)

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
      date = str_replace_all(date, "\n",""),
      date = str_trim(date)
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
  link <- link %>% read_html()
  table_df <- link %>% html_nodes("table")
  
  # Summary table (top totals)
  summary_data <-
    table_df[1] %>%
    html_table(trim = TRUE, fill = TRUE) %>%
    do.call("rbind", .) %>%
    as_tibble() %>%
    rename(
      Fighter = 1, KD = 2, Sig_Strike = 3, Sig_Strike_Percent = 4,
      Total_Strikes = 5, TD = 6, TD_Percent = 7, Sub_Attempts = 8,
      Pass = 9, Rev = 10
    ) %>%
    gather(key, value) %>%
    separate(value, into = c("fighter_1", "fighter_2"), sep = "  ", extra = "merge") %>%
    mutate(across(everything(), ~ str_replace_all(.x, "\n", "") %>% str_trim())) %>%
    pivot_wider(names_from = key, values_from = c(fighter_1, fighter_2)) %>%
    # normalize dashes BEFORE numeric coercions so Rev etc. don't become NA
    mutate(across(everything(), ~ str_replace(.x, "^-$", "0"))) %>%
    separate(fighter_1_Sig_Strike, into = c("fighter_1_Sig_Strike_Landed","fighter_1_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_Sig_Strike, into = c("fighter_2_Sig_Strike_Landed","fighter_2_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_1_Total_Strikes, into = c("fighter_1_Strike_Landed","fighter_1_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_Total_Strikes, into = c("fighter_2_Strike_Landed","fighter_2_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_1_TD, into = c("fighter_1_TD_Landed","fighter_1_TD_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_TD, into = c("fighter_2_TD_Landed","fighter_2_TD_Attempts"),
             sep = " of ", extra = "merge") %>%
    mutate_at(vars(contains("Percent")), ~ .01 * str_replace(.x, "%", "") %>% as.numeric()) %>%
    mutate_at(vars(-contains("Fighter", ignore.case = FALSE)), as.numeric)
  
  # Details (method, time, round, etc.)
  fight_details <-
    link %>%
    html_nodes(xpath = '//*[contains(concat( " ", @class, " " ), concat( " ", "b-fight-details__text", " " )) and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//i') %>%
    html_text() %>%
    as_tibble() %>%
    mutate(value = str_replace_all(value, "\n", "") %>% str_trim()) %>%
    separate(value, into = c("feature","value"), sep = ":", extra = "merge") %>%
    mutate(value = str_trim(value)) %>%
    replace_na(list(value = "")) %>%
    group_by(feature) %>%
    filter(value != "") %>%
    ungroup() %>%
    pivot_wider(names_from = feature, values_from = value) %>%
    rename_all(~ str_replace(.x, "\\s|/", "_") %>% tolower()) %>%
    cbind(
      link %>%
        html_node(".b-fight-details__persons") %>%
        html_text() %>%
        str_extract("[:upper:]{1}") %>%
        tibble(fighter_1_res = .)
    ) %>%
    mutate(
      fighter_2_res = case_when(
        fighter_1_res == "L" ~ "W",
        fighter_1_res == "W" ~ "L",
        TRUE ~ "D"
      )
    ) %>%
    rename(round_finished = round) %>%
    cbind(
      link %>%
        html_nodes(".b-fight-details__fight-title") %>%
        html_text() %>%
        str_replace_all("\n", "") %>%
        str_trim() %>%
        tibble(weight_class = .)
    )
  
  out <- cbind(summary_data, fight_details)
  pb$tick()
  Sys.sleep(1/100)
  as_tibble(out)
}

scrape_round_data <- function(link){
  link <- link %>% read_html()
  table_df <- link %>% html_nodes("table")
  
  round_data <-
    table_df[2] %>%
    html_table(trim = TRUE, fill = TRUE) %>%
    do.call("rbind", .) %>%
    as_tibble(.name_repair = "unique") %>%
    rename(
      Fighter = 1, KD = 2, Sig_Strike = 3, Sig_Strike_Percent = 4,
      Total_Strikes = 5, TD = 6, TD_Percent = 7, Sub_Attempts = 8,
      Pass = 9, Rev = 10
    ) %>%
    gather(key, value) %>%
    separate(value, into = c("fighter_1", "fighter_2"), sep = "  ", extra = "merge") %>%
    mutate(across(everything(), ~ str_replace_all(.x, "\n", " ") %>% str_trim())) %>%
    pivot_wider(names_from = key, values_from = c(fighter_1, fighter_2)) %>%
    unnest() %>%
    mutate(round = row_number()) %>%
    # normalize dashes BEFORE numeric coercions
    mutate(across(everything(), ~ str_replace(.x, "^-$", "0"))) %>%
    separate(fighter_1_Sig_Strike, into = c("fighter_1_Sig_Strike_Landed","fighter_1_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_Sig_Strike, into = c("fighter_2_Sig_Strike_Landed","fighter_2_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_1_Total_Strikes, into = c("fighter_1_Strike_Landed","fighter_1_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_Total_Strikes, into = c("fighter_2_Strike_Landed","fighter_2_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_1_TD, into = c("fighter_1_TD_Landed","fighter_1_TD_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_TD, into = c("fighter_2_TD_Landed","fighter_2_TD_Attempts"),
             sep = " of ", extra = "merge") %>%
    mutate_at(vars(contains("Percent")), ~ .01 * str_replace(.x, "%", "") %>% as.numeric()) %>%
    mutate_at(vars(-contains("Fighter", ignore.case = FALSE)), as.numeric)
  
  fight_details <-
    link %>%
    html_nodes(xpath = '//*[contains(concat( " ", @class, " " ), concat( " ", "b-fight-details__text", " " )) and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//i') %>%
    html_text() %>%
    as_tibble() %>%
    mutate(value = str_replace_all(value, "\n", "") %>% str_trim()) %>%
    separate(value, into = c("feature","value"), sep = ":", extra = "merge") %>%
    mutate(value = str_trim(value)) %>%
    replace_na(list(value = "")) %>%
    group_by(feature) %>%
    filter(value != "") %>%
    ungroup() %>%
    pivot_wider(names_from = feature, values_from = value) %>%
    rename_all(~ str_replace(.x, "\\s|/", "_") %>% tolower()) %>%
    cbind(
      link %>%
        html_node(".b-fight-details__persons") %>%
        html_text() %>%
        str_extract("[:upper:]{1}") %>%
        tibble(fighter_1_res = .)
    ) %>%
    mutate(
      fighter_2_res = case_when(
        fighter_1_res == "L" ~ "W",
        fighter_1_res == "W" ~ "L",
        TRUE ~ "D"
      )
    ) %>%
    rename(round_finished = round) %>%
    cbind(
      link %>%
        html_nodes(".b-fight-details__fight-title") %>%
        html_text() %>%
        str_replace_all("\n", "") %>%
        str_trim() %>%
        tibble(weight_class = .)
    )
  
  out <- cbind(round_data, fight_details)
  as_tibble(out)
}

# ------------------------------- Data Scraping -------------------------------

raw_path <- here("data/fight_data_raw.csv")

# If the raw file doesn't exist yet, create an empty tibble with the columns we use
if (file.exists(raw_path)) {
  scraped_cards <- readr::read_csv(raw_path, show_col_types = FALSE)
} else {
  scraped_cards <- tibble(
    cards = character(),   # used by anti_join()
    time  = character()    # used by mutate(..., time = as.character(time)) before bind_rows
  )
}

UFC <- tibble(UFC_Page = "http://ufcstats.com/statistics/events/completed?page=all") %>%
  mutate(cards  = purrr::map(UFC_Page, scrape_cards)) %>% unnest(cards) %>%
  # Remove cards that have already been scraped (safe even if scraped_cards is empty)
  anti_join(scraped_cards %>% select(cards) %>% distinct(), by = "cards") %>%
  mutate(dates  = purrr::map(cards, scrape_dates)) %>% unnest(dates) %>%
  mutate(fights = purrr::map(cards, scrape_fights)) %>% unnest(fights)

pb <- progress_bar$new(
  total  = nrow(UFC),
  format = "  downloading [:bar] ETA: :eta :current/:total"
)

UFC_Data <- UFC %>%
  mutate(fight_data = purrr::map(fights, purrr::safely(scrape_fight_summary_data)))

current_scrape <- UFC_Data %>%
  tidyr::unnest_wider(fight_data) %>%
  dplyr::filter(!purrr::map_lgl(result, is.null)) %>%
  tidyr::unnest(result) %>%
  # be robust to either 'round_finished' or 'round' existing
  dplyr::rename(round_tmp = dplyr::any_of("round")) %>%
  dplyr::mutate(
    round_finished = suppressWarnings(as.numeric(dplyr::coalesce(.data$round_finished, .data$round_tmp)))
  ) %>%
  dplyr::select(-round_tmp) %>%
  dplyr::distinct()

# Only bind with previous raw file if we actually had one
out <- if (nrow(scraped_cards) > 0) {
  dplyr::bind_rows(current_scrape, scraped_cards %>% dplyr::mutate(time = as.character(time))) %>%
    dplyr::distinct()
} else {
  current_scrape
}

readr::write_csv(out, raw_path)
