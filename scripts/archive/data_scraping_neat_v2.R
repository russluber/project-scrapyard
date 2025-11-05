# data_scraping (drop-in, with robust Rev fix and safely() handling)
library(rvest)
library(tidyverse)
library(progress)
library(here)

# --- helpers ---------------------------------------------------------------

# Normalize Unicode dashes and blanks to "0"
dash0 <- function(x) {
  x <- stringr::str_replace_all(x, "[\u2010-\u2015]", "-")  # Unicode dashes -> ASCII
  x <- stringr::str_trim(x)
  dplyr::case_when(
    x == "" ~ "0",
    stringr::str_detect(x, "^[-]+$") ~ "0",
    TRUE ~ x
  )
}

# Robust numeric parser: keep digits and dot; turn others into NA, after dash0
numify <- function(x) {
  x <- dash0(x)
  x <- stringr::str_replace_all(x, "[^0-9.]", "")
  suppressWarnings(as.numeric(x))
}

# Scraping Functions ------------------------------------------------------

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
  summary_data <- table_df[1] %>%
    html_table(trim = TRUE, fill = TRUE) %>%
    do.call("rbind", .) %>%
    as_tibble() %>%
    rename(
      Fighter = 1, KD = 2, Sig_Strike = 3, Sig_Strike_Percent = 4, Total_Strikes = 5,
      TD = 6, TD_Percent = 7, Sub_Attempts = 8, Pass = 9, Rev = 10
    ) %>%
    gather() %>%
    separate(value, into = c("fighter_1", "fighter_2"), sep = "  ", extra = "merge") %>%
    mutate_all(~ str_replace_all(.x, "\n", "") %>% str_trim()) %>%
    pivot_wider(names_from = key, values_from = c(fighter_1, fighter_2)) %>%
    mutate(across(everything(), dash0)) %>%
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
    # percents: strip % then /100
    mutate(across(contains("Percent"), ~ .01 * as.numeric(stringr::str_replace(., "%", "")))) %>%
    # counts: KD, Pass, Rev, Sub_Attempts, Landed/Attempts, etc.
    mutate(across(matches(
      "(_Sig_Strike_(Landed|Attempts)$)|(_Strike_(Landed|Attempts)$)|(_TD_(Landed|Attempts)$)|(_KD$)|(_Pass$)|(_Rev$)|(_Sub_Attempts$)"
    ), numify)) %>%
    mutate(across(matches(
      "(_Sig_Strike_(Landed|Attempts)$)|(_Strike_(Landed|Attempts)$)|(_TD_(Landed|Attempts)$)|(_KD$)|(_Pass$)|(_Rev$)|(_Sub_Attempts$)"
    ), ~ tidyr::replace_na(., 0)))
  
  # Details (method, time, round, etc.)
  fight_details <- link %>%
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
        tibble("fighter_1_res" = .)
    ) %>%
    mutate(
      fighter_2_res = case_when(
        fighter_1_res == "L" ~ "W",
        fighter_1_res == "W" ~ "L",
        TRUE ~ "D"
      )
    ) %>%
    rename("round_finished" = "round") %>%
    cbind(
      link %>%
        html_nodes(".b-fight-details__fight-title") %>%
        html_text() %>%
        str_replace_all("\n", "") %>%
        str_trim() %>%
        tibble(weight_class = .)
    )
  
  summary_data <- cbind(summary_data, fight_details)
  pb$tick()
  Sys.sleep(1/100)
  summary_data %>% as_tibble()
}

scrape_round_data <- function(link){
  link <- link %>% read_html()
  table_df <- link %>% html_nodes("table")
  
  round_data <- table_df[2] %>%
    html_table(trim = TRUE, fill = TRUE) %>%
    do.call("rbind", .) %>%
    as_tibble(.name_repair = "unique") %>%
    rename(
      Fighter = 1, KD = 2, Sig_Strike = 3, Sig_Strike_Percent = 4,
      Total_Strikes = 5, TD = 6, TD_Percent = 7, Sub_Attempts = 8,
      Pass = 9, Rev = 10
    ) %>%
    gather() %>%
    separate(value, into = c("fighter_1", "fighter_2"), sep = "  ", extra = "merge") %>%
    mutate_all(~ str_replace_all(.x, "\n", " ") %>% str_trim()) %>%
    pivot_wider(names_from = key, values_from = c(fighter_1, fighter_2)) %>%
    mutate(across(everything(), dash0)) %>%
    unnest() %>%
    mutate(round = row_number()) %>%
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
    mutate(across(contains("Percent"), ~ .01 * as.numeric(stringr::str_replace(., "%", "")))) %>%
    mutate(across(matches(
      "(_Sig_Strike_(Landed|Attempts)$)|(_Strike_(Landed|Attempts)$)|(_TD_(Landed|Attempts)$)|(_KD$)|(_Pass$)|(_Rev$)|(_Sub_Attempts$)"
    ), numify)) %>%
    mutate(across(matches(
      "(_Sig_Strike_(Landed|Attempts)$)|(_Strike_(Landed|Attempts)$)|(_TD_(Landed|Attempts)$)|(_KD$)|(_Pass$)|(_Rev$)|(_Sub_Attempts$)"
    ), ~ tidyr::replace_na(., 0)))
  
  fight_details <- link %>%
    html_nodes(xpath = '//*[contains(concat( " ", @class, " " ), concat( " ", "b-fight-details__text", " " )) and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//i') %>%
    html_text() %>%
    as_tibble() %>%
    mutate(value = str_replace_all(value, "\n", "") %>%
             str_trim()) %>%
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
        tibble("fighter_1_res" = .)
    ) %>%
    mutate(
      fighter_2_res = case_when(
        fighter_1_res == "L" ~ "W",
        fighter_1_res == "W" ~ "L",
        TRUE ~ "D"
      )
    ) %>%
    rename("round_finished" = "round") %>%
    cbind(
      link %>%
        html_nodes(".b-fight-details__fight-title") %>%
        html_text() %>%
        str_replace_all("\n", "") %>%
        str_trim() %>%
        tibble(weight_class = .)
    )
  
  as_tibble(cbind(round_data, fight_details))
}

# Data Scraping -----------------------------------------------------------

scraped_cards <- read_csv(here("data/fight_data_raw.csv"))

UFC <- tibble(UFC_Page = "http://ufcstats.com/statistics/events/completed?page=all") %>%
  mutate(cards = purrr::map(UFC_Page, scrape_cards)) %>% unnest(cards) %>%
  anti_join(scraped_cards %>% select(cards) %>% distinct(), by = "cards") %>%
  mutate(dates =  purrr::map(cards, scrape_dates)) %>% unnest(dates) %>%
  mutate(fights = purrr::map(cards, scrape_fights)) %>% unnest(fights)

if (nrow(UFC) == 0) {
  message("No new cards to scrape. File left unchanged.")
  quit(save = "no")
}

pb <- progress_bar$new(
  total  = nrow(UFC),
  format = "  downloading [:bar] ETA: :eta :current/:total"
)

# Robust safely() handling: pull result and error explicitly
UFC_Data <- UFC %>%
  mutate(safe = purrr::map(fights, purrr::safely(scrape_fight_summary_data))) %>%
  mutate(
    fight_result = purrr::map(safe, "result"),
    fight_error  = purrr::map(safe, "error")
  ) %>%
  select(-safe)

UFC_Data %>%
  filter(!purrr::map_lgl(fight_result, is.null)) %>%
  unnest(fight_result) %>%
  mutate(round_finished = suppressWarnings(as.numeric(round_finished))) %>%
  distinct() %>%
  bind_rows(scraped_cards %>% mutate(time = as.character(time))) %>%
  write_csv(here("data/fight_data_raw.csv"))
