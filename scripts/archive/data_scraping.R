# data_scraping
library(rvest)
library(tidyverse)
library(progress)
library(here)

# Scraping Functions ------------------------------------------------------

scrape_cards <- function(link){
  link %>% 
    read_html() %>% 
    html_nodes(".b-link_style_black") %>% 
    html_attr("href") %>% 
    tibble("cards" = .)
}

scrape_dates <- function(link){
  link %>% 
    read_html() %>% 
    html_nodes(".b-list__box-list-item:nth-child(1)") %>% 
    html_text() %>% 
    tibble("fight_date" = .) %>% 
    separate(fight_date, into = c("key", "value"), sep = ":") %>% 
    select(date = value) %>% 
    mutate(date = str_replace_all(date, "\n","")) %>% 
    mutate(date = str_trim(date))
  
}

scrape_fights <- function(link){
  link %>% 
    read_html() %>% 
    html_nodes("a") %>% 
    html_attr("href") %>% 
    tibble("fights" = .) %>% 
    filter(str_detect(fights, "fight-details"))
  
}

scrape_fight_summary_data <- function(link){
  link <- link %>% read_html()
  table_df <- link %>% html_nodes("table")
  
  summary_data <- table_df[1] %>% 
    html_table(trim = TRUE, fill = TRUE) %>% 
    do.call("rbind", .) %>% 
    as_tibble() %>% 
    rename("Fighter" = 1, "KD" = 2, "Sig_Strike" = 3, "Sig_Strike_Percent" = 4, "Total_Strikes" = 5, 
           "TD" = 6, "TD_Percent" = 7, "Sub_Attempts" = 8, "Rev" = 9, "Ctrl" = 10) %>% 
    gather() %>% 
    separate(value, into = c("fighter_1", "fighter_2"), sep = "  ", extra = "merge") %>% 
    mutate_all(~str_replace_all(.x, "\n", "")) %>% 
    mutate_all(str_trim) %>% 
    pivot_wider(names_from = key, values_from = c(fighter_1, fighter_2)) %>% 
    separate(fighter_1_Sig_Strike, into = c("fighter_1_Sig_Strike_Landed", "fighter_1_Sig_Strike_Attempts"), 
             sep = " of ", extra = "merge") %>% 
    separate(fighter_2_Sig_Strike, into = c("fighter_2_Sig_Strike_Landed", "fighter_2_Sig_Strike_Attempts"), 
             sep = " of ", extra = "merge") %>% 
    separate(fighter_1_Total_Strikes, into = c("fighter_1_Strike_Landed", "fighter_1_Strike_Attempts"), 
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_Total_Strikes, into = c("fighter_2_Strike_Landed", "fighter_2_Strike_Attempts"), 
             sep = " of ", extra = "merge") %>%
    separate(fighter_1_TD, into = c("fighter_1_TD_Landed", "fighter_1_TD_Attempts"), 
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_TD, into = c("fighter_2_TD_Landed", "fighter_2_TD_Attempts"), 
             sep = " of ", extra = "merge") %>% 
    mutate_at(vars(contains("Percent")), ~.01*str_replace(.x, "%", "") %>% as.numeric()) %>% 
    mutate_at(vars(-contains("Fighter", ignore.case = FALSE), -matches("Ctrl$")), as.numeric) 
  
  
  fight_details <- link %>% 
    html_nodes(xpath = '//*[contains(concat( " ", @class, " " ), concat( " ", "b-fight-details__text", " " )) and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//i') %>% 
    html_text() %>% 
    as_tibble() %>% 
    mutate(value = str_replace_all(value, "\n", "")) %>% 
    mutate(value = str_trim(value, side = "both")) %>% 
    separate(value, into = c("feature", "value"), sep = ":", extra = "merge") %>% 
    mutate(value = str_trim(value)) %>% 
    replace_na(list(value = "")) %>% 
    group_by(feature) %>% 
    filter(value != "") %>% 
    ungroup() %>% 
    pivot_wider(names_from = feature, values_from = value) %>% 
    rename_all(.funs = ~str_replace(.x, "\\s|/", "_") %>% tolower()) %>% 
    cbind(
      link %>% 
        html_node(".b-fight-details__persons") %>% 
        html_text() %>% 
        str_extract("[:upper:]{1}") %>% 
        tibble("fighter_1_res" = .)) %>% 
    mutate(fighter_2_res = case_when(
      fighter_1_res == "L" ~ "W",
      fighter_1_res == "W" ~ "L",
      TRUE ~ "D"
    )) %>% 
    rename("round_finished" = "round") %>% 
    cbind(
      link %>%       
        html_nodes(".b-fight-details__fight-title") %>% 
        html_text() %>% 
        str_replace_all("\n", "") %>% 
        str_trim() %>% 
        tibble(weight_class = .))
  
  summary_data <- cbind(summary_data, fight_details)
  pb$tick()
  Sys.sleep(1/100)
  summary_data %>% as_tibble()
}

# Data Scraping -----------------------------------------------------------

out_path <- here("data/fight_data_raw.csv")

# If fight_data_raw.csv doesn't exist yet, pretend we've scraped nothing
fight_data_raw <- if (file.exists(out_path)) {
  readr::read_csv(out_path, show_col_types = FALSE)
} else {
  tibble(cards = character())   # enough for anti_join below
}

UFC <- tibble(UFC_Page = "http://ufcstats.com/statistics/events/completed?page=all") %>% 
  mutate(cards = map(UFC_Page, scrape_cards)) %>% unnest(cards) %>% 
  anti_join(fight_data_raw %>% select(cards) %>% distinct(), by = "cards") %>% 
  mutate(dates =  map(cards, scrape_dates)) %>% unnest(dates) %>% 
  mutate(fights = map(cards, scrape_fights)) %>% unnest(fights)

pb <- progress_bar$new(total = nrow(UFC),
                       format = "  downloading [:bar] ETA: :eta :current/:total")

UFC_Data <- UFC %>% mutate(fight_data = map(fights, safely(scrape_fight_summary_data)))

new_rows <- UFC_Data %>% 
  unnest(fight_data) %>% 
  filter(row_number() %% 2 != 0) %>% 
  unnest(fight_data) %>% 
  mutate(round_finished = as.numeric(round_finished)) %>% 
  distinct()

# If the file exists, append to it; otherwise create it
fight_data_raw <- if (file.exists(out_path)) {
  bind_rows(fight_data_raw %>% mutate(time = as.character(time)), new_rows)
} else {
  new_rows
}

readr::write_csv(fight_data_raw, out_path)

