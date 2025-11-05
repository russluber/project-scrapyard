# data_scraping_neat_v3
library(rvest)
library(tidyverse)
library(progress)
library(here)

# ----------------------------- Helpers -----------------------------

# Convert "mm:ss" (or "--") to integer seconds
ctrl_to_seconds <- function(x) {
  x <- str_trim(replace_na(x, "0:00"))
  x <- if_else(x %in% c("--", "—", "–:–", ""), "0:00", x)
  mm <- suppressWarnings(as.integer(str_extract(x, "^[0-9]+")))
  ss <- suppressWarnings(as.integer(str_extract(x, "(?<=:)[0-9]+")))
  mm[is.na(mm)] <- 0L; ss[is.na(ss)] <- 0L
  mm * 60L + ss
}

# --------------------------- Scraping Fns ---------------------------

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
  
  link_html <- read_html(link)
  table_df  <- html_nodes(link_html, "table")
  if (length(table_df) < 1) stop("No tables found on fight page: ", link)
  
  # ---- Totals table (first table) must be 10 columns in modern layout
  raw_totals <- table_df[[1]] %>%
    html_table(trim = TRUE, fill = TRUE) %>%
    dplyr::bind_rows() %>%
    as_tibble()
  
  if (ncol(raw_totals) != 10) {
    stop("Unexpected Totals column count (expected 10, got ", ncol(raw_totals), ") for: ", link)
  }
  
  # Position-based rename to modern schema (NO 'Pass'; include 'Ctrl')
  # Order: Fighter, KD, Sig. str., Sig. str. %, Total str., Td, Td %, Sub. att, Rev., Ctrl
  summary_data <- raw_totals %>%
    rename(
      Fighter             = 1,
      KD                  = 2,
      Sig_Strike          = 3,
      Sig_Strike_Percent  = 4,
      Total_Strikes       = 5,
      TD                  = 6,
      TD_Percent          = 7,
      Sub_Attempts        = 8,
      Rev                 = 9,
      Ctrl                = 10
    ) %>%
    # Long → split two fighters → wide with fighter_1_* / fighter_2_*
    pivot_longer(everything(), names_to = "key", values_to = "value") %>%
    mutate(
      value = str_replace_all(value, "\n", ""),
      value = str_squish(value)
    ) %>%
    separate(value, into = c("fighter_1", "fighter_2"),
             sep = "  +", extra = "merge", fill = "right") %>%
    # If the double-space separator fails, fall back to a single split by two or more spaces
    mutate(
      fighter_2 = if_else(is.na(fighter_2),
                          str_replace(fighter_1, ".*?\\s{2,}", ""),
                          fighter_2),
      fighter_1 = if_else(str_detect(fighter_1, "\\s{2,}"),
                          str_replace(fighter_1, "\\s{2,}.*$", ""),
                          fighter_1)
    ) %>%
    pivot_wider(names_from = key, values_from = c(fighter_1, fighter_2)) %>%
    # Split "X of Y" fields (Sig_Strike, Total_Strikes, TD)
    separate(fighter_1_Sig_Strike,
             into = c("fighter_1_Sig_Strike_Landed", "fighter_1_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    separate(fighter_2_Sig_Strike,
             into = c("fighter_2_Sig_Strike_Landed", "fighter_2_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    separate(fighter_1_Total_Strikes,
             into = c("fighter_1_Strike_Landed", "fighter_1_Strike_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    separate(fighter_2_Total_Strikes,
             into = c("fighter_2_Strike_Landed", "fighter_2_Strike_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    separate(fighter_1_TD,
             into = c("fighter_1_TD_Landed", "fighter_1_TD_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    separate(fighter_2_TD,
             into = c("fighter_2_TD_Landed", "fighter_2_TD_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    # Percent columns to decimals BEFORE any numeric coercion
    mutate(
      fighter_1_Sig_Strike_Percent = as.numeric(str_remove(fighter_1_Sig_Strike_Percent, "%"))/100,
      fighter_2_Sig_Strike_Percent = as.numeric(str_remove(fighter_2_Sig_Strike_Percent, "%"))/100,
      fighter_1_TD_Percent         = as.numeric(str_remove(fighter_1_TD_Percent, "%"))/100,
      fighter_2_TD_Percent         = as.numeric(str_remove(fighter_2_TD_Percent, "%"))/100
    ) %>%
    # Ctrl: keep raw strings and add *_Ctrl_sec numeric
    mutate(
      fighter_1_Ctrl_sec = ctrl_to_seconds(fighter_1_Ctrl),
      fighter_2_Ctrl_sec = ctrl_to_seconds(fighter_2_Ctrl)
    ) %>%
    # Coerce other numeric fields safely (avoid fighter names and raw Ctrl strings)
    mutate(across(
      .cols = c(
        fighter_1_KD, fighter_2_KD,
        fighter_1_Sig_Strike_Landed, fighter_1_Sig_Strike_Attempts,
        fighter_2_Sig_Strike_Landed, fighter_2_Sig_Strike_Attempts,
        fighter_1_Strike_Landed,     fighter_1_Strike_Attempts,
        fighter_2_Strike_Landed,     fighter_2_Strike_Attempts,
        fighter_1_TD_Landed,         fighter_1_TD_Attempts,
        fighter_2_TD_Landed,         fighter_2_TD_Attempts,
        fighter_1_Sub_Attempts,      fighter_2_Sub_Attempts,
        fighter_1_Rev,               fighter_2_Rev
      ),
      ~ suppressWarnings(as.numeric(.x))
    ))
  
  # ---- Fight-level metadata (method, time, round, referee, weight class)
  fight_details <- link_html %>%
    html_nodes(xpath = '//*[contains(concat(" ", @class, " "), " b-fight-details__text ") and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//i') %>%
    html_text() %>%
    tibble(value = .) %>%
    mutate(
      value  = str_replace_all(value, "\n", ""),
      value  = str_trim(value),
      dummy  = value
    ) %>%
    separate(dummy, into = c("feature","val"), sep = ":", extra = "merge", fill = "right") %>%
    mutate(val = str_trim(replace_na(val, ""))) %>%
    filter(val != "") %>%
    select(feature, val) %>%
    distinct() %>%
    pivot_wider(names_from = feature, values_from = val) %>%
    rename_with(~ str_replace_all(., "\\s|/", "_") |> tolower()) %>%
    # winner/loser letter from the persons block
    cbind(
      link_html %>%
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
      link_html %>%
        html_nodes(".b-fight-details__fight-title") %>%
        html_text() %>%
        str_replace_all("\n", "") %>%
        str_squish() %>%
        tibble(weight_class = .)
    )
  
  out <- bind_cols(summary_data, fight_details)
  
  pb$tick()
  Sys.sleep(1/100)
  
  out %>% as_tibble()
}

# NOTE: You said you don't want per-round stats for this pipeline.
# Keeping the function here but updated the rename to match modern 10-col schema.
scrape_round_data <- function(link){
  
  link_html <- read_html(link)
  table_df  <- html_nodes(link_html, "table")
  if (length(table_df) < 2) stop("No per-round table found: ", link)
  
  raw_rounds <- table_df[[2]] %>%
    html_table(trim = TRUE, fill = TRUE) %>%
    dplyr::bind_rows() %>%
    as_tibble(.name_repair = "unique")
  
  # Expect the per-round subtable columns to align to the same 10-stat set
  # (Some pages repeat "Td %" in header; we keep positions consistent.)
  if (ncol(raw_rounds) != 10) {
    stop("Unexpected Round table column count (expected 10, got ", ncol(raw_rounds), ") for: ", link)
  }
  
  round_data <- raw_rounds %>%
    rename(
      Fighter             = 1,
      KD                  = 2,
      Sig_Strike          = 3,
      Sig_Strike_Percent  = 4,
      Total_Strikes       = 5,
      TD                  = 6,
      TD_Percent          = 7,
      Sub_Attempts        = 8,
      Rev                 = 9,
      Ctrl                = 10
    ) %>%
    pivot_longer(everything(), names_to = "key", values_to = "value") %>%
    mutate(
      value = str_replace_all(value, "\n", " "),
      value = str_squish(value)
    ) %>%
    separate(value, into = c("fighter_1", "fighter_2"),
             sep = "  +", extra = "merge", fill = "right") %>%
    mutate(
      fighter_2 = if_else(is.na(fighter_2),
                          str_replace(fighter_1, ".*?\\s{2,}", ""),
                          fighter_2),
      fighter_1 = if_else(str_detect(fighter_1, "\\s{2,}"),
                          str_replace(fighter_1, "\\s{2,}.*$", ""),
                          fighter_1)
    ) %>%
    pivot_wider(names_from = key, values_from = c(fighter_1, fighter_2)) %>%
    unnest(cols = everything(), keep_empty = TRUE) %>%
    mutate(round = row_number()) %>%
    separate(fighter_1_Sig_Strike,
             into = c("fighter_1_Sig_Strike_Landed","fighter_1_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    separate(fighter_2_Sig_Strike,
             into = c("fighter_2_Sig_Strike_Landed","fighter_2_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    separate(fighter_1_Total_Strikes,
             into = c("fighter_1_Strike_Landed","fighter_1_Strike_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    separate(fighter_2_Total_Strikes,
             into = c("fighter_2_Strike_Landed","fighter_2_Strike_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    separate(fighter_1_TD,
             into = c("fighter_1_TD_Landed","fighter_1_TD_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    separate(fighter_2_TD,
             into = c("fighter_2_TD_Landed","fighter_2_TD_Attempts"),
             sep = " of ", extra = "merge", fill = "right") %>%
    mutate(
      fighter_1_Sig_Strike_Percent = as.numeric(str_remove(fighter_1_Sig_Strike_Percent, "%"))/100,
      fighter_2_Sig_Strike_Percent = as.numeric(str_remove(fighter_2_Sig_Strike_Percent, "%"))/100,
      fighter_1_TD_Percent         = as.numeric(str_remove(fighter_1_TD_Percent, "%"))/100,
      fighter_2_TD_Percent         = as.numeric(str_remove(fighter_2_TD_Percent, "%"))/100,
      fighter_1_Ctrl_sec           = ctrl_to_seconds(fighter_1_Ctrl),
      fighter_2_Ctrl_sec           = ctrl_to_seconds(fighter_2_Ctrl)
    ) %>%
    mutate(across(
      .cols = c(
        fighter_1_KD, fighter_2_KD,
        fighter_1_Sig_Strike_Landed, fighter_1_Sig_Strike_Attempts,
        fighter_2_Sig_Strike_Landed, fighter_2_Sig_Strike_Attempts,
        fighter_1_Strike_Landed,     fighter_1_Strike_Attempts,
        fighter_2_Strike_Landed,     fighter_2_Strike_Attempts,
        fighter_1_TD_Landed,         fighter_1_TD_Attempts,
        fighter_2_TD_Landed,         fighter_2_TD_Attempts,
        fighter_1_Sub_Attempts,      fighter_2_Sub_Attempts,
        fighter_1_Rev,               fighter_2_Rev
      ),
      ~ suppressWarnings(as.numeric(.x))
    ))
  
  # Fight details (kept for parity, though you said you won't use round data)
  fight_details <- link_html %>%
    html_nodes(xpath = '//*[contains(concat(" ", @class, " "), " b-fight-details__text ") and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//i') %>%
    html_text() %>%
    tibble(value = .) %>%
    mutate(
      value  = str_replace_all(value, "\n", ""),
      value  = str_trim(value),
      dummy  = value
    ) %>%
    separate(dummy, into = c("feature","val"), sep = ":", extra = "merge", fill = "right") %>%
    mutate(val = str_trim(replace_na(val, ""))) %>%
    filter(val != "") %>%
    select(feature, val) %>%
    distinct() %>%
    pivot_wider(names_from = feature, values_from = val) %>%
    rename_with(~ str_replace_all(., "\\s|/", "_") |> tolower()) %>%
    cbind(
      link_html %>%
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
      link_html %>%
        html_nodes(".b-fight-details__fight-title") %>%
        html_text() %>%
        str_replace_all("\n", "") %>%
        str_squish() %>%
        tibble(weight_class = .)
    )
  
  bind_cols(round_data, fight_details) %>% as_tibble()
}

# --------------------------- Data Scraping ---------------------------

out_path <- here("data/fight_data_raw.csv")

# If fight_data_raw.csv is missing, pretend we have no prior cards
scraped_cards <- if (file.exists(out_path)) {
  readr::read_csv(out_path, show_col_types = FALSE)
} else {
  tibble(cards = character())
}

UFC <- tibble(UFC_Page = "http://ufcstats.com/statistics/events/completed?page=all") %>%
  mutate(cards  = map(UFC_Page, scrape_cards)) %>% unnest(cards) %>%
  anti_join(scraped_cards %>% select(cards) %>% distinct(), by = "cards") %>%
  mutate(dates  = map(cards, scrape_dates))  %>% unnest(dates) %>%
  mutate(fights = map(cards, scrape_fights)) %>% unnest(fights)

if (nrow(UFC) == 0) {
  message("No new cards to scrape.")
  quit(save = "no")  # or just return(invisible(NULL))
}

pb <- progress_bar$new(
  total  = nrow(UFC),
  format = "  downloading [:bar] ETA: :eta :current/:total"
)

UFC_Data <- UFC %>% mutate(fight_data = map(fights, safely(scrape_fight_summary_data)))

# Optional: log failures
UFC_results <- UFC_Data %>% mutate(ok = map_lgl(fight_data, ~ is.null(.x$error)))
fail_log <- UFC_results %>% filter(!ok) %>%
  transmute(cards, fights, error_msg = map_chr(fight_data, ~ .x$error$message))
if (nrow(fail_log) > 0) readr::write_csv(fail_log, here::here("data/scrape_failures.csv"))

# Keep successes
UFC_success <- UFC_results %>% 
  filter(ok) %>% 
  mutate(fight_data = map(fight_data, "result"))

new_rows <- UFC_success %>%
  unnest(fight_data) %>%                     # <- one unnest is enough
  mutate(round_finished = suppressWarnings(as.numeric(round_finished))) %>%
  distinct()


# Write first file if it doesn't exist; otherwise append
final_out <- if (file.exists(out_path)) {
  old <- readr::read_csv(out_path, show_col_types = FALSE)
  bind_rows(old %>% mutate(time = as.character(time)), new_rows)
} else {
  new_rows
}

readr::write_csv(final_out, out_path)
