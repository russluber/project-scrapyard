# 06_data_cleaning.R
library(tidyverse)
library(here)
library(lubridate)

df <- read_csv(here("data/raw/fight_data_raw_enriched.csv"), show_col_types = FALSE)

# helper: parse "mm:ss" (or "m:ss") into seconds; otherwise NA

parse_mmss <- function(x) {
  x <- stringr::str_squish(as.character(x))
  # tokens that mean zero control time
  zero_tok <- c("", "--", "—", "–")
  is_zero  <- x %in% zero_tok
  x[is_zero] <- "0:00"
  
  # valid shapes: m:ss, mm:ss, or mm:ss:cc
  ok <- stringr::str_detect(x, "^\\d{1,2}:\\d{2}(?::\\d{2})?$")
  
  # collapse mm:ss:cc -> mm:ss only on valid entries
  x_ok <- stringr::str_replace(x[ok], "^(\\d{1,2}):(\\d{2}):(\\d{2})$", "\\1:\\2")
  
  out <- rep(NA_integer_, length(x))
  out[ok] <- as.integer(lubridate::period_to_seconds(lubridate::ms(x_ok)))
  out
}

# --- Weight class & gender ---
classes_re <- regex(
  "Strawweight|Flyweight|Bantamweight|Featherweight|Lightweight|Welterweight|Middleweight|Light Heavyweight|Heavyweight|Catchweight",
  ignore_case = TRUE
)


# ------------------------------------------------------------------------------
# Base df creation
# ------------------------------------------------------------------------------

df <- df %>%
  # Convert all variable names to lowercase
  rename_with(tolower) %>%
  # remove source_path col
  select(-source_path) %>%
  # Create source_url column
  mutate(
    source_url = paste0("http://ufcstats.com/fight-details/", fight_id)
  ) %>%
  # Convert date and reorder
  # mutate(date = as.Date(date, format = "%B %d, %Y")) %>%
  mutate(date = lubridate::ymd(date)) %>%
  arrange(desc(date)) %>%
  # End-of-fight clock (string "time" to seconds)
  mutate(
    end_round_elapsed_s = parse_mmss(time)
  ) %>%
  # Convert fields to categorical variables
  mutate(
    fighter_1_fighter = as.factor(fighter_1_fighter),
    fighter_2_fighter = as.factor(fighter_2_fighter),
    fighter_1_res = as.factor(fighter_1_res),
    fighter_2_res = as.factor(fighter_2_res)
  ) %>%
  # Rename time_format to fight_format; round_finished → end_round_of_fight
  rename(
    fight_format = time_format,
    end_round_of_fight = round_finished
  ) %>%
  # Derive total rounds from fight_format; keep end_round_of_fight as character for now
  mutate(
    rounds = stringr::str_extract(fight_format, "\\d+"),
    end_round_of_fight = as.character(end_round_of_fight)
  ) %>%
  # Split method into family + detail
  separate(
    method,
    into = c("method", "method_detail"),
    sep = "\\s*-\\s*",
    fill = "right",
    extra = "drop"
  ) %>%
  mutate(
    method = stringr::str_trim(method),
    method_detail = stringr::str_trim(method_detail),
    method_detail = replace_na(method_detail, "None"),
    method = factor(method),
    method_detail = factor(method_detail)
  ) %>%
  # Split "Men_Heavyweight" into gender + weight_class and factor
  mutate(
    wc_raw = stringr::str_squish(weight_class),
    wc_raw = stringr::str_replace_all(wc_raw, "’", "'"),   # normalize curly apostrophe
    gender = if_else(stringr::str_detect(wc_raw, regex("^women'?s", ignore_case = TRUE)),
                     "Women", "Men"),
    weight_class = stringr::str_extract(weight_class, classes_re),
    weight_class = if_else(is.na(weight_class), "Catchweight", stringr::str_to_title(weight_class)), 
    gender = if_else(weight_class == "Strawweight", "Women", gender) # Strawweight is women-only in UFC
  ) %>%
  mutate(
    gender = factor(gender, levels = c("Men", "Women")),
    weight_class = factor(
      weight_class,
      levels = c(
        "Strawweight", "Flyweight", "Bantamweight", "Featherweight", "Lightweight",
        "Welterweight", "Middleweight", "Light Heavyweight", "Heavyweight", "Catchweight"
      )
    )
  ) %>%
  select(-wc_raw) %>%
  # Convert referee
  mutate(
    referee = if_else(is.na(referee), "Missing", referee),
    referee = as.factor(referee)
  ) %>%
  # Impute rounds from end_round_of_fight if missing
  mutate(rounds = if_else(is.na(rounds), end_round_of_fight, rounds)) %>%
  # Numeric variants for ordering
  mutate(
    rounds_num = readr::parse_number(rounds),
    end_round_of_fight_num = readr::parse_number(end_round_of_fight)
  ) %>%
  mutate(
    rounds = factor(rounds_num, levels = sort(unique(rounds_num))),
    end_round_of_fight = factor(end_round_of_fight_num, levels = sort(unique(end_round_of_fight_num)))
  ) %>%
  select(-any_of(c("rounds_num", "end_round_of_fight_num", "time")))


# ------------------------------------------------------------------------------
# Fighter 1 data
# ------------------------------------------------------------------------------

fighter_1 <- df %>%
  select(-any_of(c("referee", "fighter_2_res"))) %>%
  rename_with(~ stringr::str_replace(.x, "^fighter_1_", "")) %>%
  rename(
    fighter_id = id,
    opponent = fighter_2_fighter,
    opponent_id = fighter_2_id,
    strikes_landed = strike_landed,
    strikes_thrown = strike_attempts,
    sig_strikes_landed = sig_strike_landed,
    sig_strikes_thrown = sig_strike_attempts,
    strikes_landed_by_opp = fighter_2_strike_landed,
    strikes_thrown_by_opp = fighter_2_strike_attempts,
    sig_strikes_landed_by_opp = fighter_2_sig_strike_landed,
    sig_strikes_thrown_by_opp = fighter_2_sig_strike_attempts,
    kds_scored = kd,
    kds_scored_by_opp = fighter_2_kd,
    tds_landed = td_landed,
    tds_attempted = td_attempts,
    tds_attempted_by_opp = fighter_2_td_attempts,
    tds_landed_by_opp = fighter_2_td_landed,
    subs_attempted = sub_attempts,
    subs_attempted_by_opp = fighter_2_sub_attempts,
    reversals = rev,
    reversals_by_opp = fighter_2_rev,
    ctrl_time = ctrl,
    ctrl_time_for_opp = fighter_2_ctrl
  ) %>%
  # Create additional stats from available
  mutate(
    volume_strikes_landed = pmax(coalesce(strikes_landed, 0) - coalesce(sig_strikes_landed, 0), 0),
    volume_strikes_thrown = pmax(coalesce(strikes_thrown, 0) - coalesce(sig_strikes_thrown, 0), 0),
    strikes_avoided = pmax(coalesce(strikes_thrown_by_opp, 0) - coalesce(strikes_landed_by_opp, 0), 0),
    sig_strikes_avoided = pmax(coalesce(sig_strikes_thrown_by_opp, 0) - coalesce(sig_strikes_landed_by_opp, 0), 0),
    volume_strikes_landed_by_opp = pmax(coalesce(strikes_landed_by_opp, 0) - coalesce(sig_strikes_landed_by_opp, 0), 0), 
    volume_strikes_thrown_by_opp = pmax(coalesce(strikes_thrown_by_opp, 0) - coalesce(sig_strikes_thrown_by_opp, 0), 0),
    volume_strikes_avoided = pmax(coalesce(volume_strikes_thrown_by_opp, 0) - coalesce(volume_strikes_landed_by_opp, 0), 0),
    tds_stuffed = pmax(coalesce(tds_attempted_by_opp, 0) - coalesce(tds_landed_by_opp, 0), 0),
    ctrl_time_s = parse_mmss(ctrl_time),
    ctrl_time_s_for_opp = parse_mmss(ctrl_time_for_opp)
  ) %>%
  # Make count variables integers
  mutate(across(
    c(
      "strikes_landed", "strikes_thrown", "sig_strikes_landed", "sig_strikes_thrown",
      "kds_scored", "tds_landed", "tds_attempted", "subs_attempted", "reversals",
      "ctrl_time_s", "ctrl_time_s_for_opp"
      ), ~ as.integer(.)
    )) %>%
  # Drop opponent-only cols we don't need
  select(-any_of(c(
    "td_percent", "fighter_2_td_percent", "sig_strike_percent", "fighter_2_sig_strike_percent",
    "ctrl_time", "ctrl_time_for_opp"
  )))


# ------------------------------------------------------------------------------
# Fighter 2 data (mirror but for fighter 2 now)
# ------------------------------------------------------------------------------

fighter_2 <- df %>%
  select(-any_of(c("referee", "fighter_1_res"))) %>%
  rename_with(~ stringr::str_replace(.x, "^fighter_2_", "")) %>%
  rename(
    fighter_id = id,
    opponent = fighter_1_fighter,
    opponent_id = fighter_1_id,
    strikes_landed = strike_landed,
    strikes_thrown = strike_attempts,
    sig_strikes_landed = sig_strike_landed,
    sig_strikes_thrown = sig_strike_attempts,
    strikes_landed_by_opp = fighter_1_strike_landed,
    strikes_thrown_by_opp = fighter_1_strike_attempts,
    sig_strikes_landed_by_opp = fighter_1_sig_strike_landed,
    sig_strikes_thrown_by_opp = fighter_1_sig_strike_attempts,
    kds_scored = kd,
    kds_scored_by_opp = fighter_1_kd,
    tds_landed = td_landed,
    tds_attempted = td_attempts,
    tds_attempted_by_opp = fighter_1_td_attempts,
    tds_landed_by_opp = fighter_1_td_landed,
    subs_attempted = sub_attempts,
    subs_attempted_by_opp = fighter_1_sub_attempts,
    reversals = rev,
    reversals_by_opp = fighter_1_rev,
    ctrl_time = ctrl,
    ctrl_time_for_opp = fighter_1_ctrl
  ) %>%
  # Create additional stats from available
  mutate(
    volume_strikes_landed = pmax(coalesce(strikes_landed, 0) - coalesce(sig_strikes_landed, 0), 0),
    volume_strikes_thrown = pmax(coalesce(strikes_thrown, 0) - coalesce(sig_strikes_thrown, 0), 0),
    strikes_avoided = pmax(coalesce(strikes_thrown_by_opp, 0) - coalesce(strikes_landed_by_opp, 0), 0),
    sig_strikes_avoided = pmax(coalesce(sig_strikes_thrown_by_opp, 0) - coalesce(sig_strikes_landed_by_opp, 0), 0),
    volume_strikes_landed_by_opp = pmax(coalesce(strikes_landed_by_opp, 0) - coalesce(sig_strikes_landed_by_opp, 0), 0), 
    volume_strikes_thrown_by_opp = pmax(coalesce(strikes_thrown_by_opp, 0) - coalesce(sig_strikes_thrown_by_opp, 0), 0),
    volume_strikes_avoided = pmax(coalesce(volume_strikes_thrown_by_opp, 0) - coalesce(volume_strikes_landed_by_opp, 0), 0),
    tds_stuffed = pmax(coalesce(tds_attempted_by_opp, 0) - coalesce(tds_landed_by_opp, 0), 0),
    ctrl_time_s = parse_mmss(ctrl_time),
    ctrl_time_s_for_opp = parse_mmss(ctrl_time_for_opp)
  ) %>%
  # Make count variables integers
  mutate(across(
    c(
      "strikes_landed", "strikes_thrown", "sig_strikes_landed", "sig_strikes_thrown",
      "kds_scored", "tds_landed", "tds_attempted", "subs_attempted", "reversals",
      "ctrl_time_s", "ctrl_time_s_for_opp"
    ), ~ as.integer(.)
  )) %>%
  # Drop opponent-only cols we don't need
  select(-any_of(c(
    "td_percent", "fighter_1_td_percent", "sig_strike_percent", "fighter_1_sig_strike_percent",
    "ctrl_time", "ctrl_time_for_opp"
  )))


# ------------------------------------------------------------------------------
# Combining them together
# ------------------------------------------------------------------------------


display_order <- c(
  "source_url", "fight_id", "event_id", "event_name", "date", "res", "fighter_id", "fighter", "strikes_landed", "strikes_thrown",
  "sig_strikes_landed", "sig_strikes_thrown", "volume_strikes_landed", "volume_strikes_thrown",
  "strikes_avoided", "sig_strikes_avoided", "volume_strikes_avoided", "kds_scored", "tds_landed", "tds_attempted", 
  "tds_stuffed", "subs_attempted", "reversals", "ctrl_time_s", "opponent_id", "opponent", "strikes_landed_by_opp", 
  "strikes_thrown_by_opp", "sig_strikes_landed_by_opp", "sig_strikes_thrown_by_opp", "volume_strikes_landed_by_opp",
  "volume_strikes_thrown_by_opp", "kds_scored_by_opp", "tds_landed_by_opp", "tds_attempted_by_opp", 
  "subs_attempted_by_opp", "reversals_by_opp", "ctrl_time_s_for_opp", "gender", "weight_class", 
  "rounds", "method", "method_detail", "fight_format", "end_round_of_fight", "end_round_elapsed_s", "location"
)

# Combine fighter 1 and fighter 2 data
fight_data <- bind_rows(fighter_1, fighter_2) %>%
  select(any_of(display_order), everything())

# Write to CSV
write_csv(fight_data, here("data/clean/fight_data.csv"))