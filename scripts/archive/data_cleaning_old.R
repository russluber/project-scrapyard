# data_cleaning
library(tidyverse)
library(here)
library(lubridate)

df <- read_csv(here("data/fight_data_raw.csv"), show_col_types = FALSE)

# helper: parse "mm:ss" (or "m:ss") into seconds; otherwise NA
parse_mmss <- function(x) {
  x_chr <- stringr::str_squish(as.character(x))
  # keep mm:ss; if mm:ss:XX is present, collapse to mm:ss
  x_chr <- stringr::str_replace(x_chr, "^(\\d{1,2}):(\\d{2})(?::\\d{2})?$", "\\1:\\2")
  ifelse(
    stringr::str_detect(x_chr, "^\\d{1,2}:\\d{2}$"),
    lubridate::period_to_seconds(lubridate::ms(x_chr)),
    NA_real_
  )
}


# ------------------------------------------------------------------------------
# Base df creation
# ------------------------------------------------------------------------------

df <- df %>%
  # Deselect fields not used in model
  select(-any_of(c("UFC_Page", "cards"))) %>%
  # Convert all variable names to lowercase
  rename_with(tolower) %>%
  # Convert date and reorder
  mutate(date = as.Date(date, format = "%B %d, %Y")) %>%
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
    gender = if_else(stringr::str_starts(weight_class, "Women"), "Women", "Men"),
    weight_class = stringr::str_extract(
      weight_class,
      regex("Strawweight|Flyweight|Bantamweight|Featherweight|Lightweight|Welterweight|Middleweight|Heavyweight|Catchweight", ignore_case = TRUE)
    ),
    weight_class = if_else(is.na(weight_class), "Catchweight", stringr::str_to_title(weight_class))
  ) %>%
  mutate(
    gender = factor(gender, levels = c("Men", "Women")),
    weight_class = factor(
      weight_class,
      levels = c("Strawweight","Flyweight","Bantamweight","Featherweight",
                 "Lightweight","Welterweight","Middleweight","Heavyweight","Catchweight")
    )
  ) %>%
  # Convert referee
  mutate(referee = if_else(is.na(referee), "Missing", referee),
         referee = as.factor(referee)) %>%
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
  select(-rounds_num, -end_round_of_fight_num) %>%
  # Extract fight_id from fights URL, keep a canonical source_url
  mutate(
    source_url = stringr::str_squish(fights),
    source_url = stringr::str_remove(source_url, "/?$"),
    fight_id   = stringr::str_extract(
      source_url,
      stringr::regex("(?<=fight-details/)[0-9A-Fa-f]{16}", ignore_case = TRUE)
    )
  ) %>%
  select(-any_of(c("fights", "time")))




# ------------------------------------------------------------------------------
# Fighter 1 data
# ------------------------------------------------------------------------------

fighter_1 <- df %>%
  # Remove irrelevant fighter_2 stats for fighter_1
  select(-any_of(c("fighter_2_fighter", "referee", "fighter_2_res"))) %>%
  # Drop prefixes for fighter_1 specific variables
  rename_with(~ stringr::str_replace(.x, "^fighter_1_", "")) %>%
  # Defensive metrics (opponent = fighter_2_*)
  mutate(
    sig_strikes_avoided = pmax(fighter_2_sig_strike_attempts - fighter_2_sig_strike_landed, 0, na.rm = TRUE),
    tds_stuffed         = pmax(fighter_2_td_attempts - fighter_2_td_landed, 0, na.rm = TRUE),
    strikes_avoided     = pmax(fighter_2_strike_attempts - fighter_2_strike_landed, 0, na.rm = TRUE)
  ) %>%
  # Damage + own totals
  rename(
    kds_suffered          = fighter_2_kd,
    sig_strikes_absorbed  = fighter_2_sig_strike_landed,
    strikes_absorbed      = fighter_2_strike_landed,
    tds_conceded          = fighter_2_td_landed,
    kds_scored            = kd,
    tds_landed            = td_landed,
    tds_attempted         = td_attempts,
    strikes_landed        = strike_landed,
    sig_strikes_landed    = sig_strike_landed,
    sig_strikes_thrown    = sig_strike_attempts,
    strikes_thrown        = strike_attempts,
    subs_attempted        = sub_attempts,
    reversals             = rev, 
    ctrl_time             = ctrl
  ) %>%
  # Control time seconds
  mutate(
    ctrl_time_s = if ("ctrl_time" %in% names(dplyr::cur_data()))
      parse_mmss(ctrl_time)
    else NA_real_
  ) %>%
  # Volume strikes
  mutate(
    volume_landed = pmax(coalesce(strikes_landed, 0) - coalesce(sig_strikes_landed, 0), 0),
    volume_thrown = pmax(coalesce(strikes_thrown, 0) - coalesce(sig_strikes_thrown, 0), 0),
    volume_attempted = volume_thrown > 0,
    volume_inputs_missing_attempts = is.na(strikes_thrown) | is.na(sig_strikes_thrown),
    volume_inputs_missing_landed   = is.na(strikes_landed) | is.na(sig_strikes_landed)
  ) %>%
  # Make counts integers
  mutate(across(
    c(kds_scored, kds_suffered, tds_conceded, tds_stuffed,
      sig_strikes_absorbed, sig_strikes_avoided, subs_attempted,
      strikes_absorbed, strikes_avoided,
      strikes_thrown, strikes_landed,
      sig_strikes_thrown, sig_strikes_landed, tds_landed, tds_attempted,
      volume_landed, volume_thrown, reversals),
    ~ as.integer(.)
  )) %>%
  # Drop opponent-only columns we no longer need
  select(-any_of(c(
    "fighter_2_sig_strike_attempts", "fighter_2_strike_attempts",
    "fighter_2_td_attempts", "fighter_2_sub_attempts", "fighter_2_rev",
    "fighter_2_sig_strike_percent", "fighter_2_td_percent", "ctrl_time",
    "sig_strike_percent", "td_percent", "fighter_2_ctrl"
  )))



# ------------------------------------------------------------------------------
# Fighter 2 data (mirror but for fighter 2 now)
# ------------------------------------------------------------------------------
fighter_2 <- df %>%
  select(-any_of(c("fighter_1_fighter", "referee", "fighter_1_res"))) %>%
  rename_with(~ stringr::str_replace(.x, "^fighter_2_", "")) %>%
  mutate(
    sig_strikes_avoided = pmax(fighter_1_sig_strike_attempts - fighter_1_sig_strike_landed, 0, na.rm = TRUE),
    tds_stuffed         = pmax(fighter_1_td_attempts - fighter_1_td_landed, 0, na.rm = TRUE),
    strikes_avoided     = pmax(fighter_1_strike_attempts - fighter_1_strike_landed, 0, na.rm = TRUE)
  ) %>%
  rename(
    kds_suffered          = fighter_1_kd,
    sig_strikes_absorbed  = fighter_1_sig_strike_landed,
    strikes_absorbed      = fighter_1_strike_landed,
    tds_conceded          = fighter_1_td_landed,
    kds_scored            = kd,
    tds_landed            = td_landed,
    tds_attempted         = td_attempts,
    strikes_landed        = strike_landed,
    sig_strikes_landed    = sig_strike_landed,
    sig_strikes_thrown    = sig_strike_attempts,
    strikes_thrown        = strike_attempts,
    subs_attempted        = sub_attempts,
    reversals             = rev,
    ctrl_time             = ctrl
  ) %>%
  # Control time seconds
  mutate(
    ctrl_time_s = if ("ctrl_time" %in% names(dplyr::cur_data()))
      parse_mmss(ctrl_time)
    else NA_real_
  ) %>%
  mutate(
    volume_landed = pmax(coalesce(strikes_landed, 0) - coalesce(sig_strikes_landed, 0), 0),
    volume_thrown = pmax(coalesce(strikes_thrown, 0) - coalesce(sig_strikes_thrown, 0), 0),
    volume_attempted = volume_thrown > 0,
    volume_inputs_missing_attempts = is.na(strikes_thrown) | is.na(sig_strikes_thrown),
    volume_inputs_missing_landed   = is.na(strikes_landed) | is.na(sig_strikes_landed)
  ) %>%
  mutate(across(
    c(kds_scored, kds_suffered, tds_conceded, tds_stuffed,
      sig_strikes_absorbed, sig_strikes_avoided, subs_attempted,
      strikes_absorbed, strikes_avoided,
      strikes_thrown, strikes_landed,
      sig_strikes_thrown, sig_strikes_landed, tds_landed, tds_attempted,
      volume_landed, volume_thrown, reversals),
    ~ as.integer(.)
  )) %>%
  select(-any_of(c(
    "fighter_1_sig_strike_attempts", "fighter_1_strike_attempts",
    "fighter_1_td_attempts", "fighter_1_sub_attempts", "fighter_1_rev",
    "fighter_1_sig_strike_percent", "fighter_1_td_percent", "ctrl_time",
    "sig_strike_percent", "td_percent", "fighter_1_ctrl"
  )))

# ------------------------------------------------------------------------------
# Combining them together
# ------------------------------------------------------------------------------


display_order <- c(
  "fight_id", "date", "fighter", "res", "strikes_landed", "strikes_thrown",
  "sig_strikes_landed", "sig_strikes_thrown", "volume_landed", "volume_thrown",
  "strikes_avoided", "sig_strikes_avoided", "kds_scored",
  "tds_landed", "tds_attempted", "tds_stuffed", "subs_attempted", "reversals", 
  "ctrl_time_s", "kds_suffered", "sig_strikes_absorbed",
  "strikes_absorbed", "tds_conceded", "gender", "weight_class", "rounds",
  "method", "method_detail", "fight_format", "end_round_of_fight", 
  "end_round_elapsed_s", "volume_attempted", 
  "volume_inputs_missing_attempts", "volume_inputs_missing_landed"
)

# Combine fighter 1 and fighter 2 data
fight_data <- bind_rows(fighter_1, fighter_2) %>%
  select(any_of(display_order), everything())

# Write to CSV
write_csv(fight_data, here("data/fight_data.csv"))