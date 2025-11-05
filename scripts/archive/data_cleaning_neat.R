library(tidyverse)
library(here)
library(lubridate)

df <- read_csv(here("data/fight_data_raw.csv"))

# ------------------------------------------------------------------------------
# Base df creation
# ------------------------------------------------------------------------------
df <- df %>%
  # Deselect fields that will not be used in the model
  select(-UFC_Page, -cards) %>%
  # Convert field name to lower case  [rename_all -> rename_with]
  rename_with(tolower) %>%
  # Convert date
  mutate(date = as.Date(date, format = "%B %d, %Y")) %>%
  arrange(desc(date)) %>%
  # Make time more accurately descriptive
  mutate(
    time_chr = stringr::str_trim(as.character(time)),
    # If it looks like mm:ss:XX, keep only mm:ss
    time_chr = stringr::str_replace(time_chr, "^(\\d{1,2}):(\\d{2}):\\d{2}$", "\\1:\\2"),
    # Now parse mm:ss to seconds
    end_round_elapsed_s = dplyr::if_else(
      stringr::str_detect(time_chr, "^\\d{1,2}:\\d{2}$"),
      as.numeric(lubridate::ms(time_chr)),
      NA_real_
    )
  ) %>%
  select(-time_chr) %>%
  # Convert fields to categorical variables
  mutate(
    fighter_1_fighter = as.factor(fighter_1_fighter),
    fighter_2_fighter = as.factor(fighter_2_fighter),
    fighter_1_res = as.factor(fighter_1_res),
    fighter_2_res = as.factor(fighter_2_res)
  ) %>%
  # Rename time_format to fight_format (e.g. 5 Rnd (5-5-5-5-5) = 5 rounds, 5 minutes each)
  rename(
    "fight_format" = time_format,
    "end_round_of_fight" = round_finished
  ) %>%
  # Convert rounds and end_round_of_fight
  mutate(
    rounds = str_extract(fight_format, "\\d+"),
    end_round_of_fight = as.character(end_round_of_fight)
  ) %>%
  # Convert method  [keep family + detail; also keep a 'method' collapsed to family]
  separate(
    method,
    into = c("method", "method_detail"),
    sep = "\\s*-\\s*",
    fill = "right",
    extra = "drop"
  ) %>%
  mutate(
    method = str_trim(method),
    method_detail = str_trim(method_detail),
    method_detail = replace_na(method_detail, "None")
  ) %>%
  mutate(
    method = factor(method),
    method_detail = factor(method_detail)
  ) %>%
  # Split "Men_Heavyweight" into gender + weight_class and factor
  mutate(
    gender = if_else(str_starts(weight_class, "Women"), "Women", "Men"),
    weight_class = str_extract(
      weight_class,
      regex(
        "Strawweight|Flyweight|Bantamweight|Featherweight|Lightweight|Welterweight|Middleweight|Heavyweight|Catchweight",
        ignore_case = TRUE
      )
    ),
    weight_class = if_else(is.na(weight_class), "Catchweight", str_to_title(weight_class))
  ) %>%
  mutate(
    gender = factor(gender, levels = c("Men", "Women")),
    weight_class = factor(
      weight_class,
      levels = c(
        "Strawweight","Flyweight","Bantamweight","Featherweight",
        "Lightweight","Welterweight","Middleweight","Heavyweight","Catchweight"
      )
    )
  ) %>%
  # Convert referee
  mutate(referee = if_else(is.na(referee) == TRUE, "Missing", referee)) %>%
  mutate(referee = as.factor(referee)) %>%
  # Impute rounds from round_finished if missing (still characters here)
  mutate(
    rounds = dplyr::if_else(is.na(rounds), end_round_of_fight, rounds)
  ) %>%
  # Now factorize rounds and round_finished with stable ascending levels
  mutate(
    rounds_num         = readr::parse_number(rounds),
    end_round_of_fight_num = readr::parse_number(end_round_of_fight)
  ) %>%
  mutate(
    rounds         = factor(rounds_num, levels = sort(unique(rounds_num))),
    end_round_of_fight = factor(end_round_of_fight_num, levels = sort(unique(end_round_of_fight_num)))
  ) %>%
  select(-rounds_num, -end_round_of_fight_num) %>%
  mutate(
    source_url = stringr::str_squish(fights),
    source_url = stringr::str_remove(source_url, "/?$"),
    fight_id   = stringr::str_extract(
      source_url,
      stringr::regex("(?<=fight-details/)[0-9A-Fa-f]{16}", ignore_case = TRUE)
    )
  ) %>%
  select(-fights, -time)
# ------------------------------------------------------------------------------

################################################################################

# ------------------------------------------------------------------------------
# Fighter 1 data
# ------------------------------------------------------------------------------
fighter_1 <- df %>%
  # Get rid of fighter_2 specific stuff
  select(-fighter_2_fighter, -referee, -fighter_2_res) %>%
  # rename_all -> rename_with
  # drop fighter_1_ prefixes for relevant cols
  rename_with(~ str_replace(.x, "fighter_1_", "")) %>%
  # Create defensive metrics
  mutate(
    "sig_strikes_avoided" = pmax(fighter_2_sig_strike_attempts - fighter_2_sig_strike_landed, 0),
    "tds_stuffed" = pmax(fighter_2_td_attempts - fighter_2_td_landed, 0),
    "strikes_avoided" = pmax(fighter_2_strike_attempts - fighter_2_strike_landed, 0)
  ) %>%
  # Create damage metrics
  rename(
    "kds_suffered" = fighter_2_kd,
    "sig_strikes_absorbed" = fighter_2_sig_strike_landed,
    "strikes_absorbed" = fighter_2_strike_landed,
    "tds_conceded" = fighter_2_td_landed,
    "kds_scored" = kd,
    "tds_landed" = td_landed,
    "tds_attempted" = td_attempts,
    "strikes_landed" = strike_landed,
    "sig_strikes_landed" = sig_strike_landed,
    "sig_strikes_thrown" = sig_strike_attempts,
    "strikes_thrown" = strike_attempts,
    "subs_attempted" = sub_attempts
  ) %>%
  mutate(
    volume_landed = pmax(coalesce(strikes_landed, 0) - coalesce(sig_strikes_landed, 0), 0),
    volume_thrown = pmax(coalesce(strikes_thrown, 0) - coalesce(sig_strikes_thrown, 0), 0),
    volume_attempted = volume_thrown > 0,
    volume_inputs_missing_attempts = is.na(strikes_thrown) | is.na(sig_strikes_thrown),
    volume_inputs_missing_landed   = is.na(strikes_landed) | is.na(sig_strikes_landed)
  ) %>%
  # Make counts integers (storage + sanity)
  mutate(across(
    c(kds_scored, kds_suffered, tds_conceded, tds_stuffed,
      sig_strikes_absorbed, sig_strikes_avoided, subs_attempted,
      strikes_absorbed, strikes_avoided,
      strikes_thrown, strikes_landed,
      sig_strikes_thrown, sig_strikes_landed, tds_landed, volume_landed,
      volume_thrown),
    ~ as.integer(.)
  )) %>%
  # Remove unnecessary columns for fighter 1's stats
  select(
    -fighter_2_sig_strike_attempts,
    -fighter_2_strike_attempts,
    -fighter_2_td_attempts,
    -fighter_2_sub_attempts,
    -fighter_2_pass,
    -fighter_2_rev,
    -fighter_2_sig_strike_percent,
    -fighter_2_td_percent
  )
# ------------------------------------------------------------------------------

################################################################################

# ------------------------------------------------------------------------------
# Fighter 2 data
# ------------------------------------------------------------------------------
fighter_2 <- df %>%
  select(-fighter_1_fighter, -referee, -fighter_1_res) %>%
  # rename_all -> rename_with
  rename_with(~ str_replace(.x, "fighter_2_", "")) %>%
  # Create defensive metrics
  mutate(
    "sig_strikes_avoided" = pmax(fighter_1_sig_strike_attempts - fighter_1_sig_strike_landed, 0),
    "tds_stuffed" = pmax(fighter_1_td_attempts - fighter_1_td_landed, 0),
    "strikes_avoided" = pmax(fighter_1_strike_attempts - fighter_1_strike_landed, 0)
  ) %>%
  # Create damage metrics
  rename(
    "kds_suffered" = fighter_1_kd,
    "sig_strikes_absorbed" = fighter_1_sig_strike_landed,
    "strikes_absorbed" = fighter_1_strike_landed,
    "tds_conceded" = fighter_1_td_landed,
    "kds_scored" = kd,
    "tds_landed" = td_landed,
    "tds_attempted" = td_attempts,
    "strikes_landed" = strike_landed,
    "sig_strikes_landed" = sig_strike_landed,
    "sig_strikes_thrown" = sig_strike_attempts,
    "strikes_thrown" = strike_attempts,
    "subs_attempted" = sub_attempts
  ) %>%
  mutate(
    volume_landed = pmax(coalesce(strikes_landed, 0) - coalesce(sig_strikes_landed, 0), 0),
    volume_thrown = pmax(coalesce(strikes_thrown, 0) - coalesce(sig_strikes_thrown, 0), 0),
    volume_attempted = volume_thrown > 0,
    volume_inputs_missing_attempts = is.na(strikes_thrown) | is.na(sig_strikes_thrown),
    volume_inputs_missing_landed   = is.na(strikes_landed) | is.na(sig_strikes_landed)
  ) %>%
  # Make counts integers (storage + sanity)
  mutate(across(
    c(kds_scored, kds_suffered, tds_conceded, tds_stuffed,
      sig_strikes_absorbed, sig_strikes_avoided, subs_attempted,
      strikes_absorbed, strikes_avoided,
      strikes_thrown, strikes_landed,
      sig_strikes_thrown, sig_strikes_landed, tds_landed, volume_landed,
      volume_thrown),
    ~ as.integer(.)
  )) %>%
  # Remove unnecessary columns for fighter 2's stats
  select(
    -fighter_1_sig_strike_attempts,
    -fighter_1_strike_attempts,
    -fighter_1_td_attempts,
    -fighter_1_sub_attempts,
    -fighter_1_pass,
    -fighter_1_rev,
    -fighter_1_sig_strike_percent,
    -fighter_1_td_percent
  )
# ------------------------------------------------------------------------------

################################################################################


display_order <- c(
  "fight_id", "date", "fighter", "res", "strikes_landed", "strikes_thrown",
  "sig_strikes_landed", "sig_strikes_thrown", "volume_landed", "volume_thrown",
  "strikes_avoided", "sig_strikes_avoided", "kds_scored",
  "tds_landed", "tds_attempted", "tds_stuffed", "subs_attempted", "rev", "kds_suffered", "sig_strikes_absorbed",
  "strikes_absorbed", "tds_conceded", "gender", "weight_class", "rounds",
  "method", "method_detail", "fight_format", "end_round_of_fight", 
  "end_round_elapsed_s", "volume_attempted", 
  "volume_inputs_missing_attempts", "volume_inputs_missing_landed"
)

# Combine fighter 1 and fighter 2 data
fight_data <- bind_rows(fighter_1, fighter_2) %>%
  select(-any_of(c("sig_strike_percent", "td_percent"))) %>%
  select(any_of(display_order), everything())

# Write to CSV
write_csv(fight_data, here("data/fight_data.csv"))
