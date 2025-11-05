library(tidyverse)
library(here)

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
  # Convert fields to categorical variables
  mutate(
    fighter_1_fighter = as.factor(fighter_1_fighter),
    fighter_2_fighter = as.factor(fighter_2_fighter),
    fighter_1_res = as.factor(fighter_1_res),
    fighter_2_res = as.factor(fighter_2_res)
  ) %>%
  # Adjust percents  [guard divisions-by-zero; avoid NaN/Inf]
  mutate(
    fighter_1_sig_strike_percent = if_else(fighter_1_sig_strike_attempts > 0,
      fighter_1_sig_strike_landed / fighter_1_sig_strike_attempts, 0
    ),
    fighter_1_td_percent = if_else(fighter_1_td_attempts > 0,
      fighter_1_td_landed / fighter_1_td_attempts, 0
    ),
    fighter_2_sig_strike_percent = if_else(fighter_2_sig_strike_attempts > 0,
      fighter_2_sig_strike_landed / fighter_2_sig_strike_attempts, 0
    ),
    fighter_2_td_percent = if_else(fighter_2_td_attempts > 0,
      fighter_2_td_landed / fighter_2_td_attempts, 0
    )
  ) %>%
  # If no attempts then percent will be 0 (handle NaN/Inf defensively)  [mutate_at -> across]
  mutate(across(
    contains("percent"),
    ~ case_when(
      is.nan(.x) ~ 0,
      is.infinite(.x) ~ 0,
      TRUE ~ .x
    )
  )) %>%
  # Convert rounds and round_finished  [impute BEFORE factorizing]
  mutate(
    rounds = str_extract(time_format, "\\d"),
    round_finished = as.character(round_finished)
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
  mutate(rounds = if_else(is.na(rounds) == TRUE, round_finished, rounds)) %>%
  # Now factorize rounds and round_finished with stable ascending levels
  mutate(
    rounds_num         = readr::parse_number(rounds),
    round_finished_num = readr::parse_number(round_finished)
  ) %>%
  mutate(
    rounds         = factor(rounds_num, levels = sort(unique(rounds_num))),
    round_finished = factor(round_finished_num, levels = sort(unique(round_finished_num)))
  ) %>%
  select(-rounds_num, -round_finished_num) %>%
  mutate(
    source_url = stringr::str_squish(fights),
    source_url = stringr::str_remove(source_url, "/?$"),
    fight_id   = stringr::str_extract(
      source_url,
      stringr::regex("(?<=fight-details/)[0-9A-Fa-f]{16}", ignore_case = TRUE)
    )
  ) %>%
  select(-fights)
# ------------------------------------------------------------------------------

################################################################################

# ------------------------------------------------------------------------------
# Fighter 1 data
# ------------------------------------------------------------------------------
fighter_1 <- df %>%
  select(-fighter_2_fighter, -referee, -fighter_2_res) %>%
  # rename_all -> rename_with
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
    "strikes_landed" = strike_landed,
    "sig_strikes_landed" = sig_strike_landed,
    "sig_strikes_thrown" = sig_strike_attempts,
    "strikes_thrown" = strike_attempts
  ) %>%
  # Guard divisions and calculate striking metrics
  mutate(
    overall_striking_accuracy = if_else(
      strikes_thrown > 0,
      strikes_landed / strikes_thrown,
      0
    ),
    sig_strikes_prop = if_else(
      strikes_thrown > 0,
      sig_strikes_thrown / strikes_thrown,
      0
    ),
    volume_landed = pmax(coalesce(strikes_landed, 0) - coalesce(sig_strikes_landed, 0), 0),
    volume_thrown = pmax(coalesce(strikes_thrown, 0) - coalesce(sig_strikes_thrown, 0), 0),
    volume_attempted = volume_thrown > 0,
    volume_inputs_missing_attempts = is.na(strikes_thrown) | is.na(sig_strikes_thrown),
    volume_inputs_missing_landed   = is.na(strikes_landed)   | is.na(sig_strikes_landed),
    sig_accuracy = if_else(
      sig_strikes_thrown > 0,
      sig_strikes_landed / sig_strikes_thrown,
      NA_real_
    ),
    volume_accuracy = if_else(
      volume_attempted,
      volume_landed / volume_thrown,
      NA_real_
    )
  ) %>%
  mutate(across(
    c(overall_striking_accuracy, sig_strikes_prop, sig_accuracy, volume_accuracy),
    ~ pmin(pmax(.x, 0), 1)
  )) %>%
  # Make counts integers (storage + sanity)
  mutate(across(
    c(kds_scored, kds_suffered, tds_conceded, tds_stuffed,
      sig_strikes_absorbed, sig_strikes_avoided,
      strikes_absorbed, strikes_avoided,
      strike_thrown, strikes_landed,
      sig_strikes_thrown, sig_strikes_landed, tds_landed),
    ~ as.integer(.)
  )) %>%
  # Remove unnecessary columns for fighter 1's stats
  select(
    -fighter_2_sig_strike_attempts,
    -fighter_2_sig_strike_percent,
    -fighter_2_strike_attempts,
    -fighter_2_td_attempts,
    -fighter_2_td_percent,
    -fighter_2_sub_attempts,
    -fighter_2_pass,
    -fighter_2_rev
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
    "strikes_landed" = strike_landed,
    "sig_strikes_landed" = sig_strike_landed,
    "sig_strikes_thrown" = sig_strike_attempts,
    "strikes_thrown" = strike_attempts
  ) %>%
  # Guard divisions and define striking metrics
  mutate(
    overall_striking_accuracy = if_else(
      strikes_thrown > 0,
      strike_landed / strikes_thrown,
      0
    ),
    sig_strikes_prop = if_else(
      strikes_thrown > 0,
      sig_strikes_thrown / strikes_thrown,
      0
    ),
    volume_landed = pmax(coalesce(strikes_landed, 0) - coalesce(sig_strikes_landed, 0), 0),
    volume_thrown = pmax(coalesce(strikes_thrown, 0) - coalesce(sig_strikes_thrown, 0), 0),
    volume_attempted = volume_thrown > 0,
    volume_inputs_missing_attempts = is.na(strikes_thrown) | is.na(sig_strikes_thrown),
    volume_inputs_missing_landed   = is.na(strikes_landed)   | is.na(sig_strikes_landed),
    volume_accuracy = if_else(
      volume_attempted,
      volume_landed / volume_thrown,
      NA_real_
    ),
    sig_accuracy = if_else(
      sig_strikes_thrown > 0,
      sig_strikes_landed / sig_strikes_thrown,
      NA_real_
    )
  ) %>%
  # Clamp proportions to [0, 1]
  mutate(across(
    c(overall_striking_accuracy, sig_strikes_prop, sig_accuracy, volume_accuracy),
    ~ pmin(pmax(.x, 0), 1)
  )) %>%
  # Make counts integers (storage + sanity)
  mutate(across(
    c(kds_scored, kds_suffered, tds_conceded, tds_stuffed,
      sig_strikes_absorbed, sig_strikes_avoided,
      strikes_absorbed, strikes_avoided,
      strike_thrown, strikes_landed,
      sig_strikes_thrown, sig_strikes_landed, tds_landed),
    ~ as.integer(.)
  )) %>%
  # Remove unnecessary columns for fighter 2's stats
  select(
    -fighter_1_sig_strike_attempts,
    -fighter_1_sig_strike_percent,
    -fighter_1_strike_attempts,
    -fighter_1_td_attempts,
    -fighter_1_td_percent,
    -fighter_1_sub_attempts,
    -fighter_1_pass,
    -fighter_1_rev
  )
# ------------------------------------------------------------------------------

################################################################################

# Sanity checks before combining
stopifnot(!any(grepl("^fighter_\\d+_", names(fighter_1))))
stopifnot(!any(grepl("^fighter_\\d+_", names(fighter_2))))
stopifnot(all(fighter_1$sig_strikes_avoided >= 0, na.rm = TRUE))
stopifnot(all(fighter_2$tds_stuffed >= 0, na.rm = TRUE))

desired_order <- c(
  "fight_id", "date", "fighter", "res",
  "strikes_landed", "strike_attempts", "overall_striking_accuracy",
  "sig_strikes_landed", "sig_strikes_thrown", "sig_strike_percent", "sig_accuracy", "sig_attempt_prop",
  "volume_landed", "volume_thrown", "volume_accuracy", "kds_scored",
  "sig_strikes_avoided", "strikes_avoided",
  "tds_landed", "td_attempts", "td_percent", "tds_stuffed",
  "sub_attempts", "pass", "rev", "kds_suffered", "sig_strikes_absorbed", "strikes_absorbed", "tds_conceded",
  "gender", "weight_class", "rounds", "method", "method_detail",
  "round_finished", "time", "time_format",
  "volume_attempted", "volume_inputs_missing_attempts", "volume_inputs_missing_landed"
)

# Combine fighter 1 and fighter 2 data
fight_data <- bind_rows(fighter_1, fighter_2) %>%
  select(any_of(desired_order), everything())

# Column reordering

write_csv(fight_data, here("data/fight_data.csv"))
