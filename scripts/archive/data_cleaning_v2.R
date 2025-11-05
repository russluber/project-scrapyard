library(tidyverse)
library(here)

df <- read_csv(here("data/fight_data_raw.csv"))

# Clean head-to-head data
df <- df %>%
  # Deselect fields that will not be used in the model
  select(-UFC_Page, -cards, -fights) %>%
  # Convert field name to lower case  [rename_all -> rename_with]
  rename_with(tolower) %>%
  # Convert date
  mutate(date = as.Date(date, format = "%B %d, %Y")) %>%
  arrange(desc(date)) %>%
  # Convert fields to categorical variables (leave method/weight_class for later)
  mutate(
    fighter_1_fighter = as.factor(fighter_1_fighter),
    fighter_2_fighter = as.factor(fighter_2_fighter),
    method            = as.factor(method),
    fighter_1_res     = as.factor(fighter_1_res),
    fighter_2_res     = as.factor(fighter_2_res),
    weight_class      = as.factor(weight_class)
  ) %>%
  
# -------------------------------------------------------
# METHOD: collapse family + detail using your target sets
# -------------------------------------------------------
# Start from raw `method` text like "Decision - Unanimous", split, then map.
separate(method,
         into = c("method_family_raw", "method_detail_raw"),
         sep = "\\s*-\\s*", fill = "right", extra = "drop") %>%
  mutate(
    method_family_raw = stringr::str_to_upper(stringr::str_squish(method_family_raw)),
    method_detail_raw = stringr::str_to_title(stringr::str_squish(coalesce(method_detail_raw, "None"))),
    
    # Collapse to 3 families
    method = case_when(
      str_detect(method_family_raw, "KO|TKO")                         ~ "KO/TKO",
      str_detect(method_family_raw, "SUB|SUBMISSION")                 ~ "Submission",
      str_detect(method_family_raw, "DEC|DECISION|DQ|OTHER|OVERTURN|COULD NOT CONTINUE") ~ "Decision",
      TRUE                                                            ~ "Decision"
    ),
    
    # Detail only meaningful for decisions; otherwise "None"
    method_detail = case_when(
      method == "Decision" & method_detail_raw == "Unanimous" ~ "Unanimous",
      method == "Decision" & method_detail_raw == "Split"     ~ "Split",
      TRUE                                                    ~ "None"
    ),
    
    method        = factor(method,        levels = c("KO/TKO", "Submission", "Decision")),
    method_detail = factor(method_detail, levels = c("Unanimous", "Split", "None"))
  ) %>%
  select(-method_family_raw, -method_detail_raw) %>%
  
# ----------------------------------------------------
# WEIGHT CLASS: split into `gender` and `weight_class`
# ----------------------------------------------------
# Your current data often ends up like "Men_Bantamweight" or similar.
# Handle both already-split and raw textual variants defensively.
mutate(weight_class = as.character(weight_class)) %>%
  # If there's an underscore pattern like "Men_Lightweight", split on "_";
  # otherwise derive from text by detecting "Women" and the base class.
  tidyr::separate_wider_delim(
    weight_class, delim = "_",
    names = c("gender", "weight_class"),
    too_few = "align_start", cols_remove = TRUE
  ) %>%
  mutate(
    # If gender is still NA (no underscore in original), infer it:
    gender = if_else(
      !is.na(gender), gender,
      if_else(str_detect(weight_class, regex("\\bWomen\\b", ignore_case = TRUE)), "Women", "Men")
    ),
    # Extract base class from whatever remains
    weight_class = stringr::str_match(
      weight_class,
      regex(
        "Strawweight|Flyweight|Bantamweight|Featherweight|Lightweight|Welterweight|Middleweight|Light\\s*Heavyweight|Heavyweight|Catchweight",
        ignore_case = TRUE
      )
    )[, 1],
    weight_class = stringr::str_to_title(stringr::str_replace_all(coalesce(weight_class, "Catchweight"), "\\s+", " "))
  ) %>%
  mutate(
    gender = factor(gender, levels = c("Men", "Women")),
    weight_class = factor(
      weight_class,
      levels = c(
        "Strawweight","Flyweight","Bantamweight","Featherweight",
        "Lightweight","Welterweight","Middleweight","Light Heavyweight",
        "Heavyweight","Catchweight"
      )
    )
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
  # (leave the rest of your original pipeline untouched)
  mutate(rounds = if_else(is.na(rounds) == TRUE, round_finished, rounds)) %>%
  mutate(
    rounds_num         = readr::parse_number(rounds),
    round_finished_num = readr::parse_number(round_finished)
  ) %>%
  mutate(
    rounds         = factor(rounds_num, levels = sort(unique(rounds_num))),
    round_finished = factor(round_finished_num, levels = sort(unique(round_finished_num)))
  ) %>%
  select(-rounds_num, -round_finished_num) %>%
  # Create a fight pk for future feature engineering
  mutate(row_num = row_number()) %>%
  arrange(desc(row_num)) %>%
  mutate(fight_pk = row_number()) %>%
  arrange(row_num) %>%
  select(-row_num)

# Fighter 1 data
fighter_1 <- df %>%
  select(-fighter_2_fighter, -referee, -fighter_2_res) %>%
  # rename_all -> rename_with
  rename_with(~ str_replace(.x, "fighter_1_", "")) %>%
  # Create defensive metrics
  mutate(
    "sig_strikes_avoided" = pmax(fighter_2_sig_strike_attempts - fighter_2_sig_strike_landed, 0),
    "tds_stuffed"         = pmax(fighter_2_td_attempts        - fighter_2_td_landed,        0),
    "strikes_avoided"     = pmax(fighter_2_strike_attempts    - fighter_2_strike_landed,    0)
  ) %>%
  # Create damage metrics
  rename(
    "kds_suffered"         = fighter_2_kd,
    "sig_strikes_absorbed" = fighter_2_sig_strike_landed,
    "strikes_absorbed"     = fighter_2_strike_landed,
    "tds_conceded"         = fighter_2_td_landed,
    "kds_scored"           = kd,
    "tds_landed"           = td_landed
  ) %>%
  # Guard divisions and calculate striking metrics
  mutate(
    overall_striking_accuracy = if_else(
      strike_attempts > 0,
      strike_landed / strike_attempts,
      0
    ),
    sig_attempt_prop = if_else(
      strike_attempts > 0,
      sig_strike_attempts / strike_attempts,
      0
    ),
    volume_landed   = pmax(coalesce(strike_landed, 0)   - coalesce(sig_strike_landed, 0),   0),
    volume_attempts = pmax(coalesce(strike_attempts, 0) - coalesce(sig_strike_attempts, 0), 0),
    volume_attempted = volume_attempts > 0,
    volume_inputs_missing_attempts = is.na(strike_attempts) | is.na(sig_strike_attempts),
    volume_inputs_missing_landed   = is.na(strike_landed)   | is.na(sig_strike_landed),
    sig_accuracy = if_else(
      sig_strike_attempts > 0,
      sig_strike_landed / sig_strike_attempts,
      NA_real_
    ),
    volume_accuracy = if_else(
      volume_attempted,
      volume_landed / volume_attempts,
      NA_real_
    )
  ) %>%
  mutate(across(
    c(overall_striking_accuracy, sig_attempt_prop, sig_accuracy, volume_accuracy),
    ~ pmin(pmax(.x, 0), 1)
  )) %>%
  # Make counts integers (storage + sanity)
  mutate(across(
    c(kds_scored, kds_suffered, tds_conceded, tds_stuffed,
      sig_strikes_absorbed, sig_strikes_avoided,
      strikes_absorbed, strikes_avoided,
      strike_attempts, strike_landed,
      sig_strike_attempts, sig_strike_landed),
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

# Fighter 2 data
fighter_2 <- df %>%
  select(-fighter_1_fighter, -referee, -fighter_1_res) %>%
  # rename_all -> rename_with
  rename_with(~ str_replace(.x, "fighter_2_", "")) %>%
  # Create defensive metrics
  mutate(
    "sig_strikes_avoided" = pmax(fighter_1_sig_strike_attempts - fighter_1_sig_strike_landed, 0),
    "tds_stuffed"         = pmax(fighter_1_td_attempts        - fighter_1_td_landed,        0),
    "strikes_avoided"     = pmax(fighter_1_strike_attempts    - fighter_1_strike_landed,    0)
  ) %>%
  # Create damage metrics
  rename(
    "kds_suffered"         = fighter_1_kd,
    "sig_strikes_absorbed" = fighter_1_sig_strike_landed,
    "strikes_absorbed"     = fighter_1_strike_landed,
    "tds_conceded"         = fighter_1_tds_landed,
    "kds_scored"           = kd
  ) %>%
  # Guard divisions and define striking metrics
  mutate(
    overall_striking_accuracy = if_else(
      strike_attempts > 0,
      strike_landed / strike_attempts,
      0
    ),
    sig_attempt_prop = if_else(
      strike_attempts > 0,
      sig_strike_attempts / strike_attempts,
      0
    ),
    volume_landed   = pmax(coalesce(strike_landed, 0)   - coalesce(sig_strike_landed, 0),   0),
    volume_attempts = pmax(coalesce(strike_attempts, 0) - coalesce(sig_strike_attempts, 0), 0),
    volume_attempted = volume_attempts > 0,
    volume_inputs_missing_attempts = is.na(strike_attempts) | is.na(sig_strike_attempts),
    volume_inputs_missing_landed   = is.na(strike_landed)   | is.na(sig_strike_landed),
    volume_accuracy = if_else(
      volume_attempted,
      volume_landed / volume_attempts,
      NA_real_
    ),
    sig_accuracy = if_else(
      sig_strike_attempts > 0,
      sig_strike_landed / sig_strike_attempts,
      NA_real_
    )
  ) %>%
  # Clamp proportions to [0, 1]
  mutate(across(
    c(overall_striking_accuracy, sig_attempt_prop, sig_accuracy, volume_accuracy),
    ~ pmin(pmax(.x, 0), 1)
  )) %>%
  # Make counts integers (storage + sanity)
  mutate(across(
    c(kds_scored, kds_suffered, tds_conceded, tds_stuffed,
      sig_strikes_absorbed, sig_strikes_avoided,
      strikes_absorbed, strikes_avoided,
      strike_attempts, strike_landed,
      sig_strike_attempts, sig_strike_landed),
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

# Sanity checks before combining
stopifnot(!any(grepl("^fighter_\\d+_", names(fighter_1))))
stopifnot(!any(grepl("^fighter_\\d+_", names(fighter_2))))
stopifnot(all(fighter_1$sig_strikes_avoided >= 0, na.rm = TRUE))
stopifnot(all(fighter_2$tds_stuffed >= 0, na.rm = TRUE))

# Combine fighter 1 and fighter 2 data
fight_data <- bind_rows(fighter_1, fighter_2) %>% arrange(desc(fight_pk))

write_csv(fight_data, here("data/fight_data.csv"))
