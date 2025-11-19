# scripts/09_make_striking_data.R

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(lubridate)
  library(here)
})

fight_data <- read_csv(
  here("data", "clean", "fight_data.csv"),
  show_col_types = FALSE
)


# Cutoff for Unified Rules era
unified_rules_adopted = as.Date("2000-11-01")

# Build striking model dataset
striking_df <- fight_data %>%
  select(
    fight_id,
    date,
    res,
    fighter_id,
    fighter,
    sig_strikes_landed,
    sig_strikes_thrown,
    opponent_id,
    opponent,
    gender,
    weight_class
  ) %>%
  mutate(
    fight_id = as.character(fight_id),
    date = as.Date(date),
    res = as.character(res),
    fighter_id = factor(fighter_id),
    fighter = as.character(fighter),
    sig_strikes_landed = as.numeric(sig_strikes_landed),
    sig_strikes_thrown = as.numeric(sig_strikes_thrown),
    opponent_id = factor(opponent_id),
    opponent = as.character(opponent),
    gender = factor(gender),
    weight_class = factor(weight_class)
  ) %>%
  filter(
    date >= unified_rules_adopted,
    sig_strikes_thrown > 0
  )
  
  
  
  
out_dir <- here("data", "model")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)


# Write to file  
write_rds(striking_df, file = file.path(out_dir, "striking_df.rds"))
write_csv(striking_df, file = file.path(out_dir, "striking_df.csv"))