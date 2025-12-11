# scripts/make_perf_diff_data.R

suppressPackageStartupMessages({
  library(tidyverse)
  library(dplyr)
  library(readr)
  library(lubridate)
  library(here)
})

fight_data <- read_csv(
  here("data", "clean", "fight_data.csv"),
  show_col_types = FALSE
)


# Start of unified rules adoption
unified_rules_adopted = as.Date("2000-11-01")

# Build win probability model dataset
win_df <- fight_data %>%
  select(
    date, 
    fighter_id,
    fighter,
    fight_id,
    opponent,
    res,
    sig_strikes_landed,
    sig_strikes_landed_by_opp,
    kds_scored,
    kds_scored_by_opp,
    tds_landed,
    tds_landed_by_opp,
    ctrl_time_s,
    ctrl_time_s_for_opp,
  ) %>%
  mutate(date = as.Date(date)) %>%
  filter(
    date >= unified_rules_adopted,
    res %in% c("W", "L")
  ) %>%
  mutate(
    fighter_id = factor(fighter_id),
    fighter = as.character(fighter),
    fight_id = factor(fight_id),
    opponent = as.character(opponent),
    res = factor(res, levels = c("L", "W")),
    # Binarize fight outcome variable to 1 (W) and 0 (L)
    res_win = if_else(res == "W", 1L, 0L),
    sig_strikes_landed = as.numeric(sig_strikes_landed),
    sig_strikes_landed_by_opp = as.numeric(sig_strikes_landed_by_opp),
    ctrl_time_s = as.numeric(ctrl_time_s),
    ctrl_time_s_for_opp = as.numeric(ctrl_time_s_for_opp),
    kds_scored = as.numeric(kds_scored),
    kds_scored_by_opp = as.numeric(kds_scored_by_opp),
    tds_landed = as.numeric(tds_landed),
    tds_landed_by_opp = as.numeric(tds_landed_by_opp),
    # Striking and grappling differential variables - predictors
    sig_strike_diff = sig_strikes_landed - sig_strikes_landed_by_opp,
    kd_diff = kds_scored - kds_scored_by_opp,
    td_diff = tds_landed - tds_landed_by_opp,
    ctrl_time_diff = ctrl_time_s - ctrl_time_s_for_opp,
  ) %>%
  mutate(
    # Standardized fight performance differential variables
    sig_strike_diff_z = as.numeric(scale(sig_strike_diff)),
    kd_diff_z = as.numeric(scale(kd_diff)),
    td_diff_z = as.numeric(scale(td_diff)),
    ctrl_time_diff_z = as.numeric(ctrl_time_diff)
  )




out_dir <- here("data", "model")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)


# Write to file  
write_rds(win_df, file = file.path(out_dir, "win_perf_diffs_df.rds"))
write_csv(win_df, file = file.path(out_dir, "win_perf_diffs_df.csv"))