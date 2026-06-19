# scripts/striking_accuracy/make_data.R
#
# Research-question step (striking accuracy): build the model-ready
# datasets consumed by the final report, from the cleaned fight data.
#
# This is NOT part of the core data pipeline (scripts/00-02); run it
# directly after that pipeline has produced data/clean/fight_data.csv.
#
# Produces the two datasets the report's models use:
#   - striking_df       : one row per fighter-fight, significant strikes
#                         landed/thrown + covariates (RQ2 accuracy model)
#   - win_perf_diffs_df : one row per fighter-fight, standardized in-fight
#                         performance differentials (RQ1 win model)
#
# Both are written as .rds (used by the report) and .csv (for inspection).
#
# Outputs:
#   data/model/striking_df.{rds,csv}
#   data/model/win_perf_diffs_df.{rds,csv}

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(lubridate)
  library(here)
})

# Analysis window: the Unified Rules era onward.
UNIFIED_RULES_ADOPTED <- as.Date("2000-11-01")

OUT_DIR <- here("data", "model")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

fight_data <- read_csv(here("data", "clean", "fight_data.csv"), show_col_types = FALSE)

# -------------------------------------------------------------------
# 1) striking_df  — significant-strike accuracy model (RQ2)
#
# One row per fighter-fight with landed/thrown counts and the structural
# covariates (gender, weight class) plus fighter/fight ids for the
# multilevel model. Keep only fights where the fighter threw >= 1 strike.
# -------------------------------------------------------------------
striking_df <- fight_data %>%
  select(
    fight_id, date, res, fighter_id, fighter,
    sig_strikes_landed, sig_strikes_thrown,
    opponent_id, opponent, gender, weight_class
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
  filter(date >= UNIFIED_RULES_ADOPTED, sig_strikes_thrown > 0)

write_rds(striking_df, file.path(OUT_DIR, "striking_df.rds"))
write_csv(striking_df, file.path(OUT_DIR, "striking_df.csv"))

# -------------------------------------------------------------------
# 2) win_perf_diffs_df  — win-probability model (RQ1)
#
# One row per fighter-fight with the four in-fight performance
# differentials (fighter minus opponent) and their standardized (z-score)
# versions, so logistic-regression coefficients are per-1-SD.
# -------------------------------------------------------------------
win_perf_diffs_df <- fight_data %>%
  select(
    date, fighter_id, fighter, fight_id, opponent, res,
    sig_strikes_landed, sig_strikes_landed_by_opp,
    kds_scored, kds_scored_by_opp,
    tds_landed, tds_landed_by_opp,
    ctrl_time_s, ctrl_time_s_for_opp
  ) %>%
  mutate(date = as.Date(date)) %>%
  filter(date >= UNIFIED_RULES_ADOPTED, res %in% c("W", "L")) %>%
  mutate(
    fighter_id = factor(fighter_id),
    fighter = as.character(fighter),
    fight_id = factor(fight_id),
    opponent = as.character(opponent),
    res = factor(res, levels = c("L", "W")),
    res_win = if_else(res == "W", 1L, 0L),
    across(c(sig_strikes_landed, sig_strikes_landed_by_opp,
             kds_scored, kds_scored_by_opp,
             tds_landed, tds_landed_by_opp,
             ctrl_time_s, ctrl_time_s_for_opp), as.numeric),
    # In-fight performance differentials (fighter minus opponent).
    sig_strike_diff = sig_strikes_landed - sig_strikes_landed_by_opp,
    kd_diff         = kds_scored - kds_scored_by_opp,
    td_diff         = tds_landed - tds_landed_by_opp,
    ctrl_time_diff  = ctrl_time_s - ctrl_time_s_for_opp
  ) %>%
  mutate(
    # Standardized differentials: coefficients reflect a 1-SD advantage.
    sig_strike_diff_z = as.numeric(scale(sig_strike_diff)),
    kd_diff_z         = as.numeric(scale(kd_diff)),
    td_diff_z         = as.numeric(scale(td_diff)),
    ctrl_time_diff_z  = as.numeric(scale(ctrl_time_diff))
  )

write_rds(win_perf_diffs_df, file.path(OUT_DIR, "win_perf_diffs_df.rds"))
write_csv(win_perf_diffs_df, file.path(OUT_DIR, "win_perf_diffs_df.csv"))

message("Done (striking_accuracy/make_data).")
message("  striking_df       : ", nrow(striking_df), " rows")
message("  win_perf_diffs_df : ", nrow(win_perf_diffs_df), " rows")
