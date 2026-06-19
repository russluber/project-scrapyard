# scripts/run_pipeline.R
#
# Runner for the core UFC DATA pipeline. Run this whenever new UFC events
# have occurred to refresh the general-purpose datasets from scratch.
#
# Stages:
#   00_scrape_fights.R    scrape events + fights   -> data/raw/fight_data_raw_enriched.csv
#   01_scrape_fighters.R  scrape fighter metadata  -> data/raw/fighters_data_raw.csv
#   02_clean_fight_data.R clean + reshape fights   -> data/clean/fight_data.csv
#
# This pipeline stops at the cleaned, analysis-ready data. The
# research-question-specific steps live in their own subfolder and are
# intentionally NOT run here. For the striking-accuracy report, run:
#   source("scripts/striking_accuracy/make_data.R")  # build model datasets
#   source("scripts/striking_accuracy/fit_model.R")  # fit the Bayesian model
#
# Each stage is run in its own environment so the scripts' top-level
# objects don't leak between stages.
#
# Usage:
#   # Full refresh (scrape + clean):
#   Rscript scripts/run_pipeline.R
#   #   ...or in an R session:  source("scripts/run_pipeline.R")
#
#   # Skip scraping and only rebuild the clean data from existing raw CSVs:
#   Rscript scripts/run_pipeline.R --no-scrape
#
# The flag can also be set before sourcing:
#   RUN_SCRAPE <- FALSE; source("scripts/run_pipeline.R")

suppressPackageStartupMessages({
  library(here)
})

# -------------------------------------------------------------------
# Resolve options from command-line args and/or pre-set variables.
# -------------------------------------------------------------------
.args <- commandArgs(trailingOnly = TRUE)

if (!exists("RUN_SCRAPE")) RUN_SCRAPE <- !("--no-scrape" %in% .args)

# Run one pipeline stage in a fresh environment, timing it.
run_stage <- function(file) {
  path <- here::here("scripts", file)
  message("\n========================================")
  message("RUN: ", file)
  message("========================================")
  t0 <- Sys.time()
  env <- new.env(parent = globalenv())
  sys.source(path, envir = env)
  message(sprintf("OK : %s (%.1fs)", file, as.numeric(Sys.time() - t0, units = "secs")))
}

# -------------------------------------------------------------------
# Run stages
# -------------------------------------------------------------------
message("Data pipeline options: scrape=", RUN_SCRAPE)

if (RUN_SCRAPE) {
  run_stage("00_scrape_fights.R")
  run_stage("01_scrape_fighters.R")
} else {
  message("\n[skip] scraping stages (--no-scrape); using existing data/raw CSVs.")
}

run_stage("02_clean_fight_data.R")

message("\nData pipeline complete. Cleaned data is in data/clean/.")
message("For the report's models, run scripts/striking_accuracy/make_data.R then fit_model.R.")
