# scripts/run_pipeline.R
#
# End-to-end runner for the UFC data pipeline. Run this whenever new UFC
# events have occurred to refresh the datasets (and, optionally, refit the
# model) from scratch.
#
# Stages:
#   00_scrape_fights.R    scrape events + fights        -> data/raw/fight_data_raw_enriched.csv
#   01_scrape_fighters.R  scrape fighter metadata       -> data/raw/fighters_data_raw.csv
#   02_clean_fight_data.R clean + reshape fights        -> data/clean/fight_data.csv
#   03_make_model_data.R  build model datasets          -> data/model/*.rds
#   04_fit_models.R       fit Bayesian model (SLOW)     -> models/fits/*.rds
#
# Each stage is run in its own environment so the scripts' top-level
# objects don't leak between stages.
#
# Usage:
#   # Data refresh only (default — does NOT refit the model):
#   Rscript scripts/run_pipeline.R
#   #   ...or in an R session:  source("scripts/run_pipeline.R")
#
#   # Include the slow model-fitting stage:
#   Rscript scripts/run_pipeline.R --fit
#
#   # Skip scraping and only rebuild downstream data from existing raw CSVs:
#   Rscript scripts/run_pipeline.R --no-scrape
#
# Flags can be combined. They can also be set before sourcing:
#   RUN_FIT <- TRUE; RUN_SCRAPE <- FALSE; source("scripts/run_pipeline.R")

suppressPackageStartupMessages({
  library(here)
})

# -------------------------------------------------------------------
# Resolve options from command-line args and/or pre-set variables.
# -------------------------------------------------------------------
.args <- commandArgs(trailingOnly = TRUE)

if (!exists("RUN_FIT"))    RUN_FIT    <- "--fit" %in% .args
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
message("Pipeline options: scrape=", RUN_SCRAPE, "  fit=", RUN_FIT)

if (RUN_SCRAPE) {
  run_stage("00_scrape_fights.R")
  run_stage("01_scrape_fighters.R")
} else {
  message("\n[skip] scraping stages (--no-scrape); using existing data/raw CSVs.")
}

run_stage("02_clean_fight_data.R")
run_stage("03_make_model_data.R")

if (RUN_FIT) {
  run_stage("04_fit_models.R")
} else {
  message("\n[skip] model fitting (04_fit_models.R). Pass --fit (or set RUN_FIT <- TRUE) to include it.")
}

message("\nPipeline complete.")
