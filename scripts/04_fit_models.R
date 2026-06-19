# scripts/04_fit_models.R
#
# Stage 4 of the pipeline: fit and cache the Bayesian striking-accuracy
# model used in the final report (RQ2).
#
# This is the hierarchical binomial logistic model:
#   sig_strikes_landed | trials(sig_strikes_thrown) ~
#       1 + gender + weight_class + (1 | fighter_id) + (1 | fight_id)
#
# It fits two objects and saves both to models/fits/:
#   - fit_prior_acc.rds : prior-only fit (for prior predictive checks)
#   - fit_acc_model.rds : the full posterior fit
#
# NOTE: fitting compiles Stan and runs MCMC; expect this to take several
# minutes. The formula and priors here are the single source of truth and
# must match what the final report documents.
#
# Inputs : data/model/striking_df.rds  (from 03_make_model_data.R)
# Outputs: models/fits/fit_prior_acc.rds, models/fits/fit_acc_model.rds

suppressPackageStartupMessages({
  library(brms)
  library(readr)
  library(here)
})

FITS_DIR <- here("models", "fits")
dir.create(FITS_DIR, recursive = TRUE, showWarnings = FALSE)

df <- read_rds(here("data", "model", "striking_df.rds"))

# -------------------------------------------------------------------
# Model specification (shared by the prior-only and full fits)
# -------------------------------------------------------------------
formula_acc <- bf(
  sig_strikes_landed | trials(sig_strikes_thrown) ~
    1 + gender + weight_class +
    (1 | fighter_id) +
    (1 | fight_id)
)

# Baseline mean strike-landing probability on the logit scale (~0.46).
mu0 <- qlogis(0.46)

priors_acc <- c(
  set_prior(paste0("normal(", mu0, ", 1)"), class = "Intercept"),
  set_prior("normal(0, 0.3)", class = "b"),                            # gender + weight class
  set_prior("normal(0, 0.4)", class = "sd", group = "fighter_id"),     # fighter variation
  set_prior("normal(0, 0.4)", class = "sd", group = "fight_id")        # fight variation
)

# -------------------------------------------------------------------
# 1) Prior-only fit (prior predictive checks)
# -------------------------------------------------------------------
message("Fitting prior-only model (fit_prior_acc)...")
fit_prior_acc <- brm(
  formula = formula_acc,
  data = df,
  family = binomial(),
  prior = priors_acc,
  sample_prior = "only",
  iter = 1000,
  chains = 2,
  cores = 2,
  backend = getOption("brms.backend"),
  seed = 1738
)
saveRDS(fit_prior_acc, file.path(FITS_DIR, "fit_prior_acc.rds"))

# -------------------------------------------------------------------
# 2) Full posterior fit
# -------------------------------------------------------------------
message("Fitting full model (fit_acc_model)... this can take several minutes.")
fit_acc_model <- brm(
  formula = formula_acc,
  data = df,
  family = binomial(),
  prior = priors_acc,
  sample_prior = "yes",   # keep prior draws in the fit object
  iter = 2000,
  warmup = 1000,
  chains = 4,
  cores = 4,
  backend = getOption("brms.backend"),
  seed = 1738,
  control = list(adapt_delta = 0.95)   # reduce divergences in logistic models
)
saveRDS(fit_acc_model, file.path(FITS_DIR, "fit_acc_model.rds"))

message("Done (04_fit_models).")
message("  Prior fit : ", file.path(FITS_DIR, "fit_prior_acc.rds"))
message("  Model fit : ", file.path(FITS_DIR, "fit_acc_model.rds"))
