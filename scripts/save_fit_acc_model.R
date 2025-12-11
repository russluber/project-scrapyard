fit_acc_model <- brm(
  formula = formula_acc,
  data = df,
  family = binomial(),
  prior = priors_acc,
  sample_prior = "yes",   # include prior draws in the fit object
  iter = 2000,
  warmup = 1000,
  chains = 4,
  cores = 4,
  backend = getOption("brms.backend"),
  seed = 1738,
  control = list(adapt_delta = 0.95)    # reduces divergences in logistic models
)

saveRDS(
  fit_acc_model,
  here::here("models", "fits", "fit_acc_model.rds")
)