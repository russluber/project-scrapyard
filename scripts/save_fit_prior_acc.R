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

saveRDS(
  fit_prior_acc,
  here::here("models", "fits", "fit_prior_acc.rds")
)
