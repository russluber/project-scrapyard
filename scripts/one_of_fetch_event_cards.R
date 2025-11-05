suppressPackageStartupMessages({ library(tidyverse); library(here); library(httr2); library(readr) })

MANIFEST  <- here("data", "events_manifest.csv")
miss <- read_csv(MANIFEST, show_col_types = FALSE) %>%
  filter(!file.exists(path))

message("Missing event files: ", nrow(miss))

for (i in seq_len(nrow(miss))) {
  u <- miss$event_url[i]; p <- miss$path[i]
  Sys.sleep(runif(1, 0.5, 1.5))
  req <- request(u) |>
    req_user_agent("UFC stats research scraper (event cards, catch-up)") |>
    req_timeout(30) |>
    req_retry(
      max_tries = 6,
      is_transient = \(r) resp_status(r) %in% c(429,500,502,503,504),
      backoff = ~ 1 * 2^tries
    )
  ok <- FALSE; status <- NA_integer_; ctype <- NA_character_
  try({
    resp <- req_perform(req)
    status <- resp_status(resp); ctype <- resp_content_type(resp)
    if (status == 200 && grepl("text/html", ctype, fixed = TRUE)) {
      dir.create(dirname(p), recursive = TRUE, showWarnings = FALSE)
      writeBin(resp_body_raw(resp), p)
      ok <- TRUE
    }
  }, silent = TRUE)
  message(sprintf("[%d/%d] %s  status=%s  saved=%s",
                  i, nrow(miss), basename(p), status, if (ok) "yes" else "no"))
}
