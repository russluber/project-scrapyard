# parse_fight_pages.R — parse cached fight pages (no network)

suppressPackageStartupMessages({
  library(rvest)
  library(tidyverse)
  library(here)
  library(readr)
  library(stringr)
  library(purrr)
})

# ------------------------- Config -------------------------
CACHE_DIR <- here("cache", "fights")
OUT_CSV   <- here("data",  "fight_data_raw.csv")
ERR_CSV   <- here("data",  "parse_errors_fights.csv")

dir.create(dirname(OUT_CSV), recursive = TRUE, showWarnings = FALSE)

# ------------------------- Helpers ------------------------
`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b
squish_na <- function(x) { x <- str_squish(x); na_if(x, "") }

fight_id_from_path <- function(path) {
  sub("\\.html$", "", basename(path), ignore.case = TRUE)
}

# -------------------- Parse a single fight -----------------
parse_one_fight <- function(path) {
  fight_id <- fight_id_from_path(path)
  doc <- read_html(path)
  
  # -------- summary table (first table) --------
  tbl_nodes <- html_nodes(doc, "table")
  if (length(tbl_nodes) < 1) stop("No <table> nodes found")
  
  tbl_list <- html_table(tbl_nodes[1], trim = TRUE, fill = TRUE)
  if (length(tbl_list) == 0 || nrow(tbl_list[[1]]) == 0) stop("First table empty")
  
  # fighter anchors for IDs (scope to col 1; fallback selectors if needed)
  fighter_anchors <- tbl_nodes[1] %>%
    html_element("tbody") %>%
    html_elements("tr td:nth-child(1) a.b-link[href*='fighter-details']")
  
  if (length(fighter_anchors) == 0) {
    fighter_anchors <- tbl_nodes[1] %>%
      html_elements("tr td:nth-child(1) a.b-link[href*='fighter-details']")
  }
  if (length(fighter_anchors) == 0) {
    fighter_anchors <- doc %>%
      html_elements(".b-fight-details__persons .b-fight-details__person-name a.b-fight-details__person-link")
  }
  
  fighter_hrefs <- fighter_anchors %>%
    html_attr("href") %>%
    stringr::str_squish() %>%
    head(2)
  
  fighter_ids <- map_chr(
    fighter_hrefs,
    function(x) {
      if (is.na(x) || !nzchar(x)) {
        NA_character_
      } else {
        sub(".*/fighter-details/([^/?#]+).*", "\\1", x)
      }
    }
  )
  fighter_ids <- c(fighter_ids, NA_character_, NA_character_)[1:2]
  fighter_1_id <- fighter_ids[1]
  fighter_2_id <- fighter_ids[2]
  
  # -------- tidy the summary table (mirrors your original) --------
  summary_data <- bind_rows(tbl_list) %>%
    as_tibble() %>%
    rename(
      "Fighter" = 1, "KD" = 2, "Sig_Strike" = 3, "Sig_Strike_Percent" = 4,
      "Total_Strikes" = 5, "TD" = 6, "TD_Percent" = 7, "Sub_Attempts" = 8,
      "Rev" = 9, "Ctrl" = 10
    ) %>%
    pivot_longer(cols = everything(), names_to = "key", values_to = "value") %>%
    separate(value, into = c("fighter_1", "fighter_2"),
             sep = "\\s{2,}", extra = "merge", fill = "right") %>%
    mutate(across(everything(), squish_na)) %>%
    pivot_wider(names_from = key, values_from = c(fighter_1, fighter_2)) %>%
    separate(fighter_1_Sig_Strike,
             into = c("fighter_1_Sig_Strike_Landed", "fighter_1_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_Sig_Strike,
             into = c("fighter_2_Sig_Strike_Landed", "fighter_2_Sig_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_1_Total_Strikes,
             into = c("fighter_1_Strike_Landed", "fighter_1_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_Total_Strikes,
             into = c("fighter_2_Strike_Landed", "fighter_2_Strike_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_1_TD,
             into = c("fighter_1_TD_Landed", "fighter_1_TD_Attempts"),
             sep = " of ", extra = "merge") %>%
    separate(fighter_2_TD,
             into = c("fighter_2_TD_Landed", "fighter_2_TD_Attempts"),
             sep = " of ", extra = "merge") %>%
    mutate(
      across(contains("Percent"), ~ as.numeric(str_remove(.x, "%")) * 0.01),
      across(-matches("(^fighter_1$|^fighter_2$|Fighter|Ctrl$)"),
             ~ suppressWarnings(as.numeric(.x)))
    )
  
  # -------- decisions / metadata --------
  statuses <- doc %>%
    html_elements(".b-fight-details__persons .b-fight-details__person .b-fight-details__person-status") %>%
    html_text2() %>%
    str_squish() %>%
    toupper()
  statuses <- c(statuses, NA_character_, NA_character_)[1:2]
  fighter_1_res <- if (statuses[1] %in% c("W","L","D","NC")) statuses[1] else NA_character_
  fighter_2_res <- if (statuses[2] %in% c("W","L","D","NC")) statuses[2] else NA_character_
  
  # fight details block (Event, Date, Method, Round, Time, etc.)
  details_vals <- doc %>%
    html_nodes(xpath = '//*[contains(concat(" ", @class, " "), " b-fight-details__text ") and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//i') %>%
    html_text()
  fight_details <- tibble(value = details_vals) %>%
    mutate(value = str_squish(value)) %>%
    separate(value, into = c("feature", "value"), sep = ":", extra = "merge") %>%
    mutate(value = str_trim(value)) %>%
    replace_na(list(value = "")) %>%
    filter(value != "") %>%
    pivot_wider(names_from = feature, values_from = value) %>%
    rename_with(~ str_replace_all(.x, "\\s|/", "_") %>% tolower()) %>%
    rename(round_finished = round)
  
  # weight class (title line)
  title_text <- doc %>%
    html_nodes(".b-fight-details__fight-title") %>%
    html_text() %>%
    str_replace_all("\n", "") %>%
    str_trim()
  
  fight_details <- fight_details %>%
    bind_cols(tibble(
      fighter_1_res = fighter_1_res,
      fighter_2_res = fighter_2_res
    )) %>%
    bind_cols(tibble(weight_class = title_text)) %>%
    mutate(
      weight_class = str_squish(weight_class),
      weight_class = str_replace(weight_class, ".*-\\s*", "")
    )
  
  ids <- tibble(
    fight_id     = fight_id,
    fighter_1_id = fighter_1_id,
    fighter_2_id = fighter_2_id,
    source_path  = path
  )
  
  as_tibble(bind_cols(summary_data, ids, fight_details))
}

# --------------------- Main: parse all cached ---------------------
all_html <- dir(CACHE_DIR, pattern = "\\.html$", full.names = TRUE)
if (length(all_html) == 0) {
  stop("No cached HTML files found in ", CACHE_DIR)
}

# Resume support: skip fights already parsed
already <- character()
if (file.exists(OUT_CSV)) {
  old <- read_csv(OUT_CSV, show_col_types = FALSE)
  if ("fight_id" %in% names(old)) {
    already <- unique(old$fight_id)
  }
}

todo <- tibble(
  path = all_html,
  fight_id = fight_id_from_path(all_html)
) %>%
  filter(!(fight_id %in% already))

message("Total cached fights: ", length(all_html))
message("Already parsed    : ", length(already))
message("To parse now      : ", nrow(todo))

if (nrow(todo) == 0L) {
  message("Nothing new to parse. Exiting.")
  quit(save = "no")
}

# Parse (safely)
safe_parse <- safely(parse_one_fight, otherwise = NULL)
res <- map(todo$path, safe_parse)

ok_tbls  <- map(res, "result")
err_list <- map(res, "error")

# Bind successes
parsed_list <- compact(ok_tbls)
if (length(parsed_list) > 0) {
  parsed <- dplyr::bind_rows(parsed_list) %>%
    mutate(round_finished = suppressWarnings(as.integer(round_finished))) %>%
    distinct()
} else {
  parsed <- tibble()
}

# Append or write output
if (file.exists(OUT_CSV) && nrow(parsed) > 0) {
  old <- read_csv(OUT_CSV, show_col_types = FALSE)
  out <- bind_rows(old, parsed) %>% distinct(fight_id, .keep_all = TRUE)
} else {
  out <- parsed
}
write_csv(out, OUT_CSV)

# # Write error log (only rows with errors)
# if (length(err_list) > 0) {
#   err_tbl <- tibble(
#     path = todo$path,
#     fight_id = todo$fight_id,
#     error_msg = map_chr(err_list, ~ if (is.null(.x)) NA_character_ else conditionMessage(.x))
#   ) %>% filter(!is.na(error_msg))
#   if (nrow(err_tbl) > 0) write_csv(err_tbl, ERR_CSV)
# }

message("Parsed new fights : ", nrow(parsed))
message("Output written to: ", OUT_CSV)
if (file.exists(ERR_CSV)) message("Errors (if any)   : ", ERR_CSV)
