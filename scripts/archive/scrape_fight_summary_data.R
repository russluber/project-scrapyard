scrape_fight_summary_data <- function(link){
  link <- read_html(link)

  # Guard: summary table presence/shape
  table_nodes <- html_nodes(link, "table")
  if (length(table_nodes) < 1) stop("No tables found")

  tbl_list <- html_table(table_nodes[1], trim = TRUE, fill = TRUE)
  if (length(tbl_list) == 0) stop("First table empty")

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
    mutate(across(everything(), stringr::str_squish)) %>%
    mutate(across(everything(), ~ na_if(.x, ""))) %>%
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
    mutate(across(contains("Percent"),
                  ~ as.numeric(str_remove(.x, "%")) * 0.01)) %>%
    mutate(across(-matches("(^fighter_1$|^fighter_2$|Fighter|Ctrl$)"),
                  ~ suppressWarnings(as.numeric(.x))))

  # Fight details + outcome parsing (NC, Draw, W, L)
  res_text <- link %>%
    html_node(".b-fight-details__persons") %>%
    html_text() %>%
    stringr::str_squish()

  fighter_1_res <- dplyr::case_when(
    stringr::str_detect(res_text, stringr::regex("\\bNC\\b|No\\s*Contest|Overturned", ignore_case = TRUE)) ~ "NC",
    stringr::str_detect(res_text, stringr::regex("\\bD\\b|Majority\\s*Draw|Split\\s*Draw|Unanimous\\s*Draw|\\bDraw\\b", ignore_case = TRUE)) ~ "D",
    stringr::str_detect(res_text, "\\bW\\b") ~ "W",
    stringr::str_detect(res_text, "\\bL\\b") ~ "L",
    TRUE ~ NA_character_
  )

  fighter_2_res <- dplyr::case_when(
    fighter_1_res %in% c("NC", "D") ~ fighter_1_res,
    fighter_1_res == "W" ~ "L",
    fighter_1_res == "L" ~ "W",
    TRUE ~ NA_character_
  )

  fight_details_text <- link %>%
    html_nodes(xpath = '//*[contains(concat(" ", @class, " "), " b-fight-details__text ") and (((count(preceding-sibling::*) + 1) = 1) and parent::*)]//i') %>%
    html_text()

  fight_details <- tibble(value = fight_details_text) %>%
    mutate(value = str_squish(value)) %>%
    separate(value, into = c("feature", "value"), sep = ":", extra = "merge") %>%
    mutate(value = str_trim(value)) %>%
    replace_na(list(value = "")) %>%
    filter(value != "") %>%
    pivot_wider(names_from = feature, values_from = value) %>%
    rename_with(~ str_replace_all(.x, "\\s|/", "_") %>% tolower()) %>%
    rename(round_finished = round)

  title_text <- link %>%
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

  out <- dplyr::bind_cols(summary_data, fight_details)

  if (exists("pb", inherits = FALSE)) pb$tick()
  Sys.sleep(SLEEP_DELAY)

  tibble::as_tibble(out)
}