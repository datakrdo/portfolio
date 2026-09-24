#' @keywords internal
#' @import dplyr
#' @import ggplot2
#' @import S7
#' @import stringr
#' @import purrr
#' @importFrom arrow read_parquet write_parquet
#' @importFrom fs dir_create file_exists path_dir file_size
#' @importFrom ggsoccer annotate_pitch theme_pitch pitch_statsbomb
#' @importFrom glue glue
#' @importFrom gt gt tab_header tab_source_note gtsave
#' @importFrom httr2 request req_user_agent req_retry req_perform resp_status resp_body_raw
#' @importFrom jsonlite fromJSON
#' @importFrom knitr kable
#' @importFrom pointblank create_agent rows_complete col_vals_equal col_vals_in_set col_vals_not_null interrogate get_agent_report export_report vars
#' @importFrom readr read_csv write_csv
#' @importFrom rlang enquo quo_is_null :=
#' @importFrom rvest read_html html_elements html_table html_text2 html_attr html_element
#' @importFrom scales percent label_percent
#' @importFrom tibble tibble as_tibble
#' @importFrom tidyr replace_na pivot_longer pivot_wider
#' @importFrom xml2 xml_text
#' @importFrom yaml read_yaml
"_PACKAGE"
