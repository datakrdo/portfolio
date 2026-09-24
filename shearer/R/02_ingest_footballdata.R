# football-data.co.uk match results, 1993-94 through 2005-06.
# 1992-93 is NOT here (confirmed HTTP 404) -- see 03_ingest_wikipedia.R.

#' Download every season's E0.csv and return the raw rows, tagged by season.
#' Does not filter to Shearer's clubs yet -- that happens in build_matches().
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble.
#' @export
ingest_footballdata_results <- function(cfg) {
  src <- cfg$sources$football_data_co_uk

  raw <- lapply(src$season_codes, function(code) {
    url <- glue("{src$base_url}/{code}/{src$league_code}.csv")
    dest <- glue("{cfg$paths$raw}/football_data_{code}.csv")
    cache_fetch(url, dest, "football_data_co_uk", cfg, notes = code)

    # col_select rather than a later select(): pre-2000 files carry ~20 unnamed
    # trailing columns and modern ones carry ~100 betting-odds columns, none of
    # which are used. Reading only the five needed columns is faster and keeps
    # readr from emitting a "New names: ...8 -> ..." repair warning per season.
    read_csv(dest, col_select = c(Date, HomeTeam, AwayTeam, FTHG, FTAG),
             show_col_types = FALSE, progress = FALSE,
             name_repair = "unique_quiet") |>
      filter(!is.na(HomeTeam)) |>  # older seasons' files are padded with trailing blank CSV lines
      mutate(season_code = code)
  })

  bind_rows(raw)
}
