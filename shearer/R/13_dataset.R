# S7 wrapper around data/processed/*.parquet -- the app's own read boundary.
# The pointblank gate in R/07_validate.R already enforces these invariants
# once, upstream, when the pipeline runs; this class re-checks the cheap ones
# (row counts, key columns, the career goal total) at the point the app reads
# the parquet files back, so a stale or partial data/processed/ fails loudly
# at launch instead of producing a half-rendered dashboard.

#' S7 class validating the app's processed dataset at read time.
#'
#' @param matches The `matches` tibble (one row per fixture).
#' @param goal_events The `shearer_goal_events` tibble.
#' @param season_summary Output of `season_summary()` -- one row per season.
#' @param goal_type_breakdown Output of `goal_type_breakdown()`.
#' @param contemporary_summary Output of `contemporary_summary()`.
#' @param contemporary_overview Output of `contemporary_overview()`.
#' @param historical_table Output of `historical_table()`.
#' @param league_scoring_trend Output of `league_scoring_trend()`.
#' @param season_era_index Output of `season_era_index()` -- Shearer's
#'   per-season era-adjusted rate.
#' @param context_breakdown Output of `context_breakdown()`.
#' @param comparator_context_breakdown Output of `comparator_context_breakdown_tbl`
#'   (Phase 7) -- `context_breakdown()`-shaped rows for all 10 comparators, tagged
#'   by `player`.
#' @param .expected_goals Career goal total from `config.yaml`, checked
#'   against `nrow(goal_events)` by the validator.
#' @return An S7 `shearer_data` object.
#' @usage shearer_data(matches, goal_events, season_summary,
#'   goal_type_breakdown, contemporary_summary, contemporary_overview,
#'   historical_table, league_scoring_trend, season_era_index,
#'   context_breakdown, comparator_context_breakdown,
#'   .expected_goals)
#' @export
shearer_data <- new_class("shearer_data",
  properties = list(
    matches = class_data.frame,
    goal_events = class_data.frame,
    season_summary = class_data.frame,
    goal_type_breakdown = class_data.frame,
    contemporary_summary = class_data.frame,
    contemporary_overview = class_data.frame,
    historical_table = class_data.frame,
    league_scoring_trend = class_data.frame,
    season_era_index = class_data.frame,
    context_breakdown = class_data.frame,
    comparator_context_breakdown = class_data.frame,
    # Not part of the public interface, but S7 validators need to see the
    # expected total to check goal_events against it -- carried as a property
    # (set once, at construction) rather than a free variable in a closure.
    .expected_goals = class_numeric
  ),
  validator = function(self) {
    tables <- list(
      matches = self@matches, goal_events = self@goal_events,
      season_summary = self@season_summary,
      goal_type_breakdown = self@goal_type_breakdown,
      contemporary_summary = self@contemporary_summary,
      contemporary_overview = self@contemporary_overview,
      historical_table = self@historical_table,
      league_scoring_trend = self@league_scoring_trend,
      season_era_index = self@season_era_index,
      context_breakdown = self@context_breakdown,
      comparator_context_breakdown = self@comparator_context_breakdown
    )
    empty <- names(tables)[vapply(tables, nrow, integer(1)) == 0]
    if (length(empty) > 0) {
      return(glue::glue("empty table(s): {paste(empty, collapse = ', ')} -- re-run tar_make()"))
    }

    required_cols <- list(
      matches = c("match_id", "season", "home_team", "away_team"),
      goal_events = c("match_id", "season", "for_club", "opponent", "minute"),
      season_era_index = c("season", "adjusted_goals_per_appearance", "season_era_index"),
      context_breakdown = c("context", "n", "share"),
      comparator_context_breakdown = c("player", "context", "n", "share", "n_total")
    )
    for (nm in names(required_cols)) {
      missing_cols <- setdiff(required_cols[[nm]], names(tables[[nm]]))
      if (length(missing_cols) > 0) {
        return(glue::glue("{nm} is missing column(s): {paste(missing_cols, collapse = ', ')}"))
      }
    }

    n_goals <- nrow(self@goal_events)
    if (n_goals != self@.expected_goals) {
      return(glue::glue(
        "goal_events has {n_goals} rows, expected {self@.expected_goals} -- ",
        "data/processed/ looks stale, re-run tar_make()"
      ))
    }
    NULL
  }
)

#' Read every processed table into one validated shearer_data object.
#'
#' @param proc_dir Path to the `data/processed/` directory.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return See description above.
#' @export
load_shearer_data <- function(proc_dir, cfg) {
  read_tbl <- function(name) arrow::read_parquet(file.path(proc_dir, glue::glue("{name}.parquet")))
  shearer_data(
    matches = read_tbl("matches"),
    goal_events = read_tbl("shearer_goal_events"),
    season_summary = read_tbl("season_summary"),
    goal_type_breakdown = read_tbl("goal_type_breakdown"),
    contemporary_summary = read_tbl("contemporary_summary"),
    contemporary_overview = read_tbl("contemporary_overview"),
    historical_table = read_tbl("historical_table"),
    league_scoring_trend = read_tbl("league_scoring_trend"),
    season_era_index = read_tbl("season_era_index"),
    context_breakdown = read_tbl("context_breakdown"),
    comparator_context_breakdown = read_tbl("comparator_context_breakdown"),
    .expected_goals = cfg$validation$expected_total_goals
  )
}

method(print, shearer_data) <- function(x, ...) {
  cat(glue::glue(
    "<shearer_data> {nrow(x@goal_events)} goals across ",
    "{length(unique(x@matches$season))} seasons ",
    "({min(x@matches$season)}-{max(x@matches$season)})"
  ), "\n")
  invisible(x)
}
