# Derived-only summaries over shearer_goal_events. Every number here traces
# back to a data/processed table, never typed by hand.

#' Goals per season and club, plus minutes played and goals/90 -- FBref is
#' the only source with real per-match minutes, so `fbref_player_seasons`
#' (Shearer's own rows) supplies them here rather than deriving minutes from
#' appearances, which would assume every match was a full 90.
#'
#' @param goal_events The `shearer_goal_events` tibble (goal_events_checked in `_targets.R`).
#' @param fbref_player_seasons Output of `ingest_fbref_player_seasons()`.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return See description above.
#' @export
season_summary <- function(goal_events, fbref_player_seasons, cfg) {
  minutes_by_season <- fbref_player_seasons |>
    filter(player == cfg$project$player_name) |>
    transmute(season = fbref_season_to_label(season_raw), minutes)

  goal_events |>
    summarise(goals = n(), .by = c(season, for_club)) |>
    left_join(minutes_by_season, by = "season") |>
    mutate(goals_per_90 = goals / (minutes / 90)) |>
    arrange(season)
}

#' Goals per opponent, most-scored-against first.
#'
#' @param goal_events The `shearer_goal_events` tibble (goal_events_checked in `_targets.R`).
#' @return See description above.
#' @export
goals_by_opponent <- function(goal_events) {
  goal_events |>
    summarise(goals = n(), .by = opponent) |>
    arrange(desc(goals))
}

#' Goal-type distribution with each group's share of the total -- "unknown"
#' (Transfermarkt's "Not reported") is reported alongside the rest, never
#' redistributed into the classified categories.
#'
#' @param goal_events The `shearer_goal_events` tibble (goal_events_checked in `_targets.R`).
#' @return See description above.
#' @export
goal_type_breakdown <- function(goal_events) {
  n_total <- nrow(goal_events)
  goal_events |>
    summarise(n = n(), .by = goal_type_group) |>
    mutate(share = n / n_total, n_total = n_total) |>
    arrange(desc(n))
}

#' Goals by the match state they were scored in (see classify_goal_context()).
#' Same shape as goal_type_breakdown(), so both feed the same figure and app
#' panel patterns.
#'
#' @param goal_events The `shearer_goal_events` tibble (goal_events_checked in `_targets.R`).
#' @return See description above.
#' @export
context_breakdown <- function(goal_events) {
  n_total <- nrow(goal_events)
  goal_events |>
    summarise(n = n(), .by = context) |>
    mutate(share = n / n_total, n_total = n_total) |>
    arrange(desc(n))
}

#' Share of career goals that were an equaliser or a go-ahead goal -- "scored
#' when the team needed it most" (see classify_goal_context()). The app and
#' any report read this one number rather than re-deriving it from the raw
#' `context_breakdown()` rows.
#'
#' @param context_breakdown Output of `context_breakdown()`.
#' @return A single numeric share (0-1).
#' @export
clutch_share <- function(context_breakdown) {
  sum(context_breakdown$n[context_breakdown$context %in% c("equaliser", "go_ahead")]) /
    context_breakdown$n_total[[1]]
}

#' Full clutch-context breakdown for one historical comparator (Phase 7):
#' fetches that player's own PL match logs and match reports (independently
#' of Shearer's -- `cache_fetch_wayback()` skips files that already exist, so
#' this is resumable), scores and classifies them the same way Shearer's own
#' branch does, and reports the resulting context share. Never touches
#' Shearer's own targets/cache files.
#'
#' @param player Player name, matching a key in `fbref_player_ids(cfg)`.
#' @param player_id FBref id for `player`.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param fbref_player_seasons Output of `ingest_fbref_player_seasons()` --
#'   supplies `player`'s own Premier League seasons, so they aren't hardcoded here.
#' @return A `context_breakdown()`-shaped tibble tagged with a `player` column,
#'   plus a `coverage_gaps` attribute (character vector of match reports the
#'   Archive had no capture of).
#' @export
comparator_context_breakdown_for <- function(player, player_id, cfg, fbref_player_seasons) {
  seasons <- fbref_player_seasons |>
    filter(.data$player == .env$player) |>
    pull(season_raw)

  logs_raw <- ingest_fbref_match_logs(cfg, player_id = player_id, player_name = player, seasons = seasons)
  logs <- logs_raw |> filter(str_detect(comp, "Premier"))
  events <- ingest_fbref_goal_events(cfg, logs, player_name = player)
  scored <- build_comparator_goal_events(cfg, events)
  classified <- classify_goal_context(scored, cfg)
  breakdown <- context_breakdown(classified) |> mutate(player = player, .before = 1)
  attr(breakdown, "coverage_gaps") <- c(attr(logs_raw, "coverage_gaps"), attr(events, "coverage_gaps"))
  breakdown
}

#' Penalty goals as a share of the career total.
#'
#' @param goal_events The `shearer_goal_events` tibble (goal_events_checked in `_targets.R`).
#' @return See description above.
#' @export
penalty_summary <- function(goal_events) {
  n_total <- nrow(goal_events)
  n_penalty <- sum(goal_events$goal_type_group == "penalty")
  tibble(n_penalty = n_penalty, n_total = n_total, share = n_penalty / n_total)
}

#' League-wide average goals per match, per season -- context for reading
#' career goal totals across eras (a low-scoring 1990s vs. a higher-scoring
#' modern league is not the same "30 goals"). football_data_rows is the full,
#' unfiltered season CSV (every match, every club) already fetched by
#' ingest_footballdata_results() -- not just Shearer's matches.
#'
#' 1992-93 is excluded: football-data.co.uk (the only full-league source
#' used here) starts at 1993-94; Wikipedia only covers Shearer's own club
#' that season, not the whole league, so no full-league figure is derivable.
#'
#' @param football_data_rows The full, unfiltered football-data.co.uk season rows (every club, every match).
#' @return See description above.
#' @export
league_scoring_trend <- function(football_data_rows) {
  football_data_rows |>
    mutate(season = football_data_season_label(season_code), total_goals = FTHG + FTAG) |>
    summarise(avg_goals_per_match = mean(total_goals), n_matches = n(), .by = season) |>
    arrange(season)
}
