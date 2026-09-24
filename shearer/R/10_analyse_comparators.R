# Comparators against the real contemporary field (each season's actual
# top-10, from ingest_season_top_scorers()) and the all-time list (from
# ingest_all_time_scorers()). A season where Shearer isn't in the top-10 is a
# genuine gap -- carried as NA, never guessed.

#' Per-season Shearer rank/goals vs. that season's leader.
#'
#' @param top_scorers Output of `ingest_season_top_scorers()`.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return See description above.
#' @export
contemporary_summary <- function(top_scorers, cfg) {
  player <- cfg$project$player_name

  if (nrow(top_scorers) == 0) {
    return(top_scorers |> mutate(leader_goals = integer(), leader_name = character(),
                                  shearer_rank = integer(), shearer_goals = integer(),
                                  is_leader = logical(), gap_to_leader = integer()))
  }

  # A season can have more than one player tied on the top scorer's tally
  # (e.g. three players shared 1997-98's 18 goals) -- leader_name lists all of
  # them rather than picking one arbitrarily.
  leaders <- top_scorers |>
    filter(rank == 1) |>
    summarise(leader_goals = max(goals), leader_name = paste(sort(unique(player)), collapse = ", "),
              .by = season)

  shearer_rows <- top_scorers |>
    filter(player == !!player) |>
    select(season, shearer_rank = rank, shearer_goals = goals)

  leaders |>
    left_join(shearer_rows, by = "season") |>
    mutate(
      is_leader = if_else(is.na(shearer_rank), NA, shearer_rank == 1),
      gap_to_leader = leader_goals - shearer_goals
    ) |>
    arrange(season)
}

#' Career-level rollup of the per-season comparator table.
#'
#' @param summary_tbl Output of `contemporary_summary()`.
#' @return See description above.
#' @export
contemporary_overview <- function(summary_tbl) {
  tibble::tibble(
    seasons = nrow(summary_tbl),
    times_in_top10 = sum(!is.na(summary_tbl$shearer_rank)),
    times_led = sum(summary_tbl$is_leader, na.rm = TRUE)
  )
}

#' The league-wide scoring environment of each season, keyed by the calendar
#' year the season began so it can be matched against the all-time list's
#' First/Last appearance years.
#'
#' @param league_scoring_trend Output of `league_scoring_trend()` -- one row per season.
#' @return See description above.
#' @export
era_environment <- function(league_scoring_trend) {
  league_scoring_trend |>
    mutate(start_year = as.integer(str_sub(season, 1, 4))) |>
    select(start_year, avg_goals_per_match)
}

#' Mean scoring environment over each player's career, and that career's index
#' against the whole observed period (index < 1 = played in a lower-scoring
#' league than average).
#'
#' Wikipedia records First/Last as the *calendar year of the appearance*, not
#' a season -- a January debut and an August debut are both stored as that one
#' year. Seasons beginning in \[first_year - 1, last_year - 1\] is the closest
#' unambiguous reading; it can be off by one season at either boundary, which
#' is immaterial against a 10-20 season mean but is declared in
#' docs/limitations.md rather than smoothed over. `seasons_matched` carries the
#' count actually used so a thin span is visible rather than implied.
#'
#' @param all_time_scorers Output of `ingest_all_time_scorers()`.
#' @param league_scoring_trend Output of `league_scoring_trend()` -- one row per season.
#' @return See description above.
#' @export
career_era_index <- function(all_time_scorers, league_scoring_trend) {
  env <- era_environment(league_scoring_trend)
  env_ref <- mean(env$avg_goals_per_match)

  spans <- map2(
    all_time_scorers$first_year, all_time_scorers$last_year,
    function(first, last) {
      in_span <- env$avg_goals_per_match[env$start_year >= first - 1L &
                                           env$start_year <= last - 1L]
      tibble(
        seasons_matched = length(in_span),
        env_career = if (length(in_span) == 0) NA_real_ else mean(in_span)
      )
    }
  ) |> list_rbind()

  all_time_scorers |>
    bind_cols(spans) |>
    mutate(era_index = env_career / env_ref)
}

#' Shearer's own season-by-season rate, indexed to the league's scoring
#' environment *that season* rather than to a career-wide average.
#'
#' career_era_index() averages 12-17 seasons per player, which washes out
#' almost all era variation (every long career lands near the overall mean).
#' Season by season the environment genuinely moves, so this is where the
#' signal is: it separates "scored a lot" from "scored a lot in a season when
#' nobody else was".
#'
#' 1992-93 carries NA -- football-data.co.uk starts at 1993-94 and no other
#' source covers the full league that season (see docs/limitations.md).
#'
#' `goals_per_90` (from `season_summary`, sourced from FBref's real per-match
#' minutes) is the primary adjusted rate as of Phase 5 -- it doesn't reward a
#' substitute appearance or punish an early injury the way goals/appearance
#' does. `goals_per_appearance` is kept alongside, not replaced: the README
#' already cites those numbers and the contrast between the two rates is
#' itself part of the story (Shearer played almost every minute; not every
#' comparator did).
#'
#' 1992-93 carries NA -- football-data.co.uk starts at 1993-94 and no other
#' source covers the full league that season (see docs/limitations.md).
#'
#' @param season_summary Output of `season_summary()` -- one row per season/club, with `minutes`/`goals_per_90`.
#' @param shearer_appearances Output of `ingest_shearer_appearances()` / `validate_appearances()`.
#' @param league_scoring_trend Output of `league_scoring_trend()` -- one row per season.
#' @return See description above.
#' @export
season_era_index <- function(season_summary, shearer_appearances, league_scoring_trend) {
  env_ref <- mean(league_scoring_trend$avg_goals_per_match)

  season_summary |>
    left_join(select(shearer_appearances, season, appearances), by = "season") |>
    left_join(select(league_scoring_trend, season, avg_goals_per_match), by = "season") |>
    mutate(
      goals_per_appearance = goals / appearances,
      season_era_index = avg_goals_per_match / env_ref,
      adjusted_goals_per_appearance = goals_per_appearance / season_era_index,
      adjusted_goals_per_90 = goals_per_90 / season_era_index
    ) |>
    arrange(season)
}

#' All-time 100+ list with configured players flagged, goals/appearance,
#' goals/90 and the era-adjusted rate for both.
#'
#' `goals_per_90` needs real minutes, which only FBref has -- `fbref_player_seasons`
#' is pinned to the players in `config.yaml`'s `sources.fbref.comparator_player_ids`
#' (resolved via `fbref_player_ids()`). A player outside that pinned set gets
#' `NA` for `goals_per_90`/`adjusted_goals_per_90` -- a declared coverage gap,
#' not a guess -- while `goals_per_appearance` (Wikipedia-sourced, covers the
#' whole all-time list) stays populated for everyone.
#'
#' `goals_per_90`'s numerator (Wikipedia's career total) and denominator
#' (FBref's summed minutes) only agree when the cached FBref page happens to
#' cover the player's whole career. When it doesn't (an archived snapshot that
#' predates a player's later seasons), the sum of FBref's own per-season goals
#' falls short of the Wikipedia total, and dividing the full total by partial
#' minutes silently inflates the rate. `spans_agree` catches that case and
#' forces `NA` instead -- a second, distinct reason to be missing goals_per_90,
#' alongside "not in the pinned comparator set" above.
#'
#' Raw goals and raw rates are always kept alongside the adjusted figures --
#' the adjustment is offered as a second reading, never as a replacement for
#' what the sources actually recorded.
#'
#' @param all_time_scorers Output of `ingest_all_time_scorers()`.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param league_scoring_trend Output of `league_scoring_trend()` -- one row per season.
#' @param fbref_player_seasons Output of `ingest_fbref_player_seasons()`.
#' @return See description above.
#' @export
historical_table <- function(all_time_scorers, cfg, league_scoring_trend, fbref_player_seasons) {
  highlight_players <- cfg$comparators$historical$highlight

  fbref_by_player <- fbref_player_seasons |>
    summarise(
      minutes = sum(minutes),
      fbref_goals = sum(goals),
      assists = sum(assists),
      .by = player
    )

  all_time_scorers |>
    career_era_index(league_scoring_trend) |>
    mutate(
      highlight = player %in% highlight_players,
      goals_per_appearance = goals / appearances,
      adjusted_goals_per_appearance = goals_per_appearance / era_index
    ) |>
    left_join(fbref_by_player, by = "player") |>
    mutate(
      spans_agree = !is.na(fbref_goals) & fbref_goals == goals,
      minutes = if_else(spans_agree, minutes, NA_integer_),
      goals_per_90 = goals / (minutes / 90),
      adjusted_goals_per_90 = goals_per_90 / era_index,
      minutes_per_goal_or_assist = if_else(spans_agree, minutes / (fbref_goals + assists), NA_real_)
    ) |>
    select(-spans_agree, -fbref_goals) |>
    arrange(desc(goals))
}

#' Players in the pinned FBref comparator set whose cached page doesn't cover
#' their whole Wikipedia-recorded career -- named so the gap in `goals_per_90`
#' is inspectable rather than a silent NA.
#'
#' @param all_time_scorers Output of `ingest_all_time_scorers()`.
#' @param fbref_player_seasons Output of `ingest_fbref_player_seasons()`.
#' @return A tibble with one row per span mismatch: player, Wikipedia goals, FBref goals.
#' @export
historical_table_span_mismatches <- function(all_time_scorers, fbref_player_seasons) {
  fbref_by_player <- fbref_player_seasons |>
    summarise(fbref_goals = sum(goals), .by = player)

  all_time_scorers |>
    select(player, wikipedia_goals = goals) |>
    inner_join(fbref_by_player, by = "player") |>
    filter(wikipedia_goals != fbref_goals)
}
