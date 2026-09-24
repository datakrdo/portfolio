# One function per app chart, each returning a glue string read from a
# football angle -- form, rank, "what it meant on the pitch" -- rather than
# the statistical reading the helpText()s already give. Every number is
# computed from the same tables the chart draws, never typed by hand, so
# these are tested like any other derived-only summary (R/09_analyse.R).

#' Blackburn vs. Newcastle scoring rate, read as a career arc rather than a
#' list of season totals.
#'
#' @param season_summary Output of `season_summary()`.
#' @return A glue string.
#' @export
trajectory_insight <- function(season_summary) {
  by_club <- season_summary |>
    summarise(avg_goals = mean(goals), n_seasons = dplyr::n(), .by = for_club) |>
    arrange(match(for_club, unique(season_summary$for_club)))
  first_club <- by_club[1, ]
  later_clubs <- by_club[-1, ]
  glue(
    "At {first_club$for_club} he averaged {round(first_club$avg_goals, 1)} goals a season across ",
    "{first_club$n_seasons} seasons -- a strike rate he never sustained for that long again: across his ",
    "{sum(later_clubs$n_seasons)} seasons at {paste(unique(later_clubs$for_club), collapse = ' and ')} he ",
    "averaged {round(weighted.mean(later_clubs$avg_goals, later_clubs$n_seasons), 1)}, pulled down by the two ",
    "injury-hit seasons footnoted below."
  )
}

#' Seasons Shearer topped or ran up the league's scoring charts.
#'
#' @param season_summary Output of `season_summary()`.
#' @param contemporary_summary Output of `contemporary_summary()`.
#' @return A glue string.
#' @export
contemporary_insight <- function(season_summary, contemporary_summary) {
  led <- season_summary |>
    left_join(contemporary_summary, by = "season") |>
    filter(!is.na(is_leader))
  n_led <- sum(led$is_leader)
  n_runner_up <- sum(!led$is_leader & led$shearer_rank == 2, na.rm = TRUE)
  glue(
    "Consistency, not just peaks: Shearer led the league in {n_led} seasons and was runner-up in ",
    "{n_runner_up} more. The seasons the line collapses (1997-98, 2000-01) are the injury years ",
    "footnoted on Overview, not a decline."
  )
}

#' How much the era adjustment moves Shearer relative to the rest of the top
#' 10 -- read as "who was he really competing with", not as a statistic.
#'
#' @param top10_bands Output of `historical_table()`, filtered to the top 10
#'   by goals with `adjusted_goals_per_90` non-missing (`app.R`'s `top10_bands`).
#' @param player The player to report on, e.g. `cfg$project$player_name`.
#' @return A glue string.
#' @export
dumbbell_insight <- function(top10_bands, player) {
  ranked <- top10_bands |>
    mutate(
      raw_rank = rank(-goals_per_90, ties.method = "min"),
      adj_rank = rank(-adjusted_goals_per_90, ties.method = "min")
    )
  me <- ranked[ranked$player == player, ]
  ahead <- ranked |> filter(adj_rank < me$adj_rank) |> arrange(adj_rank) |> pull(player)
  movement <- me$raw_rank - me$adj_rank
  direction <- if (movement > 0) glue("up {movement} place{if (movement > 1) 's' else ''}")
               else if (movement < 0) glue("down {abs(movement)} place{if (abs(movement) > 1) 's' else ''}")
               else "in the same spot"
  glue(
    "He played in a tighter-scoring league than most of this list: adjusting for era moves {player} ",
    "{direction}, to {scales::ordinal(me$adj_rank)} of {nrow(ranked)}",
    if (length(ahead) > 0) glue(" -- still behind {paste(ahead, collapse = ', ')}.") else "."
  )
}

#' Shearer's own best season and whether he ever matched it again after the
#' 1997-98 injury.
#'
#' @param season_era_index Output of `career_era_index()` joined onto
#'   `season_summary()` (`app.R`'s `season_era_index_tbl`), filtered to
#'   `!is.na(adjusted_goals_per_90)`.
#' @return A glue string.
#' @export
season_line_insight <- function(season_era_index) {
  seasons <- season_era_index |> arrange(season)
  peak <- seasons |> slice_max(adjusted_goals_per_90, n = 1, with_ties = FALSE)
  peak_year <- as.integer(substr(peak$season, 1, 4))
  after_peak <- seasons |> filter(as.integer(substr(season, 1, 4)) > peak_year)
  matched_again <- any(after_peak$adjusted_goals_per_90 >= peak$adjusted_goals_per_90)
  glue(
    "His peak was {peak$season} at {peak$for_club} ({round(peak$adjusted_goals_per_90, 2)} era-adjusted ",
    "goals per 90) -- ",
    if (matched_again) "and he did get back to that level again later in his career."
    else "a level he came close to but never matched again once the injuries started."
  )
}

#' Where Shearer's goals came from -- penalties vs. open play, with the
#' undocumented share named rather than folded into either bucket.
#'
#' @param goal_type_breakdown Output of `goal_type_breakdown()`.
#' @return A glue string.
#' @export
goal_type_insight <- function(goal_type_breakdown) {
  share_of <- function(type) {
    row <- goal_type_breakdown[goal_type_breakdown$goal_type_group == type, ]
    if (nrow(row) == 0) 0 else row$share[[1]]
  }
  glue(
    "Roughly {scales::percent(share_of('open_play'), accuracy = 1)} of his goals came from open play and ",
    "{scales::percent(share_of('penalty'), accuracy = 1)} from the penalty spot -- with ",
    "{scales::percent(share_of('unknown'), accuracy = 1)} left as 'unknown' because the source match report ",
    "didn't record how the goal was scored, not folded into either bucket."
  )
}

#' The moments Shearer's goals mattered most -- decisive vs. cushion/consolation.
#'
#' @param context_breakdown Output of `context_breakdown()`.
#' @return A glue string.
#' @export
context_insight <- function(context_breakdown) {
  share_of <- function(ctx) {
    row <- context_breakdown[context_breakdown$context == ctx, ]
    if (nrow(row) == 0) 0 else row$share[[1]]
  }
  glue(
    "{scales::percent(share_of('go_ahead'), accuracy = 1)} of his goals put his team ahead and ",
    "{scales::percent(share_of('equaliser'), accuracy = 1)} were equalisers -- together, ",
    "{scales::percent(clutch_share(context_breakdown), accuracy = 1)} of his career total changed the ",
    "scoreline in his team's favour, against just {scales::percent(share_of('reduce_deficit'), accuracy = 1)} ",
    "scored while already behind."
  )
}

#' Shearer's clutch-context rank among the 10 career comparators.
#'
#' @param comparator_clutch_shares One row per player with a `clutch_share`
#'   column, sorted descending (`app.R`'s `comparator_clutch_shares`).
#' @param player The player to report on, e.g. `cfg$project$player_name`.
#' @return A glue string.
#' @export
comparator_clutch_insight <- function(comparator_clutch_shares, player) {
  ranked <- comparator_clutch_shares |> arrange(desc(clutch_share))
  rank <- match(player, ranked$player)
  leader <- ranked[1, ]
  glue(
    "{player} ranks {scales::ordinal(rank)} of {nrow(ranked)} comparators for scoring goals that changed the ",
    "scoreline in his team's favour ({scales::percent(ranked$clutch_share[rank], accuracy = 1)}) -- ",
    if (leader$player == player) "the highest share of the ten."
    else glue("behind {leader$player} ({scales::percent(leader$clutch_share, accuracy = 1)}).")
  )
}

#' Head-to-head record against one opponent, read in football terms
#' ("scored in X of N meetings") rather than as a confidence interval.
#'
#' @param rival_matches One row per meeting, with a `shearer_goals` column
#'   (`app.R`'s `rival_matches()`).
#' @param rival Opponent name, for the sentence.
#' @return A glue string.
#' @export
rival_insight <- function(rival_matches, rival) {
  n <- nrow(rival_matches)
  x <- sum(rival_matches$shearer_goals > 0)
  total_goals <- sum(rival_matches$shearer_goals)
  if (n == 0) return(glue("No meetings with {rival} in this season range."))
  rate <- if (x == 0) "never" else glue("once every {round(n / x, 1)} meetings")
  glue(
    "{total_goals} goals in {n} meetings with {rival} -- he scored in {x} of them, {rate}."
  )
}
