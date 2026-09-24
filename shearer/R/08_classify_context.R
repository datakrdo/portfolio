# Goal-context taxonomy, derived purely from score_before/score_after and
# minute -- no external "clutch" rating is fetched or invented.
#
# Reconstructs the classification rules originally written in the retired
# `08_clutch_goals_analysis.R` (the one script in the prior attempt that
# wasn't fabricated). That file was accidentally deleted while reorganizing
# this project; its weighting scheme and validation guard are reproduced
# here from the audit transcript, not from the original bytes.

#' Classify each goal's context from the score immediately before it. A goal
#' always moves the scorer's margin by exactly +1, so "ahead" is only
#' reachable from level (0 -> 1), never directly from behind (-1 -> 0 is a
#' tie, not a lead):
#'   equaliser       team was 1 down, goal ties it (margin_before == -1)
#'   go_ahead        team was level, goal puts them ahead (margin_before == 0)
#'   reduce_deficit  team still behind after the goal (margin_before < -1)
#'   extend_lead     team already ahead before the goal (margin_before > 0)
#' Also flags late_goal (>= late_goal_minute) and a close-match state
#' (margin before the goal <= close_margin_abs).
#'
#' @param goal_events The `shearer_goal_events` tibble (goal_events_checked in `_targets.R`),
#'   or the lighter comparator tibble from `build_comparator_goal_events()`
#'   (Phase 7), which has no `home_goals`/`away_goals` (no fixture list for
#'   comparators) -- `final_margin_le_1` is `NA` in that case, a declared gap
#'   rather than a guess.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return See description above.
#' @export
classify_goal_context <- function(goal_events, cfg) {
  th <- cfg$context_classification

  goal_events |>
    mutate(
      margin_before = score_before_for - score_before_against,
      context = case_when(
        margin_before == -1 ~ "equaliser",
        margin_before == 0 ~ "go_ahead",
        margin_before < 0 ~ "reduce_deficit",
        margin_before > 0 ~ "extend_lead",
        TRUE ~ NA_character_
      ),
      is_late_goal = minute >= th$late_goal_minute,
      state_close_before_goal = abs(margin_before) <= th$close_margin_abs,
      final_margin_le_1 = if (all(c("home_goals", "away_goals") %in% names(goal_events)))
        abs(home_goals - away_goals) <= th$close_margin_abs
      else NA
    )
}

# A weighted "clutch score" (equaliser 3.0, go-ahead 2.0, ... plus a lateness
# bonus) lived here, carried over from the retired `08_clutch_goals_analysis.R`.
# It was removed rather than tuned: the weights were asserted, not derived from
# anything, and a project whose whole claim is that every number traces to a
# source has no business publishing one that traces to nobody. The underlying
# facts it was built on -- context and minute per goal -- are reported directly.

#' Refuse to run downstream analysis on an unvalidated goal log -- mirrors
#' the retired script's guard, generalized to the config's expected total
#' instead of a hardcoded 260.
#'
#' @param goal_events The `shearer_goal_events` tibble (goal_events_checked in `_targets.R`).
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return See description above.
#' @export
stopifnot_validated_goal_events <- function(goal_events, cfg) {
  stopifnot(
    "shearer_goal_events must match config.yaml's validation.expected_total_goals" =
      nrow(goal_events) == cfg$validation$expected_total_goals
  )
}
