library(testthat)
library(dplyr)
library(tibble)

goal_events_fixture <- tibble::tribble(
  ~season,    ~for_club,          ~opponent,   ~minute, ~goal_type_group, ~match_id,
  "1992–93",  "Blackburn Rovers", "Arsenal",   10L,     "open_play",      "m1",
  "1992–93",  "Blackburn Rovers", "Arsenal",   80L,     "penalty",        "m1",
  "1993–94",  "Blackburn Rovers", "Sunderland",5L,      "open_play",      "m2",
  "1996–97",  "Newcastle United", "Sunderland",90L,     "unknown",        "m3",
  "1996–97",  "Newcastle United", "Chelsea",   45L,     "free_kick",      "m4"
)

test_config <- list(
  project = list(player_name = "Alan Shearer"),
  club_name_lookup = list()
)

fbref_seasons_fixture <- tibble::tribble(
  ~player,        ~season_raw,   ~minutes,
  "Alan Shearer", "1992-1993",   1800L,
  "Alan Shearer", "1993-1994",   900L,
  "Alan Shearer", "1996-1997",   1350L
)

test_that("season_summary counts goals per season and club", {
  result <- season_summary(goal_events_fixture, fbref_seasons_fixture, test_config)
  expect_equal(nrow(result), 3)
  expect_equal(result$goals[result$season == "1992–93"], 2)
  expect_equal(result$goals[result$season == "1996–97" & result$for_club == "Newcastle United"], 2)
  expect_equal(result$goals_per_90[result$season == "1993–94"], 1 / (900 / 90))
})

test_that("season_summary handles empty input", {
  result <- season_summary(goal_events_fixture[0, ], fbref_seasons_fixture, test_config)
  expect_equal(nrow(result), 0)
})

test_that("goals_by_opponent counts and sorts descending", {
  result <- goals_by_opponent(goal_events_fixture)
  expect_equal(result$goals[[1]], max(result$goals))
  expect_equal(result$goals[result$opponent == "Sunderland"], 2)
  expect_true(all(diff(result$goals) <= 0))
})

test_that("wilson_ci returns estimate within [lower, upper] and bounds within [0, 1]", {
  ci <- wilson_ci(2, 5)
  expect_equal(ci$estimate, 0.4)
  expect_true(ci$lower >= 0 && ci$lower <= ci$estimate)
  expect_true(ci$upper <= 1 && ci$upper >= ci$estimate)
})

test_that("wilson_ci handles x = 0 and x = n edge cases without going outside [0, 1]", {
  zero <- wilson_ci(0, 5)
  expect_equal(zero$lower, 0)
  expect_true(zero$upper > 0)

  full <- wilson_ci(5, 5)
  expect_equal(full$upper, 1)
  expect_true(full$lower < 1)
})

test_that("wilson_ci errors informatively when n is 0", {
  expect_error(wilson_ci(0, 0), "n must be > 0")
})

test_that("goal_type_breakdown shares sum to 1 and declares unknown coverage", {
  result <- goal_type_breakdown(goal_events_fixture)
  expect_equal(sum(result$share), 1)
  expect_equal(result$n[result$goal_type_group == "unknown"], 1)
  expect_equal(result$n_total[[1]], 5)
})

test_that("goal_type_breakdown handles a fixture with no unknown goals", {
  known_only <- goal_events_fixture |> dplyr::filter(goal_type_group != "unknown")
  result <- goal_type_breakdown(known_only)
  expect_false("unknown" %in% result$goal_type_group)
})

test_that("penalty_summary counts penalties out of total goals", {
  result <- penalty_summary(goal_events_fixture)
  expect_equal(result$n_penalty, 1)
  expect_equal(result$n_total, 5)
  expect_equal(result$share, 1 / 5)
})

test_that("penalty_summary handles zero penalties", {
  no_pens <- goal_events_fixture |> dplyr::filter(goal_type_group != "penalty")
  result <- penalty_summary(no_pens)
  expect_equal(result$n_penalty, 0)
  expect_equal(result$share, 0)
})

context_events_fixture <- tibble::tribble(
  ~context,
  "equaliser", "go_ahead", "extending_lead", "extending_lead", "cushion"
)

test_that("clutch_share sums equaliser and go-ahead goals as a share of the total", {
  breakdown <- context_breakdown(context_events_fixture)
  expect_equal(clutch_share(breakdown), 2 / 5)
})

test_that("clutch_share is 0 when no goal was an equaliser or go-ahead", {
  no_clutch <- context_events_fixture |> dplyr::filter(!context %in% c("equaliser", "go_ahead"))
  breakdown <- context_breakdown(no_clutch)
  expect_equal(clutch_share(breakdown), 0)
})

football_data_fixture <- tibble::tribble(
  ~season_code, ~FTHG, ~FTAG,
  "9394",        2,     1,
  "9394",        0,     0,
  "0506",        3,     3,
  "0506",        1,     2,
  "0506",        2,     1
)

test_that("league_scoring_trend averages goals per match within each season, not across seasons", {
  result <- league_scoring_trend(football_data_fixture)
  expect_equal(result$avg_goals_per_match[result$season == "1993–94"], 1.5)
  expect_equal(result$avg_goals_per_match[result$season == "2005–06"], 4)
  expect_equal(result$n_matches[result$season == "2005–06"], 3)
})
