library(testthat)
library(dplyr)
library(tibble)

season_summary_fixture <- tibble::tribble(
  ~season,    ~for_club,          ~goals, ~minutes, ~goals_per_90,
  "1992–93",  "Blackburn Rovers", 16L,    1830L,    0.787,
  "1993–94",  "Blackburn Rovers", 34L,    3145L,    0.973,
  "1996–97",  "Newcastle United", 2L,     1399L,    0.129,
  "1997–98",  "Newcastle United", 22L,    3313L,    0.598
)

test_that("trajectory_insight compares the first club against the rest", {
  out <- trajectory_insight(season_summary_fixture)
  expect_match(out, "Blackburn Rovers")
  expect_match(out, "25") # mean(16, 34)
  expect_match(out, "Newcastle United")
  expect_match(out, "12") # mean(2, 22)
})

contemporary_summary_fixture <- tibble::tribble(
  ~season,    ~shearer_rank, ~is_leader,
  "1992–93",  5L,            FALSE,
  "1993–94",  2L,            FALSE,
  "1996–97",  1L,            TRUE,
  "1997–98",  NA_integer_,   NA
)

test_that("contemporary_insight counts seasons led and runner-up", {
  out <- contemporary_insight(season_summary_fixture, contemporary_summary_fixture)
  expect_match(out, "led the league in 1 season")
  expect_match(out, "runner-up in 1 more")
})

top10_bands_fixture <- tibble::tribble(
  ~player,          ~goals_per_90, ~adjusted_goals_per_90,
  "Alan Shearer",   0.60,          0.65,
  "Thierry Henry",  0.73,          0.75,
  "Sergio Agüero",  0.84,          0.60
)

test_that("dumbbell_insight reports Shearer's adjusted rank and who's still ahead", {
  out <- dumbbell_insight(top10_bands_fixture, "Alan Shearer")
  expect_match(out, "2nd of 3")
  expect_match(out, "Thierry Henry")
  expect_false(grepl("Sergio Agüero", out))
})

season_era_fixture <- tibble::tribble(
  ~season,    ~for_club,          ~adjusted_goals_per_90,
  "1994–95",  "Blackburn Rovers", 0.90,
  "1996–97",  "Newcastle United", 0.80,
  "1999–00",  "Newcastle United", 0.60
)

test_that("season_line_insight names the peak season and whether it was matched again", {
  out <- season_line_insight(season_era_fixture)
  expect_match(out, "1994–95")
  expect_match(out, "never matched again")
})

test_that("season_line_insight recognises a later season matching the peak", {
  fixture <- season_era_fixture |>
    mutate(adjusted_goals_per_90 = if_else(season == "1999–00", 0.90, adjusted_goals_per_90))
  out <- season_line_insight(fixture)
  expect_match(out, "did get back to that level")
})

goal_type_fixture <- tibble::tribble(
  ~goal_type_group, ~n, ~share,
  "open_play",       50, 0.5,
  "penalty",         20, 0.2,
  "unknown",         30, 0.3
)

test_that("goal_type_insight reports open play, penalty and unknown shares", {
  out <- goal_type_insight(goal_type_fixture)
  expect_match(out, "50%")
  expect_match(out, "20%")
  expect_match(out, "30%")
})

context_fixture <- tibble::tribble(
  ~context,          ~n,  ~share,        ~n_total,
  "go_ahead",        42,  42 / 100,      100,
  "equaliser",       18,  18 / 100,      100,
  "extend_lead",     36,  36 / 100,      100,
  "reduce_deficit",  4,   4 / 100,       100
)

test_that("context_insight names the decisive-goal share", {
  out <- context_insight(context_fixture)
  expect_match(out, "42%")
  expect_match(out, "18%")
  expect_match(out, "60%") # clutch_share = go_ahead + equaliser
  expect_match(out, "4%")
})

comparator_clutch_fixture <- tibble::tribble(
  ~player,          ~clutch_share,
  "Alan Shearer",   0.60,
  "Thierry Henry",  0.65,
  "Harry Kane",     0.55
)

test_that("comparator_clutch_insight ranks Shearer and names the leader", {
  out <- comparator_clutch_insight(comparator_clutch_fixture, "Alan Shearer")
  expect_match(out, "2nd of 3")
  expect_match(out, "Thierry Henry")
})

test_that("comparator_clutch_insight recognises Shearer as the leader", {
  out <- comparator_clutch_insight(comparator_clutch_fixture, "Thierry Henry")
  expect_match(out, "1st of 3")
  expect_match(out, "highest share")
})

rival_matches_fixture <- tibble::tribble(
  ~shearer_goals,
  1L, 0L, 2L, 0L
)

test_that("rival_insight reports meetings, goals and scoring rate", {
  out <- rival_insight(rival_matches_fixture, "Arsenal")
  expect_match(out, "3 goals in 4 meetings")
  expect_match(out, "scored in 2 of them")
})

test_that("rival_insight handles no meetings and never-scored cases", {
  expect_match(rival_insight(rival_matches_fixture[0, ], "Arsenal"), "No meetings")
  never <- tibble::tibble(shearer_goals = c(0L, 0L, 0L))
  expect_match(rival_insight(never, "Arsenal"), "never")
})
