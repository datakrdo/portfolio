library(testthat)
library(dplyr)
library(tibble)

test_config <- list(
  project = list(clubs = c("Blackburn Rovers", "Newcastle United")),
  paths = list(docs = withr::local_tempdir()),
  validation = list(
    expected_total_goals = 2,
    expected_total_appearances = 2,
    expected_club_split = list("Blackburn Rovers" = 1L, "Newcastle United" = 1L)
  )
)

good_events <- tibble::tibble(
  match_id = c("m1", "m2"), minute = c(10L, 20L),
  score_after_for = c(1L, 2L), score_before_for = c(0L, 1L),
  goal_type_group = c("open_play", "penalty"),
  for_club = c("Blackburn Rovers", "Newcastle United"),
  season = c("1992–93", "2005–06"),
  home_goals = c(1L, 2L), away_goals = c(1L, 1L)
)

test_that("validate_goal_events passes silently when totals and club split match cfg", {
  expect_no_error(validate_goal_events(good_events, test_config))
})

test_that("validate_goal_events stops when the total goal count doesn't match cfg", {
  short <- good_events[1, ]
  expect_error(validate_goal_events(short, test_config), "1 rows, not the expected 2")
})

test_that("validate_goal_events stops when a club's goal count doesn't match cfg", {
  wrong_club <- good_events
  wrong_club$for_club <- c("Blackburn Rovers", "Blackburn Rovers")
  expect_error(validate_goal_events(wrong_club, test_config), "goals parsed, expected")
})

test_that("validate_goal_events stops when a post-join match column is NA (silent join failure)", {
  unjoined <- good_events
  unjoined$home_goals[1] <- NA_integer_
  expect_error(validate_goal_events(unjoined, test_config))
})
