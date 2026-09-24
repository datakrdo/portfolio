library(testthat)
library(dplyr)

taxonomy_fixture <- list(
  open_play = c("Right-footed shot", "Left-footed shot", "Header"),
  penalty = c("Penalty", "Penalty rebound"),
  free_kick = c("Direct free kick"),
  unknown = c("Not reported")
)

test_that("tm_season_to_label converts Transfermarkt's 'YY/YY' to the en-dash label", {
  expect_equal(tm_season_to_label("92/93"), "1992–93")
  expect_equal(tm_season_to_label("05/06"), "2005–06")
})

test_that("parse_minute folds stoppage time into the preceding minute", {
  expect_equal(parse_minute("5'"), 5L)
  expect_equal(parse_minute("90'+3"), 93L)
  expect_equal(parse_minute("45'+2"), 47L)
})

test_that("classify_goal_type maps every taxonomy alias to its group", {
  expect_equal(unname(classify_goal_type("Header", taxonomy_fixture)), "open_play")
  expect_equal(unname(classify_goal_type("Penalty", taxonomy_fixture)), "penalty")
  expect_equal(unname(classify_goal_type("Direct free kick", taxonomy_fixture)), "free_kick")
  expect_equal(unname(classify_goal_type("Not reported", taxonomy_fixture)), "unknown")
})

test_that("classify_goal_type errors on a goal type absent from the taxonomy, rather than guessing", {
  expect_error(classify_goal_type("Bicycle kick", taxonomy_fixture), "Unclassified goal type")
})

comparator_config_fixture <- list(club_name_lookup = list())

fbref_events_fixture <- tibble::tribble(
  ~is_own_goal, ~season_raw,    ~opponent, ~for_club,   ~minute_raw, ~venue,  ~score_at,
  FALSE,        "1996-1997",    "Arsenal", "Newcastle", "20'",       "Home",  "2:1",
  FALSE,        "1996-1997",    "Arsenal", "Newcastle", "80'",       "Away",  "1:2"
)

test_that("build_comparator_goal_events derives score_before_for/against for both venues", {
  out <- build_comparator_goal_events(comparator_config_fixture, fbref_events_fixture)
  expect_equal(out$score_before_for, c(1L, 1L))
  expect_equal(out$score_before_against, c(1L, 1L))
})

test_that("build_comparator_goal_events drops own goals", {
  with_og <- fbref_events_fixture |> dplyr::mutate(is_own_goal = c(FALSE, TRUE))
  out <- build_comparator_goal_events(comparator_config_fixture, with_og)
  expect_equal(nrow(out), 1L)
})
