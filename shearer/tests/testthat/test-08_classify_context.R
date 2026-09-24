library(testthat)
library(dplyr)
library(tibble)

test_config <- list(context_classification = list(late_goal_minute = 75, close_margin_abs = 1))

events_fixture <- tibble::tribble(
  ~score_before_for, ~score_before_against, ~minute, ~home_goals, ~away_goals,
  0,                  1,                     20,      2,           2,          # equaliser
  1,                  1,                     50,      2,           1,          # go_ahead
  0,                  2,                     10,      1,           2,          # reduce_deficit
  2,                  0,                     80,      3,           0,          # extend_lead
  1,                  0,                     78,      2,           1           # late, already ahead
)

test_that("classify_goal_context maps margin_before to the four context labels", {
  out <- classify_goal_context(events_fixture, test_config)
  expect_equal(out$context, c("equaliser", "go_ahead", "reduce_deficit", "extend_lead", "extend_lead"))
})

test_that("classify_goal_context: margin_before == 0 maps to go_ahead, not extend_lead", {
  level <- tibble::tibble(score_before_for = 3, score_before_against = 3, minute = 60,
                           home_goals = 4, away_goals = 3)
  out <- classify_goal_context(level, test_config)
  expect_equal(out$context, "go_ahead")
})

test_that("classify_goal_context flags late goals using cfg's late_goal_minute threshold", {
  out <- classify_goal_context(events_fixture, test_config)
  expect_equal(out$is_late_goal, c(FALSE, FALSE, FALSE, TRUE, TRUE))
})

test_that("classify_goal_context flags a close pre-goal state using cfg's close_margin_abs", {
  out <- classify_goal_context(events_fixture, test_config)
  expect_equal(out$state_close_before_goal, c(TRUE, TRUE, FALSE, FALSE, TRUE))
})

test_that("classify_goal_context flags a close final margin from the match scoreline", {
  out <- classify_goal_context(events_fixture, test_config)
  expect_equal(out$final_margin_le_1, c(TRUE, TRUE, TRUE, FALSE, TRUE))
})

test_that("classify_goal_context sets final_margin_le_1 to NA when home_goals/away_goals are absent (comparator branch)", {
  no_score <- events_fixture |> select(-home_goals, -away_goals)
  out <- classify_goal_context(no_score, test_config)
  expect_true(all(is.na(out$final_margin_le_1)))
  expect_equal(out$context, c("equaliser", "go_ahead", "reduce_deficit", "extend_lead", "extend_lead"))
})
