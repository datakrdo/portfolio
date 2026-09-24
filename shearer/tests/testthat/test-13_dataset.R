library(testthat)

valid_args <- function(n_goals = 3) {
  list(
    matches = tibble::tibble(match_id = "m1", season = "1992–93",
                              home_team = "Blackburn Rovers", away_team = "Arsenal"),
    goal_events = tibble::tibble(match_id = rep("m1", n_goals), season = "1992–93",
                                  for_club = "Blackburn Rovers", opponent = "Arsenal",
                                  minute = seq_len(n_goals)),
    season_summary = tibble::tibble(season = "1992–93", goals = n_goals),
    goal_type_breakdown = tibble::tibble(goal_type_group = "open_play", n = n_goals),
    contemporary_summary = tibble::tibble(season = "1992–93", leader_goals = n_goals),
    contemporary_overview = tibble::tibble(times_led = 1L),
    historical_table = tibble::tibble(player = "Alan Shearer", goals = n_goals),
    league_scoring_trend = tibble::tibble(season = "1993–94", avg_goals_per_match = 2.5),
    season_era_index = tibble::tibble(season = "1993–94", adjusted_goals_per_appearance = 0.8,
                                       season_era_index = 0.96),
    context_breakdown = tibble::tibble(context = "go_ahead", n = n_goals, share = 1),
    comparator_context_breakdown = tibble::tibble(player = "Alan Shearer", context = "go_ahead",
                                                   n = n_goals, share = 1, n_total = n_goals),
    .expected_goals = n_goals
  )
}

test_that("a valid set of tables constructs without error", {
  dat <- do.call(shearer_data, valid_args())
  expect_s7_class(dat, shearer_data)
  expect_equal(nrow(dat@goal_events), 3)
})

test_that("an empty table is rejected", {
  args <- valid_args()
  args$matches <- args$matches[0, ]
  expect_error(do.call(shearer_data, args), "empty table")
})

test_that("a goal count mismatch is rejected with a message naming it", {
  args <- valid_args(n_goals = 3)
  args$.expected_goals <- 260
  expect_error(do.call(shearer_data, args), "260")
})

test_that("print returns its argument invisibly", {
  dat <- do.call(shearer_data, valid_args())
  expect_invisible(print(dat))
})
