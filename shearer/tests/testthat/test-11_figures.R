library(testthat)
library(dplyr)
library(tibble)
library(ggplot2)

season_summary_fixture <- tibble::tribble(
  ~season,    ~for_club,          ~goals,
  "1992–93",  "Blackburn Rovers", 16,
  "1993–94",  "Blackburn Rovers", 31,
  "1994–95",  "Blackburn Rovers", 34,
  "1996–97",  "Newcastle United", 25
)

goal_type_fixture <- tibble::tribble(
  ~goal_type_group, ~n,  ~share,       ~n_total,
  "open_play",       151, 151 / 260,   260,
  "penalty",         57,  57 / 260,    260,
  "unknown",         46,  46 / 260,    260,
  "free_kick",       6,   6 / 260,     260
)

context_fixture <- tibble::tribble(
  ~context,          ~n,
  "go_ahead",        109,
  "extend_lead",     93,
  "equaliser",       47,
  "reduce_deficit",  11
)

test_config <- list(project = list(clubs = c("Blackburn Rovers", "Newcastle United"),
                                   player_name = "Alan Shearer"))

historical_fixture <- tibble::tribble(
  ~player,          ~goals, ~appearances, ~highlight, ~goals_per_appearance, ~era_index, ~adjusted_goals_per_appearance, ~seasons_matched,
  "Alan Shearer",   260,    441,          TRUE,       260 / 441,             0.967,      (260 / 441) / 0.967,            13,
  "Harry Kane",     213,    320,          TRUE,       213 / 320,             1.021,      (213 / 320) / 1.021,            12,
  "Wayne Rooney",   208,    491,          TRUE,       208 / 491,             0.987,      (208 / 491) / 0.987,            17,
  "Sadio Mané",     111,    355,          FALSE,      111 / 355,             1.010,      (111 / 355) / 1.010,            8
)

shots_fixture <- tibble::tribble(
  ~x,    ~y,   ~body_part,  ~outcome,
  108,   31.8, "Head",      "Off T",
  106.1, 72.9, "Right Foot","Off T"
)

test_that("fig_trajectory returns a ggplot keyed on season and goals, faceted/coloured by club", {
  p <- fig_trajectory(season_summary_fixture, test_config)
  expect_s3_class(p, "ggplot")
  expect_setequal(names(p$data), names(season_summary_fixture))
  expect_equal(nrow(p$data), nrow(season_summary_fixture))
})

test_that("fig_trajectory derives its club colour mapping from cfg (first club = accent), not a hardcoded name", {
  cfg_reordered <- list(project = list(clubs = c("Newcastle United", "Blackburn Rovers")))
  built <- ggplot_build(fig_trajectory(season_summary_fixture, cfg_reordered))
  point_colours <- built$data[[2]] |> select(colour) |> distinct() |> pull()
  expect_setequal(point_colours, c(SHEARER_COLOUR, NEUTRAL_COLOUR))
})

test_that("fig_goal_types returns a ggplot whose bars sum to n_total, unknown included", {
  p <- fig_goal_types(goal_type_fixture)
  expect_s3_class(p, "ggplot")
  expect_equal(sum(p$data$n), goal_type_fixture$n_total[[1]])
  expect_true("unknown" %in% p$data$goal_type_group)
})

test_that("fig_context returns a ggplot with all four context categories", {
  p <- fig_context(context_fixture)
  expect_s3_class(p, "ggplot")
  expect_setequal(p$data$context, context_fixture$context)
})

test_that("fig_comparators returns a ggplot with no goals-per-90 column present anywhere", {
  p <- fig_comparators(historical_fixture, test_config)
  expect_s3_class(p, "ggplot")
  expect_false(any(grepl("90", names(p$data))))
  expect_equal(nrow(p$data), nrow(historical_fixture))
})

test_that("fig_comparators drops players with no measurable era index rather than erroring", {
  fixture_with_na <- bind_rows(
    historical_fixture,
    tibble::tibble(player = "Unmeasurable", goals = 100, appearances = 200,
                    highlight = FALSE, goals_per_appearance = 0.5, era_index = NA_real_,
                    adjusted_goals_per_appearance = NA_real_, seasons_matched = 0)
  )
  p <- fig_comparators(fixture_with_na, test_config)
  expect_equal(nrow(p$data), nrow(historical_fixture))
})

test_that("fig_statsbomb_shots renders on a pitch and reports the shot count in its title", {
  p <- fig_statsbomb_shots(shots_fixture)
  expect_s3_class(p, "ggplot")
  expect_equal(nrow(p$data), nrow(shots_fixture))
  expect_match(p$labels$title, "2")
})

test_that("fig_statsbomb_shots handles zero shots without erroring", {
  p <- fig_statsbomb_shots(shots_fixture[0, ])
  expect_s3_class(p, "ggplot")
  expect_equal(nrow(p$data), 0)
})
