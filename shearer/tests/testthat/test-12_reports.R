library(testthat)
library(dplyr)
library(tibble)

goal_type_fixture <- tibble::tribble(
  ~goal_type_group, ~n,  ~share,     ~n_total,
  "open_play",       151, 151 / 260, 260,
  "penalty",         57,  57 / 260,  260,
  "unknown",         46,  46 / 260,  260,
  "free_kick",       6,   6 / 260,   260
)

overview_fixture <- tibble::tibble(seasons = 14L, times_in_top10 = 10L, times_led = 3L)

matches_fixture <- tibble::tibble(match_id = "m1", date = as.Date("1992-08-15"),
                                   home_team = "Blackburn Rovers", away_team = "Arsenal",
                                   home_goals = 3L, away_goals = 3L, season = "1992–93")
goal_events_fixture <- tibble::tibble(match_id = "m1", minute = 66L, for_club = "Blackburn Rovers",
                                       opponent = "Arsenal", score_before_for = 1L,
                                       score_after_for = 2L, goal_type_group = "open_play",
                                       context = "equaliser", season = "1992–93")

test_config <- list(
  comparators = list(historical = list(highlight = c("Alan Shearer", "Harry Kane"))),
  sources = list(
    football_data_co_uk = list(base_url = "https://www.football-data.co.uk/mmz4281"),
    transfermarkt_goal_log = list(url = "https://www.transfermarkt.com/alan-shearer/alletore/spieler/3110")
  )
)

test_that("write_coverage_report embeds the goal-type coverage numbers from its input, not typed literals", {
  path <- withr::local_tempfile(fileext = ".md")
  result_path <- write_coverage_report(goal_type_fixture, overview_fixture, path)
  expect_equal(result_path, path)
  content <- readLines(path)
  text <- paste(content, collapse = "\n")
  expect_match(text, "151")
  expect_match(text, "58%")   # 151/260, rounded
  expect_match(text, "46")
  expect_match(text, "10")    # times_in_top10
  expect_match(text, "14")    # seasons
})

test_that("write_coverage_report declares unknown coverage rather than omitting it", {
  path <- withr::local_tempfile(fileext = ".md")
  write_coverage_report(goal_type_fixture, overview_fixture, path)
  text <- paste(readLines(path), collapse = "\n")
  expect_match(text, "unknown")
})

test_that("write_data_dictionary lists every column of the tables it's given", {
  path <- withr::local_tempfile(fileext = ".md")
  write_data_dictionary(list(matches = matches_fixture, shearer_goal_events = goal_events_fixture), path)
  text <- paste(readLines(path), collapse = "\n")
  for (col in c(names(matches_fixture), names(goal_events_fixture))) {
    expect_match(text, col, fixed = TRUE, info = col)
  }
})

test_that("write_methodology_report cites real source URLs from cfg, not hardcoded ones", {
  path <- withr::local_tempfile(fileext = ".md")
  write_methodology_report(test_config, path)
  text <- paste(readLines(path), collapse = "\n")
  expect_match(text, "https://www.football-data.co.uk/mmz4281", fixed = TRUE)
  expect_match(text, "https://www.transfermarkt.com/alan-shearer/alletore/spieler/3110", fixed = TRUE)
  expect_match(text, "football_data_co_uk", fixed = TRUE)
  expect_match(text, "transfermarkt_goal_log", fixed = TRUE)
})

test_that("write_limitations_report states both interval methods", {
  path <- withr::local_tempfile(fileext = ".md")
  write_limitations_report(test_config, path)
  text <- paste(readLines(path), collapse = "\n")
  expect_match(text, "Wilson", fixed = TRUE)
  expect_match(text, "Beta-Binomial", fixed = TRUE)
})
