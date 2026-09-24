library(testthat)
library(dplyr)
library(tibble)

top_scorers_fixture <- tibble::tribble(
  ~season,   ~rank, ~player,          ~club,               ~goals,
  "1994–95", 1L,    "Alan Shearer",   "Blackburn Rovers",  34L,
  "1994–95", 2L,    "Robbie Fowler",  "Liverpool",         25L,
  "1995–96", 1L,    "Alan Shearer",   "Blackburn Rovers",  31L,
  "1995–96", 2L,    "Robbie Fowler",  "Liverpool",         28L,
  "1997–98", 1L,    "Chris Sutton",   "Blackburn Rovers",  18L,
  "1997–98", 2L,    "Dion Dublin",    "Coventry City",     18L,
  "1998–99", 1L,    "Michael Owen",   "Liverpool",         18L,
  "1998–99", 7L,    "Alan Shearer",   "Newcastle United",  14L,
  # 1997-98: Shearer scored only 2 goals that season and never appears in
  # the top-10 table at all — a real gap, not a parsing failure.
  "1999–00", 1L,    "Kevin Phillips", "Sunderland",        30L,
  "1999–00", 1L,    "Jimmy Hasselbaink", "Leeds United",   30L
  # 1999-00: two players genuinely tied at rank 1 — tests leader_name joining.
)

all_time_fixture <- tibble::tribble(
  ~player,          ~goals, ~appearances, ~first_year, ~last_year,
  "Alan Shearer",   260L,   441L,         1992L,       2006L,
  "Harry Kane",     213L,   320L,         2012L,       2023L,
  "Robbie Fowler",  163L,   369L,         1993L,       2009L
)

# Only Shearer has FBref minutes pinned -- Kane/Fowler are the declared
# coverage gap historical_table() reports as NA goals_per_90, not a guess.
# fbref_goals matches Wikipedia's 260 exactly (spans_agree), so Shearer's
# goals_per_90 computes; a mismatch case is covered separately below.
fbref_seasons_fixture <- tibble::tibble(
  player = "Alan Shearer", season_raw = "1992-1993",
  minutes = 39690L, goals = 260L, assists = 64L
)

# Two seasons at 2.5 goals/match and two at 3.5, so the mean environment is
# exactly 3.0 and each era index is a round number the assertions can name.
trend_fixture <- tibble::tribble(
  ~season,    ~avg_goals_per_match,
  "1993–94",  2.5,
  "1994–95",  2.5,
  "2012–13",  3.5,
  "2013–14",  3.5
)

test_config <- list(
  project = list(player_name = "Alan Shearer"),
  comparators = list(historical = list(highlight = c("Alan Shearer", "Harry Kane")))
)

test_that("contemporary_summary reports Shearer's rank, goals, gap to the leader, and whether he led", {
  result <- contemporary_summary(top_scorers_fixture, test_config)
  expect_equal(nrow(result), 5)  # every season present in the fixture, incl. one Shearer didn't make

  s9495 <- result[result$season == "1994–95", ]
  expect_equal(s9495$shearer_goals, 34)
  expect_equal(s9495$shearer_rank, 1)
  expect_true(s9495$is_leader)
  expect_equal(s9495$gap_to_leader, 0)

  s9899 <- result[result$season == "1998–99", ]
  expect_equal(s9899$shearer_rank, 7)
  expect_false(s9899$is_leader)
  expect_equal(s9899$gap_to_leader, 4)  # 18 - 14
})

test_that("contemporary_summary declares NA (not FALSE/0) for a season Shearer isn't in", {
  result <- contemporary_summary(top_scorers_fixture, test_config)
  s9798 <- result[result$season == "1997–98", ]
  expect_true(is.na(s9798$shearer_goals))
  expect_true(is.na(s9798$shearer_rank))
  expect_true(is.na(s9798$is_leader))
})

test_that("contemporary_summary handles a tied leader without duplicating the season row", {
  result <- contemporary_summary(top_scorers_fixture, test_config)
  s9798 <- result[result$season == "1997–98", ]
  expect_equal(nrow(s9798), 1)
  expect_equal(s9798$leader_goals, 18)
})

test_that("contemporary_summary names the leader, joining tied names alphabetically", {
  result <- contemporary_summary(top_scorers_fixture, test_config)
  s9495 <- result[result$season == "1994–95", ]
  expect_equal(s9495$leader_name, "Alan Shearer")

  s9798 <- result[result$season == "1997–98", ]
  expect_equal(s9798$leader_name, "Chris Sutton")  # only Sutton is rank 1 in this fixture; Dublin is rank 2

  s9900 <- result[result$season == "1999–00", ]
  expect_equal(s9900$leader_name, "Jimmy Hasselbaink, Kevin Phillips")  # genuinely tied at rank 1
})

test_that("contemporary_summary handles empty input", {
  result <- contemporary_summary(top_scorers_fixture[0, ], test_config)
  expect_equal(nrow(result), 0)
})

test_that("contemporary_overview aggregates times led and top-10 appearances, ignoring absent seasons", {
  summary_tbl <- contemporary_summary(top_scorers_fixture, test_config)
  overview <- contemporary_overview(summary_tbl)
  expect_equal(overview$seasons, 5)
  expect_equal(overview$times_in_top10, 3)
  expect_equal(overview$times_led, 2)
})

test_that("historical_table flags configured highlight players and computes goals per appearance", {
  result <- historical_table(all_time_fixture, test_config, trend_fixture, fbref_seasons_fixture)
  expect_equal(result$player[[1]], "Alan Shearer")  # sorted by goals desc
  expect_true(result$highlight[result$player == "Alan Shearer"])
  expect_true(result$highlight[result$player == "Harry Kane"])
  expect_false(result$highlight[result$player == "Robbie Fowler"])
  expect_equal(result$goals_per_appearance[result$player == "Alan Shearer"], 260 / 441)
})

test_that("historical_table handles no highlighted players matching", {
  cfg_no_match <- list(comparators = list(historical = list(highlight = character(0))))
  result <- historical_table(all_time_fixture, cfg_no_match, trend_fixture, fbref_seasons_fixture)
  expect_false(any(result$highlight))
})

test_that("career_era_index indexes each career against the whole observed period", {
  result <- career_era_index(all_time_fixture, trend_fixture)

  # Shearer 1992-2006 covers the two 2.5 seasons -> 2.5 / 3.0.
  shearer <- result[result$player == "Alan Shearer", ]
  expect_equal(shearer$seasons_matched, 2L)
  expect_equal(shearer$era_index, 2.5 / 3.0)

  # Kane 2012-2023 covers the two 3.5 seasons -> played in the richer era.
  expect_equal(result$era_index[result$player == "Harry Kane"], 3.5 / 3.0)

  # Fowler retired in 2009, before the fixture's high-scoring seasons begin.
  expect_equal(result$era_index[result$player == "Robbie Fowler"], 2.5 / 3.0)

  # A career straddling both halves averages them and lands on the mean.
  straddler <- tibble::tibble(player = "Straddler", goals = 100L, appearances = 200L,
                              first_year = 1994L, last_year = 2014L)
  expect_equal(career_era_index(straddler, trend_fixture)$era_index, 1)
})

test_that("historical_table revises a low-scoring era's rate up and keeps the raw one", {
  result <- historical_table(all_time_fixture, test_config, trend_fixture, fbref_seasons_fixture)
  shearer <- result[result$player == "Alan Shearer", ]

  expect_equal(shearer$adjusted_goals_per_appearance, (260 / 441) / (2.5 / 3.0))
  expect_gt(shearer$adjusted_goals_per_appearance, shearer$goals_per_appearance)
  expect_equal(shearer$goals, 260L)  # raw columns survive the adjustment
})

test_that("career_era_index carries NA rather than guessing when no season matches the span", {
  outside <- tibble::tibble(player = "Someone", goals = 100L, appearances = 200L,
                            first_year = 1960L, last_year = 1970L)
  result <- career_era_index(outside, trend_fixture)

  expect_equal(result$seasons_matched, 0L)
  expect_true(is.na(result$era_index))
})

test_that("historical_table forces goals_per_90 to NA when FBref's summed goals don't match Wikipedia's total (Kane-style span mismatch)", {
  # Kane's cached FBref page originally only covered part of his career:
  # 136 of 213 Wikipedia-recorded goals. Dividing the full 213 by partial
  # minutes silently inflated the rate -- this is the regression guard.
  mismatched_fixture <- tibble::tibble(
    player = "Harry Kane", season_raw = "2012-2013",
    minutes = 16381L, goals = 136L, assists = 20L
  )
  fbref_seasons <- dplyr::bind_rows(fbref_seasons_fixture, mismatched_fixture)

  result <- historical_table(all_time_fixture, test_config, trend_fixture, fbref_seasons)
  kane <- result[result$player == "Harry Kane", ]

  expect_true(is.na(kane$goals_per_90))
  expect_true(is.na(kane$adjusted_goals_per_90))
  expect_true(is.na(kane$minutes_per_goal_or_assist))
  expect_true(is.na(kane$minutes))  # not the misleading partial figure

  # Shearer's spans agree, so his rate still computes normally.
  shearer <- result[result$player == "Alan Shearer", ]
  expect_false(is.na(shearer$goals_per_90))
})

test_that("historical_table_span_mismatches names players whose FBref/Wikipedia goal totals disagree", {
  mismatched_fixture <- tibble::tibble(
    player = "Harry Kane", season_raw = "2012-2013", minutes = 16381L, goals = 136L
  )
  fbref_seasons <- dplyr::bind_rows(
    dplyr::select(fbref_seasons_fixture, player, season_raw, minutes, goals),
    mismatched_fixture
  )

  result <- historical_table_span_mismatches(all_time_fixture, fbref_seasons)

  expect_equal(nrow(result), 1)
  expect_equal(result$player, "Harry Kane")
  expect_equal(result$wikipedia_goals, 213L)
  expect_equal(result$fbref_goals, 136L)
})
