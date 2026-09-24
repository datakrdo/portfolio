library(testthat)
library(dplyr)

test_config <- list(
  project = list(clubs = c("Blackburn Rovers", "Newcastle United")),
  club_name_lookup = list(
    "Blackburn Rovers" = c("Blackburn", "Blackburn Rovers"),
    "Newcastle United" = c("Newcastle", "Newcastle United"),
    "Manchester United" = c("Man United", "Manchester United", "Man Utd")
  )
)

test_that("normalize_club_name maps every alias to its canonical form", {
  expect_equal(normalize_club_name("Blackburn", test_config), "Blackburn Rovers")
  expect_equal(normalize_club_name("Man Utd", test_config), "Manchester United")
  expect_equal(normalize_club_name("Blackburn Rovers", test_config), "Blackburn Rovers")
})

test_that("normalize_club_name passes through names absent from the lookup unchanged", {
  expect_equal(normalize_club_name("Arsenal", test_config), "Arsenal")
})

test_that("parse_footballdata_date handles both 2-digit and 4-digit years in the same column", {
  x <- c("17/08/2002", "13/08/05")
  parsed <- parse_footballdata_date(x)
  expect_equal(parsed, as.Date(c("2002-08-17", "2005-08-13")))
})

test_that("parse_wiki_date parses 'D Month YYYY' independent of system locale", {
  expect_equal(parse_wiki_date("15 August 1992"), as.Date("1992-08-15"))
  expect_equal(parse_wiki_date("3 May 1996"), as.Date("1996-05-03"))
})

test_that("football_data_season_label converts a 4-digit code to an en-dash label", {
  expect_equal(football_data_season_label("9394"), "1993–94")
  expect_equal(football_data_season_label("0506"), "2005–06")
})
