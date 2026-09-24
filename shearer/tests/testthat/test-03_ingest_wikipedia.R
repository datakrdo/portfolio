library(testthat)

# Regression tests for wikitext cell-parsing helpers. These lock in real bugs
# found while ingesting live Wikipedia tables: the "||value" empty-attribute
# quirk swallowing the first cell, and hyphenated attribute names
# (data-sort-value) defeating strip_cell_attrs(). Synthetic fixtures only —
# no network — so they stay fast and don't depend on Wikipedia's current text.

test_that("split_wikitable_cells handles the normal one-cell-per-line shape", {
  row <- "\n|1\n|align=\"left\"|Player A\n|24"
  expect_equal(split_wikitable_cells(row), c("1", "Player A", "24"))
})

test_that("split_wikitable_cells collapses the '||value' empty-attribute quirk", {
  # A cell written as "||1" (empty attribute string before the real "|") must
  # not be swallowed by the leading-empty-piece drop — this previously
  # discarded the rank entirely (e.g. Thierry Henry's 2001-02 row).
  row <- "\n||1\n|align=\"left\"|Thierry Henry\n|24"
  expect_equal(split_wikitable_cells(row), c("1", "Thierry Henry", "24"))
})

test_that("strip_cell_attrs drops hyphenated attribute names", {
  cell <- 'style="text-align:left" data-sort-value="Shearer, Alan"|[[Alan Shearer]]'
  expect_equal(strip_cell_attrs(cell), "[[Alan Shearer]]")
})

test_that("strip_cell_attrs leaves a cell with no attributes unchanged", {
  expect_equal(strip_cell_attrs("260"), "260")
})

test_that("wiki_cell_is_int distinguishes numbers (incl. bold) from markup/text", {
  expect_true(wiki_cell_is_int("24"))
  expect_true(wiki_cell_is_int("'''260'''"))
  expect_false(wiki_cell_is_int("[[Alan Shearer]]"))
  expect_false(wiki_cell_is_int("{{Decimals|{{#expr:127/333}}|2}}"))
})

test_that("wiki_cell_as_int strips bold markup before parsing", {
  expect_equal(wiki_cell_as_int("'''260'''"), 260L)
  expect_equal(wiki_cell_as_int("441"), 441L)
})

test_that("clean_wiki_markup strips flagicon templates with multiple parameters", {
  # {{flagicon|FRA|1974}} previously only matched the single-parameter form.
  expect_equal(clean_wiki_markup("{{flagicon|FRA|1974}} [[Eric Cantona]]"), "Eric Cantona")
})

test_that("parse_career_stats_row reads a normal season row (rowspan club cell stripped elsewhere)", {
  row <- "\n|[[1993–94 Blackburn Rovers F.C. season|1993–94]]\n|Premier League\n|40||31||4||2||4||1||colspan=\"2\"|–||colspan=\"2\"|–||48||34"
  result <- parse_career_stats_row(row)
  expect_equal(result$season, "1993–94")
  expect_equal(result$appearances, 40L)
  expect_equal(result$goals, 31L)
})

test_that("parse_career_stats_row drops the leading rowspan club-name cell", {
  row <- "\n|rowspan=\"5\"|[[Blackburn Rovers F.C.|Blackburn Rovers]]\n|[[1992–93 Blackburn Rovers F.C. season|1992–93]]\n|[[Premier League]]\n|21||16||0||0||5||6||colspan=\"2\"|–||colspan=\"2\"|–||26||22"
  result <- parse_career_stats_row(row)
  expect_equal(result$season, "1992–93")
  expect_equal(result$appearances, 21L)
  expect_equal(result$goals, 16L)
})

test_that("normalize_season_label collapses a spelled-out century-crossing season to two digits", {
  expect_equal(normalize_season_label("1999–2000"), "1999–00")
  expect_equal(normalize_season_label("1993–94"), "1993–94")  # already two-digit, unchanged
})

test_that("parse_career_stats_row skips non-Premier-League divisions and Total rows", {
  first_division <- "\n|[[1987–88 Southampton F.C. season|1987–88]]\n|[[Football League First Division|First Division]]\n|5||3||0||0||0||0||colspan=\"2\"|–||colspan=\"2\"|–||5||3"
  expect_equal(nrow(parse_career_stats_row(first_division)), 0)

  total_row <- "\n!colspan=\"2\"|Total\n!138!!112!!8!!2!!16!!14!!8!!2!!1!!0!!171!!130"
  expect_equal(nrow(parse_career_stats_row(total_row)), 0)
})

# --- club-season results parsing -------------------------------------------
# Two article formats and two wikitable layouts, all real. The fixtures below
# are trimmed verbatim from the cached wikitext of the seasons named.

test_that("results_table_columns reads a header laid out on one line", {
  section <- "===FA Premier League===\n!Date!!Opponent!!Venue!!Result!!Attendance!!Scorers"
  expect_equal(results_table_columns(section),
               c("date", "opponent", "venue", "result", "attendance", "scorers"))
})

test_that("results_table_columns reads a header laid out one cell per line, folding '<br />' suffixes", {
  section <- "===FA Premier League===\n!Date\n!Opponent\n!Venue\n!Result<br />F–A\n!Scorers\n!Attendance"
  expect_equal(results_table_columns(section),
               c("date", "opponent", "venue", "result", "scorers", "attendance"))
})

test_that("parse_results_rows locates Scorers by header name, not position", {
  # 1992-93 Blackburn: Scorers is the 6th column, after Attendance.
  section <- paste0(
    "===FA Premier League===\n{| class=\"wikitable sortable\"\n",
    "!Date!!Opponent!!Venue!!Result!!Attendance!!Scorers\n",
    "|-\n|15 August 1992||[[Crystal Palace F.C.|Crystal Palace]]||A||3–3||17,086||[[Alan Shearer|Shearer]] (2), Ripley\n",
    "|}"
  )
  result <- parse_results_rows(section)

  expect_equal(nrow(result), 1)
  expect_equal(result$opponent, "Crystal Palace")
  expect_equal(result$venue, "A")
  expect_equal(result$scorers_raw, "Shearer (2), Ripley")
})

test_that("parse_results_rows handles the one-cell-per-line layout and drops its inline header row", {
  # 1994-95 Blackburn: cells on separate lines, Scorers before Attendance.
  section <- paste0(
    "===FA Premier League===\n{| class=\"wikitable\"\n",
    "|-\n!Date\n!Opponent\n!Venue\n!Result<br />F–A\n!Scorers\n!Attendance\n",
    "|- bgcolor=\"#ffffdd\"\n|20 August 1994\n|[[Southampton F.C.|Southampton]]\n|A\n|1–1\n|[[Alan Shearer|Shearer]]\n|14,209\n",
    "|}"
  )
  result <- parse_results_rows(section)

  expect_equal(nrow(result), 1)  # the header row is not mistaken for a fixture
  expect_equal(result$opponent, "Southampton")
  expect_equal(result$scorers_raw, "Shearer")
})

test_that("parse_football_boxes reads the away side of a fixture from the article's own perspective", {
  section <- paste0(
    "{{football box collapsible\n|date = 21 September 1996\n",
    "|team1 = [[Leeds United F.C.|Leeds United]]\n|score = 0–1\n",
    "|team2 = [[Newcastle United F.C.|Newcastle United]]\n",
    "|goals1 = \n|goals2 = [[Alan Shearer|Shearer]] {{goal|59}}\n}}"
  )
  cfg <- list(club_name_lookup = list("Leeds United" = c("Leeds", "Leeds United")))
  result <- parse_football_boxes(section, "Newcastle United", cfg)

  expect_equal(result$venue, "A")
  expect_equal(result$opponent, "Leeds United")
  expect_equal(result$result, "0–1")
  expect_equal(result$scorers_raw, "[[Alan Shearer|Shearer]] {{goal|59}}")
})

test_that("count_shearer_goals_in_box counts minutes, not templates, so a hat-trick is not read as one goal", {
  # 1996-97 v Leicester: {{goal|77||83||90}} is three goals in one template.
  raw <- "[[Robbie Elliott|R. Elliott]] {{goal|3}}<br>[[Alan Shearer|Shearer]] {{goal|77||83||90}}"
  expect_equal(count_shearer_goals_in_box(raw), list(goals = 3L, penalties = 0L))
})

test_that("count_shearer_goals_in_box separates penalties and ignores other scorers' goals", {
  raw <- "[[Alan Shearer|Shearer]] {{goal|13|pen.}}<br>[[Les Ferdinand|Ferdinand]] {{goal|61||74}}"
  expect_equal(count_shearer_goals_in_box(raw), list(goals = 1L, penalties = 1L))
})

test_that("count_shearer_goals_in_box returns zero when Shearer did not score", {
  raw <- "[[David Batty|Batty]] {{goal|3}}<br>[[Les Ferdinand|Ferdinand]] {{goal|88}}"
  expect_equal(count_shearer_goals_in_box(raw), list(goals = 0L, penalties = 0L))
})

test_that("count_shearer_goals_in_box does not mistake a sending-off for a goal", {
  raw <- "[[Alan Shearer|Shearer]] {{goal|41}}<br>[[David Batty|Batty]] {{sent off|1|52}}"
  expect_equal(count_shearer_goals_in_box(raw)$goals, 1L)
})

test_that("extract_pl_section stops at the next section so cup fixtures are excluded", {
  wikitext <- paste0(
    "===[[1996–97 FA Premier League|FA Premier League]]===\nleague content\n",
    "===FA Cup===\ncup content\n"
  )
  section <- extract_pl_section(wikitext)

  expect_true(grepl("league content", section))
  expect_false(grepl("cup content", section))
})

test_that("parse_season_pl_results reports no rows rather than failing when an article has no match data", {
  # 1998-99 Newcastle transcludes the league table instead of listing fixtures.
  wikitext <- "===[[1998–99 FA Premier League|FA Premier League]]===\n{{:1998–99 FA Premier League|showteam=NEW}}\n==Cups==\n"
  result <- parse_season_pl_results(wikitext, "Newcastle United", list())

  expect_equal(nrow(result), 0)
  expect_true(all(c("shearer_goals", "shearer_penalties") %in% names(result)))
})
