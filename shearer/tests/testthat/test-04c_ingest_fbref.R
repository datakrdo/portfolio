library(testthat)

# Synthetic fixtures only -- no network -- for FBref's two custom HTML walks
# (data-stat-keyed tables and newline-separated match-report events), since
# html_table() breaks on both (see R/04c_ingest_fbref.R header comment).

test_that("fbref_table_rows keys columns by data-stat, not header position", {
  html <- '<table><tbody>
    <tr><th data-stat="date">1995-08-19</th><td data-stat="opponent">Arsenal</td>
        <td data-stat="goals">1</td></tr>
    <tr><th data-stat="date">1995-08-23</th><td data-stat="opponent">Everton</td>
        <td data-stat="goals">0</td></tr>
  </tbody></table>'
  table_node <- rvest::html_element(xml2::read_html(html), "table")
  result <- fbref_table_rows(table_node)
  expect_equal(result$date, c("1995-08-19", "1995-08-23"))
  expect_equal(result$opponent, c("Arsenal", "Everton"))
  expect_equal(result$goals, c("1", "0"))
})

test_that("fbref_table_rows skips spacer/thead rows and pulls the match-report href", {
  html <- '<table><tbody>
    <tr class="thead"><th data-stat="date">Date</th></tr>
    <tr><th data-stat="date">1995-08-19</th>
        <td data-stat="match_report"><a href="/en/matches/abc123">Match Report</a></td></tr>
  </tbody></table>'
  table_node <- rvest::html_element(xml2::read_html(html), "table")
  result <- fbref_table_rows(table_node)
  expect_equal(nrow(result), 1L)
  expect_equal(result$report_href, "/en/matches/abc123")
})

test_that("fbref_parse_event reads minute/score/player/detail from a goal event", {
  html <- '<div class="event a">
    <div class="event_icon goal"></div>
    <div>16&#8217;<br>1:0<br>Alan Shearer<br>Assist: Tim Flowers</div>
  </div>'
  event_node <- rvest::html_element(xml2::read_html(html), "div.event")
  result <- fbref_parse_event(event_node)
  expect_equal(result$minute_raw, "16’")
  expect_equal(result$score_at, "1:0")
  expect_equal(result$player, "Alan Shearer")
  expect_equal(result$goal_type_raw, "Assist: Tim Flowers")
  expect_false(result$is_penalty)
  expect_false(result$is_own_goal)
})

test_that("fbref_parse_event flags a penalty from the icon class, not the text", {
  html <- '<div class="event a">
    <div class="event_icon penalty_goal"></div>
    <div>60&#8217;<br>3:0<br>Alan Shearer<br>Penalty Kick</div>
  </div>'
  event_node <- rvest::html_element(xml2::read_html(html), "div.event")
  result <- fbref_parse_event(event_node)
  expect_true(result$is_penalty)
  expect_false(result$is_own_goal)
})

test_that("fbref_find_table matches a page-template id infix (e.g. stats_standard_ks_dom_lg)", {
  html <- '<div><table id="stats_standard_ks_dom_lg"><tbody>
    <tr><th data-stat="season">1999-2000</th></tr>
  </tbody></table></div>'
  doc <- xml2::read_html(html)
  table_node <- fbref_find_table(doc, "stats_standard_dom_lg")
  expect_false(is.null(table_node))
  expect_equal(xml2::xml_attr(table_node, "id"), "stats_standard_ks_dom_lg")
})

test_that("fbref_find_table pulls a comment-hidden table out and re-parses it", {
  html <- paste0(
    '<div><!--<table id="stats_standard_dom_lg"><tbody>',
    '<tr><th data-stat="season">1999-2000</th></tr>',
    '</tbody></table>--></div>'
  )
  doc <- xml2::read_html(html)
  table_node <- fbref_find_table(doc, "stats_standard_dom_lg")
  expect_false(is.null(table_node))
})
