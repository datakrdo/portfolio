# Transfermarkt "alletore" (all goals) page -- the only free source with
# minute, running score, and goal type for every Shearer goal, 1987-2006.
#
# One page, fetched once via cache_fetch() (ToS forbids systematic scraping),
# parsed with rvest/xml2 below. Row layout is irregular in two ways, both
# handled here:
#
#   1. A normal goal row has 11 or 12 <td>s depending on whether Transfermarkt
#      shows the opponent's league-table rank as its own cell. Fixed columns
#      are anchored from the LEFT (comp, matchday, venue, for) and from the
#      RIGHT (type, at_score, minute, pos, result, opponent) -- the variable
#      cell in between is the opponent's optional rank, which is discarded.
#   2. When Shearer scored more than once in a match, every goal after the
#      first is a 4-<td> continuation row (pos, minute, at_score, type only)
#      that inherits comp/matchday/venue/for/opponent/result from the last
#      full row above it. Missing this row type undercounts goals -- the
#      season total only reaches 260 when these are included (verified
#      against the 92/93-05/06 breakdown in docs/validation.md).
#
# A single-<td> row reading "Season XX/YY" marks the season boundary and is
# tracked as state, not emitted as a goal row.

#' Parse the cached Transfermarkt goal-log HTML into one row per goal.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble.
#' @export
ingest_transfermarkt_goal_log <- function(cfg) {
  src <- cfg$sources$transfermarkt_goal_log
  dest <- file.path(cfg$paths$raw, "transfermarkt_shearer_goals.html")
  cache_fetch(src$url, dest, "transfermarkt_goal_log", cfg)

  doc <- read_html(dest)
  tables <- html_elements(doc, "table")
  has_goal_log_header <- vapply(tables, function(t) {
    "Comp." %in% html_text2(html_elements(t, "th"))
  }, logical(1))
  table_idx <- which(has_goal_log_header)[1]
  if (is.na(table_idx)) {
    stop("No table with a 'Comp.' header found on the cached Transfermarkt page -- layout may have changed")
  }
  table_node <- tables[[table_idx]]

  rows <- html_elements(table_node, "tbody > tr")
  parse_goal_rows(rows)
}

#' Walk the goal-log <tr> nodes in document order, carrying season and
#' last-full-row state forward for continuation rows.
#'
#' @param rows A list/vector of `<tr>` nodes from the cached Transfermarkt goal-log HTML.
#' @return A tibble.
#' @export
parse_goal_rows <- function(rows) {
  season <- NA_character_
  last <- NULL  # named list: comp, matchday, venue, for_club, opponent, result
  out <- vector("list", length(rows))
  n_out <- 0L

  for (tr in rows) {
    tds <- html_elements(tr, "td")
    n <- length(tds)

    if (n == 1) {
      txt <- str_trim(html_text2(tds[[1]]))
      m <- str_match(txt, "^Season (\\d\\d/\\d\\d)")
      if (!is.na(m[1, 2])) season <- m[1, 2]
      next
    }

    if (n %in% c(11, 12)) {
      last <- list(
        comp       = html_attr(html_element(tds[[1]], "img"), "alt"),
        matchday   = str_trim(html_text2(tds[[2]])),
        venue      = str_trim(html_text2(tds[[3]])),
        for_club   = html_attr(html_element(tds[[4]], "img"), "alt"),
        opponent   = str_remove(str_trim(html_text2(tds[[n - 5]])), "\\s*\\(\\d+\\.\\)\\s*$"),
        result     = str_trim(html_text2(tds[[n - 4]]))
      )
      pos       <- str_trim(html_text2(tds[[n - 3]]))
      minute    <- str_trim(html_text2(tds[[n - 2]]))
      at_score  <- str_trim(html_text2(tds[[n - 1]]))
      goal_type <- str_trim(html_text2(tds[[n]]))
    } else if (n == 4) {
      if (is.null(last)) stop("Continuation row before any full row -- unexpected page layout")
      pos       <- str_trim(html_text2(tds[[1]]))
      minute    <- str_trim(html_text2(tds[[2]]))
      at_score  <- str_trim(html_text2(tds[[3]]))
      goal_type <- str_trim(html_text2(tds[[4]]))
    } else {
      next  # header/footer/ad rows -- not a goal row
    }

    n_out <- n_out + 1L
    out[[n_out]] <- tibble(
      season = season,
      comp = last$comp, matchday = last$matchday, venue = last$venue,
      for_club = last$for_club, opponent = last$opponent, result = last$result,
      pos = pos, minute_raw = minute, at_score = at_score, goal_type = goal_type
    )
  }

  bind_rows(out[seq_len(n_out)])
}
