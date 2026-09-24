# Wikipedia is the only free source with the 1992-93 season (football-data.co.uk
# starts at 1993-94) and the only source for the season-by-season "who else was
# scoring" comparator tables. Every fetch is cached with its revision id so a
# rerun is reproducible against a fixed Wikipedia revision, not "today".

WIKI_API <- "https://en.wikipedia.org/w/api.php"

#' Wikipedia article titles spell out a century-crossing season in full
#' ("1999-2000"); every other table in this project (matches, season_summary,
#' league_scoring_trend) uses the two-digit convention ("1999-00") that
#' football_data_season_label() produces. Normalizing here keeps every
#' season column joinable on the same label -- without it, tables built from
#' Wikipedia (contemporary_summary, shearer_appearances) silently fail to
#' join against football-data-derived tables for that one season.
#'
#' @param x See description above.
#' @return See description above.
#' @export
normalize_season_label <- function(x) {
  m <- str_match(x, "^(\\d{4})\\u2013(\\d{4})$")
  if_else(is.na(m[, 1]), x, paste0(m[, 2], "\u2013", str_sub(m[, 3], 3, 4)))
}

#' The 14 career seasons as the two-digit labels every downstream table uses.
#'
#' config.yaml stores them in Wikipedia's article-title convention
#' ("1999-2000") because that is what the fetch URLs need. Anything that
#' *compares* or *joins* on a season must go through here instead of reading
#' the config list directly -- otherwise 1999-2000 is the one season that
#' silently fails to match and disappears from the result.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return See description above.
#' @export
career_season_labels <- function(cfg) {
  normalize_season_label(unlist(cfg$sources$wikipedia_season_results$seasons))
}

#' Shearer played for Blackburn Rovers 1992-93 to 1995-96 and Newcastle United
#' 1996-97 to 2005-06 (transferred summer 1996). Used to pick which club's
#' Wikipedia season article to read. Vectorised over `season_label`.
#'
#' Only meaningful for the 14 seasons of Shearer's career -- any other season
#' label falls through to Newcastle, so callers working off a wider season
#' list must filter first (see build_matches()).
#'
#' @param season_label A season label, e.g. "1996-97".
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return See description above.
#' @export
club_for_season <- function(season_label, cfg) {
  blackburn_seasons <- c("1992\u201393", "1993\u201394", "1994\u201395", "1995\u201396")
  if_else(season_label %in% blackburn_seasons, cfg$project$clubs[[1]], cfg$project$clubs[[2]])
}

#' Fetch a Wikipedia article's wikitext (cached) via action=parse.
#'
#' @param page_title Wikipedia article title to fetch.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param dest_slug Filename slug used to cache the fetched wikitext under data/raw/.
#' @return See description above.
#' @export
wiki_fetch_wikitext <- function(page_title, cfg, dest_slug) {
  url <- glue(
    "{WIKI_API}?action=parse&page={URLencode(page_title)}",
    "&prop=wikitext&format=json"
  )
  dest <- glue("{cfg$paths$raw}/wikipedia_{dest_slug}.json")
  parsed <- cache_fetch_json(url, dest, "wikipedia", cfg, notes = page_title)

  if (!is.null(parsed$error)) {
    stop(glue("Wikipedia parse error for '{page_title}': {parsed$error$info}"))
  }
  parsed$parse$wikitext[[1]]
}

#' The league-results section of a club-season article.
#'
#' Heading style varies: the Blackburn articles write a plain
#' `===FA Premier League===`, the Newcastle ones a wikilinked
#' `===[[1996-97 FA Premier League|FA Premier League]]===`. The lookahead
#' stops at the next same-or-higher-level heading, which is what keeps the
#' cup competitions that follow out of the result.
PL_SECTION_RE <- paste0(
  "(?s)===\\s*(?:\\[\\[[^]|]*\\|)?(?:FA )?Premier League(?:\\]\\])?\\s*===",
  ".*?(?=\\n===|\\n==[^=])"
)

#' Returns NA when the article has no such section. Not every season article
#' carries one: 1998-99 transcludes the league table from the competition
#' article and 2005-06 has the heading with no match data. Callers that can
#' report partial coverage handle the NA; ingest_1992_93_matches() cannot, and
#' stops.
#'
#' @param wikitext Raw wikitext of a Wikipedia article (or section).
#' @return See description above.
#' @export
extract_pl_section <- function(wikitext) {
  str_extract(wikitext, PL_SECTION_RE)
}

#' Parse a club's "FA Premier League" results wikitable out of its season
#' article wikitext. Returns Date / Opponent / Venue / Result / Scorers.
#' Used for the four Blackburn seasons; the Newcastle articles use
#' {{football box collapsible}} templates instead (see parse_football_boxes()).
#'
#' @param wikitext Raw wikitext of a Wikipedia article (or section).
#' @return A tibble.
#' @export
parse_club_season_pl_results <- function(wikitext) {
  section <- extract_pl_section(wikitext)
  if (is.na(section)) stop("Could not find a Premier League results section")
  parse_results_rows(section)
}

parse_results_rows <- function(section) {
  cols <- results_table_columns(section)

  rows <- str_split(section, "\\n\\|-")[[1]][-1]
  rows <- str_remove(rows, "\\n\\|\\}\\s*$")  # strip the table-close marker off the last row
  cells <- map(rows, split_results_row_cells)

  cell <- function(name) {
    n <- match(name, cols)
    if (is.na(n)) return(rep(NA_character_, length(cells)))
    map_chr(cells, ~ if (length(.x) >= n) .x[[n]] else NA_character_)
  }

  tibble(
    date = cell("date"),
    opponent = clean_wikilinks(cell("opponent")),
    venue = cell("venue"),
    result = cell("result"),
    scorers_raw = clean_wikilinks(cell("scorers"))
  ) |>
    # A real fixture date starts with the day number. This also drops the
    # header row of the tables that put it inside the first "|-" block.
    filter(str_detect(coalesce(date, ""), "^\\d"))
}

#' Column order is not stable across articles -- 1992-93 runs
#' Date/Opponent/Venue/Result/Attendance/Scorers, 1994-95 swaps the last two --
#' so the cells are located by header name rather than by position. Header text
#' is truncated at the first tag to fold "Result<br />F-A" into "result".
#'
#' @param section A wikitext section (e.g. from `extract_pl_section()`).
#' @return See description above.
#' @export
results_table_columns <- function(section) {
  lines <- str_split(section, "\n")[[1]]
  lines[str_detect(lines, "^!")] |>
    str_remove("^!") |>
    str_split("!!") |>
    unlist() |>
    strip_cell_attrs() |>
    clean_wikilinks() |>
    str_remove("<.*") |>
    str_trim() |>
    tolower()
}

# Cells are laid out one of two ways: all on one line separated by literal "||"
# (1992-93, 1995-96) or one per line each prefixed with a single "|"
# (1994-95). Splitting on both -- but never on a bare single "|" -- keeps
# wikilinks such as "[[Crystal Palace F.C.|Crystal Palace]]" intact.
split_results_row_cells <- function(row) {
  row |>
    str_remove("^.*?\\n") |>   # drop the "|-style=..." row-marker line
    str_remove("^\\|") |>       # drop the leading "|" of the first cell
    str_split("\\|\\||\\n\\|") |>
    unlist() |>
    strip_cell_attrs() |>
    str_trim()
}

clean_wikilinks <- function(x) {
  x |>
    str_replace_all("\\[\\[([^|\\]]*)\\|([^\\]]*)\\]\\]", "\\2") |>
    str_replace_all("\\[\\[([^\\]]*)\\]\\]", "\\1") |>
    str_remove_all("\\{\\{flag\\s?icon(\\|[^{}]*)?\\}\\}") |>
    str_trim()
}

#' Count Shearer's goals per match from a "Scorers" wikitext cell such as
#' "Shearer (2, 1 pen), Ripley" -> list(goals = 2, penalties = 1).
#' Returns 0 goals if Shearer did not score in that match.
#'
#' @param scorers_raw Raw "Scorers" wikitext cell text.
#' @return See description above.
#' @export
count_shearer_goals_in_cell <- function(scorers_raw) {
  m <- str_match(scorers_raw, "Shearer\\s*(\\(([^)]*)\\))?")
  if (is.na(m[1, 1])) return(list(goals = 0L, penalties = 0L))

  detail <- m[1, 3]
  if (is.na(detail)) return(list(goals = 1L, penalties = 0L))

  n_goals <- str_extract(detail, "^\\d+")
  n_goals <- if (is.na(n_goals)) 1L else as.integer(n_goals)
  n_pens <- str_extract(detail, "\\d+(?=\\s*pen)")
  n_pens <- if (is.na(n_pens)) as.integer(str_detect(detail, "pen")) else as.integer(n_pens)

  list(goals = n_goals, penalties = n_pens)
}

#' Parse a `{{football box collapsible}}` run into the same
#' date / opponent / venue / result / scorers_raw shape the wikitable parser
#' returns. Each template is a flat list of `|name = value` lines; only
#' date, team1, team2, score and goals1/goals2 are needed.
#'
#' `club` is the canonical name of the club whose article this is, used to
#' decide which side of each fixture is "us" -- the templates are written
#' home-team-first regardless of who the article is about.
#'
#' @param section A wikitext section (e.g. from `extract_pl_section()`).
#' @param club Canonical club name (as used in config.yaml's `club_name_lookup`).
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble.
#' @export
parse_football_boxes <- function(section, club, cfg) {
  boxes <- str_split(section, fixed("{{football box collapsible"))[[1]][-1]

  map(boxes, function(box) {
    field <- function(name) {
      v <- str_match(box, paste0("\\|\\s*", name, "\\s*=\\s*([^\\n]*)"))[, 2]
      if (is.na(v)) "" else str_trim(v)
    }
    team1 <- clean_wikilinks(field("team1"))
    team2 <- clean_wikilinks(field("team2"))
    is_home <- normalize_club_name(team1, cfg) == club

    tibble(
      date = field("date"),
      opponent = if (is_home) team2 else team1,
      venue = if (is_home) "H" else "A",
      result = field("score"),
      scorers_raw = if (is_home) field("goals1") else field("goals2")
    )
  }) |> list_rbind()
}

#' Count Shearer's goals in a {{football box}} goals list, e.g.
#' "\[\[Alan Shearer|Shearer\]\] {{goal|77||83||90}}<br>\[\[Gary Speed|Speed\]\] {{goal|40}}".
#'
#' Scorers are <br>-separated, so Shearer's own entry is isolated first. A
#' hat-trick is one template carrying several minutes, not several templates --
#' counting `{{goal` occurrences undercounts every multi-goal match. Goals are
#' therefore the template arguments that begin with a digit (a minute, possibly
#' "90+3"); the rest are annotations such as "pen." or "o.g.".
#'
#' @param goals_raw Raw `{{football box}}` goals-list text, e.g. "{{goal|77||83||90}}".
#' @return See description above.
#' @export
count_shearer_goals_in_box <- function(goals_raw) {
  entries <- str_split(goals_raw, "<br\\s*/?>")[[1]]
  mine <- entries[str_detect(entries, "Shearer")]
  if (length(mine) == 0) return(list(goals = 0L, penalties = 0L))

  args <- mine |>
    str_extract_all("\\{\\{goal\\|([^{}]*)\\}\\}") |>
    unlist() |>
    str_remove("^\\{\\{goal\\|") |>
    str_remove("\\}\\}$") |>
    str_split(fixed("|")) |>
    unlist()

  list(
    goals = sum(str_detect(args, "^\\d")),
    penalties = sum(str_detect(args, "pen"))
  )
}

#' Parse whichever of the two league-results formats this season's article
#' uses, returning one row per match with Shearer's goals counted -- or zero
#' rows for a season the article does not cover match by match, which the
#' caller declares as a coverage gap rather than filling in.
#'
#' @param wikitext Raw wikitext of a Wikipedia article (or section).
#' @param club Canonical club name (as used in config.yaml's `club_name_lookup`).
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble.
#' @export
parse_season_pl_results <- function(wikitext, club, cfg) {
  section <- extract_pl_section(wikitext)
  empty <- tibble(date = character(), opponent = character(), venue = character(),
                  result = character(), scorers_raw = character(),
                  shearer_goals = integer(), shearer_penalties = integer())
  if (is.na(section)) return(empty)

  if (str_detect(section, fixed("{{football box"))) {
    results <- parse_football_boxes(section, club, cfg)
    counts <- map(results$scorers_raw, count_shearer_goals_in_box)
  } else {
    results <- parse_results_rows(section)
    counts <- map(results$scorers_raw, count_shearer_goals_in_cell)
  }

  results |>
    mutate(
      shearer_goals = map_int(counts, "goals"),
      shearer_penalties = map_int(counts, "penalties")
    )
}

#' 1992-93 Blackburn Rovers matches -- the one season football-data.co.uk lacks.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble.
#' @export
ingest_1992_93_matches <- function(cfg) {
  page <- "1992-93 Blackburn Rovers F.C. season"
  wikitext <- wiki_fetch_wikitext(page, cfg, "1992_93_blackburn_season")
  parse_club_season_pl_results(wikitext)
}

#' Independent per-match Shearer goal count from Wikipedia club-season
#' articles, for all 14 seasons. Used to cross-validate the Transfermarkt
#' goal log's per-match totals (see docs/validation.md).
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble.
#' @export
ingest_wikipedia_goal_crosscheck <- function(cfg) {
  seasons <- cfg$sources$wikipedia_season_results$seasons

  map(seasons, function(season_label) {
    club <- club_for_season(season_label, cfg)
    page <- glue("{season_label} {club} F.C. season")
    slug <- glue("season_{str_replace_all(season_label, '[^0-9]', '_')}_{str_replace_all(club, ' ', '_')}")
    wikitext <- wiki_fetch_wikitext(page, cfg, slug)

    parse_season_pl_results(wikitext, club, cfg) |>
      mutate(
        season = normalize_season_label(season_label),
        club = club,
        opponent = normalize_club_name(opponent, cfg),
        match_id = paste(season, venue, opponent, sep = "|")
      )
  }) |> list_rbind()
}

#' The all-time 100+ Premier League goals table -- historical comparators.
#'
#' Unlike the season-results tables (one row per "|-", cells separated by
#' "||" on the same line), this table puts one cell per line, each starting
#' with a single "|". A few cells also prefix their content with a
#' `style="..."|` or `data-sort-value="..."|` attribute before the actual
#' pipe-delimited value -- stripped by `strip_cell_attrs()` below.
#'
#' Rank and Goals are both tied via `rowspan=N` when two players share a
#' value, and -- unlike the per-season tables -- they aren't always tied
#' together: a rank tie alone drops just the rank cell, but a goals tie
#' (two players on the exact same career total) drops rank *and* goals, so
#' the row is player/appearances/ratio/first-last/club only. Detect the
#' shape from which cells parse as integers rather than assuming a fixed
#' position for goals.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble.
#' @export
ingest_all_time_scorers <- function(cfg) {
  page <- cfg$sources$wikipedia_all_time_scorers$page
  wikitext <- wiki_fetch_wikitext(page, cfg, "all_time_scorers")

  table_text <- str_extract(wikitext, "(?s)\\{\\|class=\"wikitable sortable.*?\\n\\|\\}")
  rows <- str_split(table_text, "\\n\\|-")[[1]][-1]
  last_goals <- NA_integer_

  map(rows, function(row) {
    cells <- split_wikitable_cells(row)
    if (length(cells) == 0) return(tibble())
    if (wiki_cell_is_int(cells[[1]])) cells <- cells[-1]  # drop the tied-rank cell, unused here
    if (length(cells) < 2) return(tibble())

    if (length(cells) >= 3 && wiki_cell_is_int(cells[[3]])) {
      last_goals <<- wiki_cell_as_int(cells[[2]])
      appearances <- wiki_cell_as_int(cells[[3]])
      span <- parse_career_span(cells, 5)
    } else {
      appearances <- wiki_cell_as_int(cells[[2]])
      span <- parse_career_span(cells, 4)
    }

    tibble(player = clean_wiki_markup(cells[[1]]), goals = last_goals,
           appearances = appearances,
           first_year = span[[1]], last_year = span[[2]])
  }) |> list_rbind() |>
    filter(!is.na(goals))
}

#' The First/Last Premier League year pair, written as one cell holding two
#' values joined by "||" ("1992||2006") because the table's other cells are
#' one-per-line. `idx` differs by one depending on whether the row carried its
#' own goals cell or inherited it via rowspan, so the caller passes it in.
#' Returns NA/NA rather than erroring -- a comparator with no parseable span is
#' excluded from the era adjustment, not guessed at (see R/10).
#'
#' @param cells Character vector of a wikitable row's pipe-delimited cells.
#' @param idx Index of the goals cell within `cells` (varies with rowspan continuation).
#' @return A tibble.
#' @export
parse_career_span <- function(cells, idx) {
  if (length(cells) < idx) return(list(NA_integer_, NA_integer_))
  years <- as.integer(str_extract_all(cells[[idx]], "\\d{4}")[[1]])
  if (length(years) < 2) return(list(NA_integer_, NA_integer_))
  list(years[[1]], years[[2]])
}

#' Drop leading wikitable cell attributes (`style="..."`, `align=center`,
#' `rowspan=3`, `data-sort-value="..."`, in any combination) and the "|"
#' that separates them from the cell's actual content.
#'
#' The leading `\\s*` matters: most rows are written `|style="..."|value` but
#' some use `| style="..." |value` with padding spaces (e.g. Erling Haaland's
#' row). Without it those cells keep their raw attribute string and surface as
#' a mangled player name.
#'
#' @param cell A single wikitable cell's raw text.
#' @return See description above.
#' @export
strip_cell_attrs <- function(cell) {
  str_remove(cell, '^\\s*(?:[\\w-]+=(?:"[^"]*"|\\S+)\\s*)*\\|')
}

#' Split a "|-"-delimited wikitable row into its one-cell-per-line cells.
#' Some rows write a cell with no attributes as "||value" (an empty
#' attribute string before the real cell-start "|") instead of "|value" --
#' collapsing that quirk to a single "|" first means every row's first real
#' delimiter lines up, so the leading empty piece dropped by `[-1]` is
#' always actually empty, never a swallowed cell.
#'
#' @param row A single `{{football box collapsible}}` template's lines.
#' @return See description above.
#' @export
split_wikitable_cells <- function(row) {
  row |>
    str_replace_all("\\n\\|\\|", "\n|") |>
    str_split("\\n\\|") |>
    (\(x) x[[1]][-1])() |>
    map_chr(strip_cell_attrs)
}

#' Parse a wikitable cell as an integer, stripping the bold markup
#' ('''260''') numeric cells sometimes carry. NA for anything else (a
#' player name, wikilink, or template) -- used to tell a numeric cell
#' (rank/goals/appearances) apart from a text one without assuming a fixed
#' column position, since rowspan continuation rows omit columns
#' inconsistently across these tables.
#'
#' @param x A wikitable cell's raw text.
#' @return See description above.
#' @export
wiki_cell_as_int <- function(x) suppressWarnings(as.integer(str_trim(str_remove_all(x, "'{2,}"))))
wiki_cell_is_int <- function(x) !is.na(wiki_cell_as_int(x))

#' Strip the wiki markup this table uses around player names -- {{flagicon}}
#' and {{efn}} templates, italics for still-active players -- then resolve
#' the remaining wikilink to display text.
#'
#' @param x Text containing wiki markup ({{templates}}, \[\[wikilinks\]\]).
#' @return See description above.
#' @export
clean_wiki_markup <- function(x) {
  x |>
    str_remove_all("\\{\\{flag\\s?icon(\\|[^{}]*)?\\}\\}") |>
    str_remove_all("(?s)\\{\\{efn\\|.*?\\}\\}") |>
    str_remove_all("'{2,}") |>
    clean_wikilinks() |>
    str_trim()
}

#' Shearer's own Wikipedia infobox "Career statistics" table gives *league*
#' appearances per season (cited there to premierleague.com) -- real context
#' for why some seasons' goal tallies are low (fewer games played that
#' season, not a scoring slump). Its League Goals column is also an
#' independent cross-check against season_summary()'s Transfermarkt-derived
#' totals.
#'
#' Row shape: a "|-"-delimited row is season-link / division / "||"-joined
#' numeric cells (League Apps, League Goals, then other competitions). The
#' first row of each club also carries a `rowspan=N` cell naming the club,
#' dropped here since for_club is picked up from club_for_season() instead.
#' "Total" and "Career total" rows use "!" cells, not "|", and are skipped.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble.
#' @export
ingest_shearer_appearances <- function(cfg) {
  wikitext <- wiki_fetch_wikitext(cfg$project$player_name, cfg, "career_statistics")
  section <- str_extract(wikitext, "(?s)===Club===.*?\\n\\|\\}")
  rows <- str_split(section, "\\n\\|-")[[1]][-1]

  map(rows, parse_career_stats_row) |>
    list_rbind() |>
    filter(season %in% career_season_labels(cfg)) |>
    mutate(for_club = club_for_season(season, cfg))
}

#' Parse one "|-"-delimited row of the career-statistics table into
#' season/appearances/goals, or an empty tibble for a row that isn't a
#' Premier League season row (club-total rows, career-total row, or a
#' non-Premier-League division such as Southampton's First Division years).
#'
#' @param row A single `{{football box collapsible}}` template's lines.
#' @return A tibble.
#' @export
parse_career_stats_row <- function(row) {
  cells <- str_split(str_remove(row, "^\\n"), "\\n\\|")[[1]]
  cells[[1]] <- str_remove(cells[[1]], "^\\|")
  if (str_starts(cells[[1]], "!")) return(tibble())  # Total / Career total row
  if (str_detect(cells[[1]], "^rowspan=")) cells <- cells[-1]  # club-name cell
  if (length(cells) != 3) return(tibble())

  season_label <- str_match(cells[[1]], "\\|([0-9].*)\\]\\]$")[, 2]
  if (is.na(season_label)) season_label <- cells[[1]]
  division <- str_remove_all(cells[[2]], "\\[\\[|\\]\\]")
  if (!str_detect(division, "Premier League")) return(tibble())

  nums <- str_split(cells[[3]], "\\|\\|")[[1]]
  tibble(season = normalize_season_label(season_label),
         appearances = as.integer(nums[[1]]), goals = as.integer(nums[[2]]))
}

#' Per-season top-10 scorer table -- the real competitive field each season,
#' used for contemporary comparators. Not a curated shortlist of players.
#'
#' Same one-cell-per-line style as `ingest_all_time_scorers()`, plus
#' `rowspan=N` continuation rows for tied ranks -- but Wikipedia is not
#' consistent about *what* gets carried by rowspan: some seasons repeat each
#' tied player's goal tally on its own row (continuation row = 3 cells:
#' player/club/goals, only rank carried), others also rowspan the goals
#' column itself (continuation row = 2 cells: player/club, both rank and
#' goals carried). Rather than assume one shape, classify each cell as
#' numeric (rank or goals) or text (player or club) and infer the shape from
#' that, carrying forward whichever of rank/goals a row omits.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble.
#' @export
ingest_season_top_scorers <- function(cfg) {
  seasons <- cfg$sources$wikipedia_season_results$seasons

  map(seasons, function(season_label) {
    page <- glue("{season_label} FA Premier League")
    slug <- glue("top_scorers_{str_replace_all(season_label, '[^0-9]', '_')}")
    wikitext <- wiki_fetch_wikitext(page, cfg, slug)

    section <- str_extract(wikitext, "(?s)==Top scorers==.*?\\{\\|.*?\\n\\|\\}")
    if (is.na(section)) {
      warning(glue("No 'Top scorers' table found for {season_label}"))
      return(tibble())
    }
    section <- str_remove(section, "\\n\\|\\}\\s*$")  # strip the table-close marker

    rows <- str_split(section, "\\n\\|-")[[1]][-1]
    last_rank <- NA_integer_
    last_goals <- NA_integer_

    map(rows, function(row) {
      cells <- split_wikitable_cells(row)
      if (length(cells) == 0) return(tibble())

      is_numeric <- wiki_cell_is_int(cells)

      if (is_numeric[[1]]) {
        last_rank <<- wiki_cell_as_int(cells[[1]])
        cells <- cells[-1]
        is_numeric <- is_numeric[-1]
      }
      if (length(cells) > 0 && is_numeric[[length(cells)]]) {
        last_goals <<- wiki_cell_as_int(cells[[length(cells)]])
        cells <- cells[-length(cells)]
      }
      if (length(cells) != 2) return(tibble())

      tibble(
        season = normalize_season_label(season_label), rank = last_rank,
        player = clean_wiki_markup(cells[[1]]),
        club = str_trim(cells[[2]]),
        goals = last_goals
      )
    }) |> list_rbind()
  }) |> list_rbind()
}
