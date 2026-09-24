# FBref (fbref.com / Sports Reference), the primary source of shearer_goal_events
# from Phase 5 onward -- the only free source with real per-match minutes,
# assists and running score for Shearer's whole career. Every page below is
# fetched through cache_fetch_wayback() (R/01_sources.R): fbref.com itself
# returns a Cloudflare Managed Challenge on the first request (see
# THIRD_PARTY_NOTICES.md), so the Internet Archive is the only transport used.
#
# Two FBref parsing quirks, both handled here:
#   1. `html_table()` breaks on FBref's two-level column headers -- every
#      table is parsed by walking <tr>/<th|td> and keying on the `data-stat`
#      attribute instead.
#   2. Player pages hide some tables (including stats_standard_dom_lg) inside
#      an HTML comment, presumably to keep them out of naive scrapers -- these
#      have to be pulled out of `//comment()` and re-parsed as their own
#      document before the data-stat walk applies. Match logs and match
#      reports are not hidden this way.

#' Parse a `data-stat`-keyed HTML table into one row per `<tr>`, one column
#' per distinct `data-stat` value. Shared by every FBref table this file reads
#' (match logs, match reports use their own walk, player-season stats).
#'
#' @param table_node An `xml_node` for a single `<table>`.
#' @return A tibble, one row per `<tr>` in `<tbody>`.
fbref_table_rows <- function(table_node) {
  rows <- html_elements(table_node, "tbody > tr")
  # Some comparator pages add a repeated sub-header row with class
  # "over_header thead sr_added_headers" -- a word, not the whole attribute,
  # so this has to be a substring/word match rather than `%in%` exact equality.
  row_class <- coalesce(html_attr(rows, "class"), "")
  rows <- rows[!str_detect(row_class, "\\b(thead|spacer)\\b")]
  map_dfr(rows, function(r) {
    cells <- html_elements(r, "th, td")
    data_stats <- html_attr(cells, "data-stat")
    stats <- setNames(as.list(html_text2(cells)), data_stats)
    # The match-report link lives in the match_report cell's <a href>, which
    # html_text2() discards (it keeps only the link text, "Match Report").
    # `data_stats` can hold NA -- filter with `%in%`, not `==`, so an NA
    # comparison doesn't produce an NA-indexed xml_nodeset subset (xml2 errors
    # on that: "Expecting an external pointer").
    report_cell <- cells[data_stats %in% "match_report"]
    if (length(report_cell) > 0) {
      stats$report_href <- html_attr(html_element(report_cell[[1]], "a"), "href")
    }
    as_tibble(stats)
  })
}

#' A table FBref hides inside an HTML comment (player pages do this for
#' several tables, including stats_standard_dom_lg) has to be extracted as
#' comment text and re-parsed as its own document before `html_elements()`
#' can see inside it -- it is invisible to a normal query on `doc`.
#'
#' @param doc A parsed `xml_document` (from `read_html()`).
#' @param table_id The table's `id` attribute, e.g. "stats_standard_dom_lg".
#' @return An `xml_node` for the `<table>`, or `NULL` if not found (visible or hidden).
fbref_find_table <- function(doc, table_id) {
  # Newer FBref markup splices a per-page infix into the id (e.g.
  # "stats_standard_ks_dom_lg" instead of "stats_standard_dom_lg", seen on
  # Henry/Kane's pages but not Shearer's) -- match head+tail, not exact id.
  head <- "stats_standard"
  tail <- str_remove(table_id, "^stats_standard_")
  sel <- paste0("table[id^='", head, "'][id$='", tail, "']")
  visible <- html_element(doc, sel)
  if (!is.na(visible)) return(visible)

  # xml_text(), not html_text2() -- html_text2() returns "" for comment
  # nodes (it walks child *elements*, and a comment has none), which silently
  # broke this fallback entirely.
  comments <- html_elements(doc, xpath = "//comment()")
  comment_text <- xml2::xml_text(comments)
  hit <- comment_text[str_detect(comment_text, paste0("id=\"", head, "[a-z_]*", tail, "\""))]
  if (length(hit) == 0) return(NULL)

  html_element(read_html(hit[[1]]), sel)
}

#' Match logs for all PL seasons of a player's career, one row per match (PL
#' and every other competition -- callers filter to Premier League). This is
#' the backbone `build_goal_events()` (Shearer) and
#' `comparator_context_breakdown_for()` (Phase 7 historical comparators)
#' build from: season/opponent/venue/result/minutes/goals per match.
#'
#' Defaults reproduce the original Shearer-only call exactly, including the
#' cache filename (`fbref_matchlog_{season}.html`, no player infix) -- so the
#' ~14 already-cached fetches from before this function took a player
#' argument aren't invalidated. A comparator's `player_id` differs from the
#' default, so its files get a player-id infix instead, keeping the ten
#' players' caches from colliding with each other.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param player_id FBref player id. Defaults to Shearer's.
#' @param player_name Player's full name (used to build the URL slug). Defaults to Shearer's.
#' @param seasons Character vector of FBref season labels (e.g. "1996-1997") to fetch.
#'   Defaults to Shearer's full career (`cfg$sources$fbref$seasons`).
#' @return A tibble.
#' @export
ingest_fbref_match_logs <- function(cfg, player_id = cfg$sources$fbref$player_id,
                                     player_name = cfg$project$player_name,
                                     seasons = cfg$sources$fbref$seasons) {
  src <- cfg$sources$fbref
  is_primary <- identical(player_id, src$player_id)

  gaps <- character(0)

  result <- map_dfr(seasons, function(season) {
    url <- glue(src$matchlog_url_template, player_id = player_id, season = season,
                slug = str_replace_all(player_name, " ", "-"))
    summary_url <- sub("(/matchlogs/[^/]+)/", "\\1/summary/", url)
    dest <- if (is_primary) glue("{cfg$paths$raw}/fbref_matchlog_{season}.html")
            else glue("{cfg$paths$raw}/fbref_matchlog_{player_id}_{season}.html")

    fetch_ok <- tryCatch(
      {
        tryCatch(
          cache_fetch_wayback(url, dest, "fbref", cfg, notes = glue("{player_name} {season}")),
          # FBref split the once-single matchlog page into tabs (summary/passing/
          # defense/...) for newer seasons -- the base URL has no Wayback capture
          # from that point on, only .../summary/... does. Same table (id
          # "matchlogs_all"), different path. Never hit by Shearer's own seasons
          # (1992-2006, pre-redesign), so this only fires for comparators.
          error = function(e) {
            cache_fetch_wayback(summary_url, dest, "fbref", cfg, notes = glue("{player_name} {season}"))
          }
        )
        TRUE
      },
      # A small subset of comparator seasons has no Wayback capture at all under
      # either URL shape -- fall back to a live fetch through a real headless
      # Chrome (cache_fetch_chromote()), same as ingest_fbref_goal_events().
      error = function(e) {
        tryCatch(
          { cache_fetch_chromote(summary_url, dest, "fbref", cfg, notes = glue("{player_name} {season}")); TRUE },
          error = function(e2) {
            warning(glue("[fbref] no usable match log for {player_name} season {season}: {conditionMessage(e2)}"), call. = FALSE)
            gaps[[length(gaps) + 1]] <<- glue("{player_name} {season}")
            FALSE
          }
        )
      }
    )
    if (!fetch_ok) return(tibble())

    doc <- read_html(dest)
    table_node <- html_element(doc, "table#matchlogs_all")
    if (is.na(table_node)) {
      warning(glue("No matchlogs_all table for FBref player {player_name} season {season}"))
      return(tibble())
    }
    fbref_table_rows(table_node) |> mutate(season_raw = season)
  })

  attr(result, "coverage_gaps") <- gaps
  result
}

#' One event per goal, parsed from each scoring match's report page
#' (`#events_wrap div.event`). Only fetches reports for matches with
#' `goals > 0` in `logs` -- matches with no goal from `player_name` have
#' nothing a report would add. Match reports are cached by match slug, not by
#' player, so two comparators who both scored in the same historical match
#' share one fetch.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param logs Output of `ingest_fbref_match_logs()`, filtered to Premier League.
#' @param player_name Player's full name, matched against the report's scorer
#'   text. Defaults to Shearer's.
#' @return A tibble, one row per goal, with a `coverage_gaps` attribute
#'   (character vector naming any match report the Archive had no capture of).
#' @export
ingest_fbref_goal_events <- function(cfg, logs, player_name = cfg$project$player_name) {
  src <- cfg$sources$fbref
  scoring <- logs |> filter(as.integer(goals) > 0, !is.na(report_href))
  gaps <- character(0)

  result <- map_dfr(seq_len(nrow(scoring)), function(i) {
    row <- scoring[i, ]
    # report_href comes from an already-archived page, so Wayback has rewritten
    # it to "/web/{timestamp}/https://fbref.com/en/matches/..." -- pull the
    # live fbref.com URL back out rather than prefixing base_url onto that.
    # A Wayback-archived match-log page has its hrefs rewritten to
    # "/web/{timestamp}/https://fbref.com/..." -- pull the live URL back out.
    # A live-fetched page (cache_fetch_chromote(), no Wayback snapshot exists)
    # keeps FBref's own relative href ("/en/matches/..."), never touching
    # "https://fbref.com" at all, so it needs the base URL added instead.
    report_url <- coalesce(
      str_extract(row$report_href, "https://fbref\\.com/.*$"),
      glue("https://fbref.com{row$report_href}")
    )
    slug <- str_replace_all(str_remove(report_url, "^https://fbref\\.com/en/matches/"), "/", "_")
    dest <- glue("{cfg$paths$raw}/fbref_report_{slug}.html")

    # A handful of match reports 404 even when the CDX API listed a 200
    # snapshot (a stale/broken CDX entry, observed in Phase 0's probe at
    # ~10/12) -- retried against other snapshots inside cache_fetch_wayback().
    # A smaller, confirmed-permanent subset (28 matches, verified 2026-09-20 via
    # direct CDX queries with no status filter: zero snapshots at any status
    # code) has no Wayback capture at all -- those fall back to a live fetch
    # through a real headless Chrome (cache_fetch_chromote()), which clears
    # fbref.com's Cloudflare challenge where a plain HTTP request can't.
    fetch_ok <- tryCatch(
      { cache_fetch_wayback(report_url, dest, "fbref", cfg, notes = row$match_report); TRUE },
      error = function(e) {
        tryCatch(
          { cache_fetch_chromote(report_url, dest, "fbref", cfg, notes = row$match_report); TRUE },
          error = function(e2) {
            warning(glue("[fbref] no usable report for {player_name} vs {row$opponent} ({row$date}): {conditionMessage(e2)}"), call. = FALSE)
            gaps[[length(gaps) + 1]] <<- glue("{player_name} vs {row$opponent} ({row$date})")
            FALSE
          }
        )
      }
    )
    if (!fetch_ok) return(tibble())

    doc <- read_html(dest)
    events <- html_elements(doc, "#events_wrap div.event")
    if (length(events) == 0) return(tibble())

    # The event's own class is just "event"/"event a" -- the goal/penalty/own-goal
    # marker lives on a child <div class="event_icon ...">, shared with substitutions
    # ("event_icon substitute_in/out"), so match specifically on the goal classes.
    icon_classes <- map_chr(events, function(e) {
      icon <- html_elements(e, "div[class*=event_icon]")
      if (length(icon) == 0) return(NA_character_)
      html_attr(icon[[1]], "class")
    })
    is_goal <- str_detect(coalesce(icon_classes, ""), "\\b(goal|own_goal|penalty_goal)\\b")
    events <- events[is_goal]
    if (length(events) == 0) return(tibble())

    parsed <- map_dfr(events, fbref_parse_event)
    parsed |>
      filter(str_detect(player, fixed(player_name))) |>
      mutate(
        season_raw = row$season_raw, match_report_href = row$report_href,
        opponent = row$opponent, venue = row$venue, for_club = row$team
      )
  })

  attr(result, "coverage_gaps") <- gaps
  result
}

#' Parse one `div.event` node into minute/running-score/player/type. `html_text2()`
#' renders each event as one line per child div, e.g. `"66’\n2:2\nAlan Shearer\n
#' Assist: Mike Newell\n — Goal 2:2"` -- not pipe-separated, newline-separated.
#' The icon div's class carries the event kind (`event_icon goal` /
#' `event_icon penalty_goal` / `event_icon own_goal`).
#'
#' @param event_node An `xml_node` for one `div.event`.
#' @return A one-row tibble.
fbref_parse_event <- function(event_node) {
  icon_class <- html_attr(html_element(event_node, "div[class*=event_icon]"), "class")
  txt <- html_text2(event_node)
  parts <- str_trim(str_split(txt, "\n")[[1]])
  parts <- parts[parts != ""]

  minute <- parts[[1]]
  score_at <- parts[[2]]
  player <- if (length(parts) >= 3) parts[[3]] else NA_character_
  # Any line(s) between the player and the trailing status line are detail
  # (e.g. "Assist: X", "Penalty Kick") -- take the first one, if present.
  detail <- if (length(parts) >= 4) parts[[4]] else NA_character_

  tibble(
    minute_raw = minute,
    score_at = score_at,
    player = player,
    goal_type_raw = detail,
    is_penalty = str_detect(coalesce(icon_class, ""), "penalty_goal"),
    is_own_goal = str_detect(coalesce(icon_class, ""), "own_goal")
  )
}

#' Per-season Premier League stats (games, minutes, goals, goals_per90) for a
#' set of players -- Shearer plus the historical comparators, keyed by FBref
#' player id. Powers the goals/90 numerator in R/09 and R/10.
#'
#' `stats_standard_dom_lg` is a comment-hidden table on player pages (see
#' `fbref_find_table()`), filtered to `comp_level == "1. Premier League"` --
#' player pages also carry cup and other-league seasons on the same table.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param player_ids Named character vector, player name -> FBref id (see `fbref_player_ids()`).
#' @return A tibble, one row per player-season.
#' @export
ingest_fbref_player_seasons <- function(cfg, player_ids) {
  src <- cfg$sources$fbref

  map_dfr(names(player_ids), function(player) {
    id <- player_ids[[player]]
    # FBref's own URL slugs are ASCII-only (e.g. "Sergio-Aguero", not
    # "Sergio-Agüero") even though the player's name carries diacritics
    # everywhere else (config, Wikipedia). Strip them for the slug only.
    slug <- str_replace_all(iconv(player, from = "UTF-8", to = "ASCII//TRANSLIT"), " ", "-")
    url <- glue(src$player_url_template, player_id = id, slug = slug)
    dest <- glue("{cfg$paths$raw}/fbref_player_{id}.html")
    cache_fetch_wayback(url, dest, "fbref", cfg, notes = player)

    doc <- read_html(dest)
    table_node <- fbref_find_table(doc, "stats_standard_dom_lg")
    if (is.null(table_node)) {
      warning(glue("No stats_standard_dom_lg table for FBref player {player} ({id})"))
      return(tibble())
    }

    rows <- fbref_table_rows(table_node)
    # Older/newer FBref page templates label the same two columns differently
    # ("year_id"/"team" on Shearer's page vs "season"/"squad" on Henry's and
    # Kane's) -- normalize before the shared transmute below.
    if (!"year_id" %in% names(rows)) rows <- rename(rows, year_id = season)
    if (!"team" %in% names(rows)) rows <- rename(rows, team = squad)

    rows |>
      # FBref labelled the English top flight "Premiership" rather than
      # "Premier League" on some player pages for 1992-2007 seasons (seen on
      # Henry's page, not Shearer's or Kane's) -- same competition, both accepted.
      filter(comp_level %in% c("1. Premier League", "1. Premiership")) |>
      transmute(
        player = player,
        season_raw = year_id,
        team = team,
        games = as.integer(games),
        minutes = as.integer(str_remove_all(minutes, ",")),
        goals = as.integer(goals),
        assists = as.integer(assists),
        goals_per90_fbref = as.numeric(goals_per90),
        goals_assists_per90_fbref = as.numeric(goals_assists_per90)
      )
  })
}

#' FBref player ids for the historical comparators plus Shearer, pinned in
#' `config.yaml`'s `sources.fbref.comparator_player_ids` -- resolved once
#' (2026-09-20) against an archived Premier-League season-stats page, which
#' lists every that-season player's `/en/players/{id}/{name}` link. Returned
#' from config rather than re-parsed on every run: the ids don't change, and
#' re-deriving them would add a fragile network dependency for no benefit.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A named character vector, player name -> FBref id.
#' @export
fbref_player_ids <- function(cfg) {
  unlist(cfg$sources$fbref$comparator_player_ids)
}
