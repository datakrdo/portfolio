# Builds one `matches` table for Shearer's two clubs, 1992-93 through
# 2005-06, from football-data.co.uk (1993-94 onward) plus Wikipedia
# (1992-93, the one season football-data lacks). Every row gets a stable
# match_id so shearer_goal_events (R/06) can join to it without re-deriving
# dates.

#' Flatten config.yaml's canonical -> aliases table into an alias -> canonical
#' named vector, so normalization is a single hash lookup rather than a scan
#' over every canonical name's alias vector.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return See description above.
#' @export
club_alias_lookup <- function(cfg) {
  lookup <- cfg$club_name_lookup
  setNames(rep(names(lookup), lengths(lookup)), unlist(lookup, use.names = FALSE))
}

#' Map any source's club-name spelling to the canonical form in
#' config.yaml's club_name_lookup. Vectorised over `name`; names absent from
#' the lookup are assumed identical across sources and returned unchanged.
#'
#' @param name A club name as spelled by some source.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return See description above.
#' @export
normalize_club_name <- function(name, cfg) {
  coalesce(unname(club_alias_lookup(cfg)[name]), name)
}

#' Combine football-data.co.uk (1993-94..2005-06) and the 1992-93 Wikipedia
#' results into one long table of every Blackburn/Newcastle Premier League
#' match, with a reproducible match_id (season + venue + opponent -- unique
#' because each PL season has exactly one home and one away fixture per
#' opponent pair).
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble.
#' @export
build_matches <- function(cfg) {
  # football-data.co.uk is ingested well past 2006 so R/10 can measure the
  # league-wide scoring environment across every comparator's career. `matches`
  # is only ever Shearer's own career, and club_for_season() labels *any*
  # non-Blackburn season "Newcastle United" -- without this filter every
  # post-2006 Newcastle match would be pulled in and break the sec. 14 gate.
  career_seasons <- career_season_labels(cfg)

  fd <- ingest_footballdata_results(cfg) |>
    mutate(season = football_data_season_label(season_code)) |>
    filter(season %in% career_seasons) |>
    mutate(
      home_team = normalize_club_name(HomeTeam, cfg),
      away_team = normalize_club_name(AwayTeam, cfg),
      shearer_club = club_for_season(season, cfg)
    ) |>
    filter(home_team == shearer_club | away_team == shearer_club) |>
    transmute(
      season, date = parse_footballdata_date(Date),
      home_team, away_team, home_goals = FTHG, away_goals = FTAG
    )

  wiki_9293 <- ingest_1992_93_matches(cfg) |>
    mutate(
      club = "Blackburn Rovers",
      opponent = normalize_club_name(opponent, cfg),
      is_home = venue == "H",
      goals_for = as.integer(str_extract(result, "^\\d+")),
      goals_against = as.integer(str_extract(result, "\\d+$"))
    ) |>
    transmute(
      season = "1992\u201393",
      date = parse_wiki_date(date),
      home_team = if_else(is_home, club, opponent),
      away_team = if_else(is_home, opponent, club),
      home_goals = if_else(is_home, goals_for, goals_against),
      away_goals = if_else(is_home, goals_against, goals_for)
    )

  bind_rows(fd, wiki_9293) |>
    mutate(
      shearer_club = club_for_season(season, cfg),
      is_home = home_team == shearer_club,
      match_id = paste(season, if_else(is_home, "H", "A"),
                        if_else(is_home, away_team, home_team), sep = "|")
    ) |>
    select(-shearer_club, -is_home) |>
    arrange(date)
}

#' football-data.co.uk switches date format partway through the archive --
#' "17/08/2002" (4-digit year) in some seasons, "13/08/05" (2-digit) in
#' others, within the same column. Dispatch on the year field's actual
#' digit count rather than trying "%Y" first: R's strptime accepts a 2-digit
#' string for "%Y" too (e.g. "01" -> year 1), so a blind try-then-coalesce
#' silently keeps the wrong parse instead of falling back.
#'
#' @param x See description above.
#' @return A tibble.
#' @export
parse_footballdata_date <- function(x) {
  is_four_digit <- str_detect(x, "^\\d{2}/\\d{2}/\\d{4}$")
  if_else(is_four_digit, as.Date(x, format = "%d/%m/%Y"), as.Date(x, format = "%d/%m/%y"))
}

#' Parse a Wikipedia date like "15 August 1992" without depending on the
#' system's LC_TIME locale (%B needs English month names; on a non-English
#' locale -- e.g. es_AR.UTF-8 -- every date silently parses to NA).
#'
#' @param x See description above.
#' @return A tibble.
#' @export
parse_wiki_date <- function(x) {
  months <- c("January", "February", "March", "April", "May", "June", "July",
              "August", "September", "October", "November", "December")
  m <- str_match(x, "^(\\d{1,2}) (\\w+) (\\d{4})$")
  as.Date(paste(m[, 4], match(m[, 3], months), m[, 2], sep = "-"), format = "%Y-%m-%d")
}

#' football-data.co.uk season codes ("9394") to season labels ("1993-94").
#'
#' @param code A football-data.co.uk season code, e.g. "9394".
#' @return See description above.
#' @export
football_data_season_label <- function(code) {
  start <- as.integer(str_sub(code, 1, 2))
  end <- str_sub(code, 3, 4)
  century <- if_else(start >= 92, 1900L, 2000L)
  paste0(century + start, "\u2013", end)
}
