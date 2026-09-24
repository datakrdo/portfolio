# Turns FBref's match logs + match reports into shearer_goal_events: one row
# per PL goal, joined to `matches`, with score_before/after derived from the
# report's running score. FBref is the primary source (Phase 5, 2026-09) --
# it is the only source with real per-match minutes, so the goals/90 metric
# in R/09/R/10 can be computed at all. Transfermarkt is kept as a left-join
# source of `goal_type` only (FBref doesn't distinguish header/free
# kick/etc) and as an independent cross-check in R/07_validate.R.

#' "92/93" -> "1992-93", matching the season labels used by `matches`.
#'
#' @param season A season label, e.g. "1996-97".
#' @return See description above.
#' @export
tm_season_to_label <- function(season) {
  start <- as.integer(str_sub(season, 1, 2))
  end <- str_sub(season, 4, 5)
  century <- if_else(start >= 92, 1900L, 2000L)
  paste0(century + start, "\u2013", end)
}

#' "1992-1993" (FBref's URL/`year_id` season spelling) -> "1992-93", matching
#' the season labels used by `matches`.
#'
#' @param season An FBref season label, e.g. "1996-1997".
#' @return See description above.
#' @export
fbref_season_to_label <- function(season) {
  paste0(str_sub(season, 1, 4), "\u2013", str_sub(season, -2))
}

#' Parse minute strings ("5'", "90'+3", "60\u2019", "60\u2019+3") into an
#' integer minute (stoppage time folded into the preceding minute, e.g.
#' 90+3 -> 93). Shared by the Transfermarkt (ASCII apostrophe) and FBref
#' (right single quote) parsers -- both use the same "base+stoppage" shape.
#'
#' @param minute_raw A raw minute string, e.g. "90'+3" or "60\u2019".
#' @return See description above.
#' @export
parse_minute <- function(minute_raw) {
  base <- as.integer(str_extract(minute_raw, "^\\d+"))
  stoppage <- as.integer(str_extract(minute_raw, "(?<=\\+)\\d+"))
  base + coalesce(stoppage, 0L)
}

#' Shared FBref scoring step: normalizes club names, parses the minute, and
#' derives `score_before_for`/`score_before_against` from the report's
#' running score (literal home:away, converted to for/against via `venue`).
#' Used by both `build_goal_events()` (Shearer, + Transfermarkt/matches joins)
#' and `build_comparator_goal_events()` (Phase 7 historical comparators, no
#' joins).
#'
#' @param fbref_goal_events Output of `ingest_fbref_goal_events()`.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble, one row per goal (own goals excluded).
#' @export
fbref_goal_events_scored <- function(fbref_goal_events, cfg) {
  fbref_goal_events |>
    filter(!is_own_goal) |>
    mutate(
      season = fbref_season_to_label(season_raw),
      opponent = normalize_club_name(opponent, cfg),
      for_club = normalize_club_name(for_club, cfg),
      minute = parse_minute(minute_raw),
      # FBref's match log spells venue out ("Home"/"Away"); matches' match_id
      # (R/05_build_matches.R) uses the "H"/"A" code -- normalize before join.
      venue = if_else(venue == "Home", "H", "A"),
      score_after_home = as.integer(str_extract(score_at, "^\\d+")),
      score_after_away = as.integer(str_extract(score_at, "\\d+$")),
      score_after_for = if_else(venue == "H", score_after_home, score_after_away),
      score_after_against = if_else(venue == "H", score_after_away, score_after_home),
      score_before_for = score_after_for - 1L,
      score_before_against = score_after_against
    )
}

#' Build shearer_goal_events from FBref's match logs + match reports, then
#' left-joins Transfermarkt's goal log for `goal_type` only (by season,
#' opponent, minute -- a goal not matched there is reported as "unknown",
#' not guessed at). Joins to `matches` via (season, venue, opponent), the
#' same key `matches$match_id` is built from.
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param matches The `matches` tibble (one row per fixture).
#' @param fbref_goal_events Output of `ingest_fbref_goal_events()` (a `_targets.R`
#'   target of its own, so the ~185 match-report fetches are cached independently
#'   of this function).
#' @return A tibble.
#' @export
build_goal_events <- function(cfg, matches, fbref_goal_events) {
  taxonomy <- cfg$goal_type_taxonomy

  tm_goal_types <- ingest_transfermarkt_goal_log(cfg) |>
    filter(str_to_lower(comp) == "premier league") |>
    transmute(
      season = tm_season_to_label(season),
      opponent = normalize_club_name(opponent, cfg),
      minute = parse_minute(minute_raw),
      goal_type
    ) |>
    distinct(season, opponent, minute, .keep_all = TRUE)

  goals <- fbref_goal_events_scored(fbref_goal_events, cfg) |>
    mutate(match_id = paste(season, venue, opponent, sep = "|")) |>
    left_join(tm_goal_types, by = c("season", "opponent", "minute")) |>
    mutate(goal_type_group = classify_goal_type_or_unknown(goal_type, taxonomy))

  goals |>
    left_join(matches |> select(-season), by = "match_id") |>
    select(-goal_type)
}

#' Lighter version of `build_goal_events()` for the historical comparators
#' (Phase 7): only `score_before_for`/`score_before_against` and `minute` are
#' needed to feed `classify_goal_context()` -- no Transfermarkt `goal_type`
#' join (not fetched for comparators) and no `matches` join (comparators have
#' no football-data.co.uk fixture list, so no `home_goals`/`away_goals`;
#' `classify_goal_context()` tolerates their absence).
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param fbref_goal_events Output of `ingest_fbref_goal_events()` for one comparator.
#' @return A tibble, one row per goal.
#' @export
build_comparator_goal_events <- function(cfg, fbref_goal_events) {
  fbref_goal_events_scored(fbref_goal_events, cfg)
}

#' Map a free-text "Type of goal" value to the vocabulary in
#' config.yaml's goal_type_taxonomy. Anything not listed there is an error,
#' not silently dropped -- a new Transfermarkt label must be classified, not
#' guessed at.
#'
#' @param goal_type Free-text "Type of goal" value from the Transfermarkt log.
#' @param taxonomy The `goal_type_taxonomy` list from config.yaml.
#' @return See description above.
#' @export
classify_goal_type <- function(goal_type, taxonomy) {
  group_for <- function(x) {
    hit <- names(taxonomy)[map_lgl(taxonomy, ~ x %in% .x)]
    if (length(hit) == 0) stop(glue::glue("Unclassified goal type: '{x}' -- add it to config.yaml's goal_type_taxonomy"))
    hit[[1]]
  }
  vapply(goal_type, group_for, character(1))
}

#' `classify_goal_type()`, but for a goal that has no Transfermarkt match at
#' all (NA `goal_type`, e.g. no `(season, opponent, minute)` hit) -- reported
#' as "unknown" instead of erroring, since a missing cross-check row is a
#' declared coverage gap, not an unclassified value.
#'
#' @param goal_type Free-text "Type of goal" value, or NA if unmatched.
#' @param taxonomy The `goal_type_taxonomy` list from config.yaml.
#' @return See description above.
#' @export
classify_goal_type_or_unknown <- function(goal_type, taxonomy) {
  out <- rep("unknown", length(goal_type))
  known <- !is.na(goal_type)
  out[known] <- classify_goal_type(goal_type[known], taxonomy)
  out
}
