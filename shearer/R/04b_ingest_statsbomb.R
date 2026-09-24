# hudl/open-data (formerly statsbomb/open-data) -- the only public event data
# with x/y coordinates inside Shearer's career (PL 2003/04, Arsenal's 38
# matches only). Used for exactly one figure: what spatial data exists for
# Shearer in open data, not a shot-map analysis (see docs/limitations.md).
#
# Match IDs are discovered from the competition's matches.json rather than
# hardcoded, so a change of season_id in config.yaml doesn't silently ingest
# the wrong matches.

#' All of Shearer's shots (any outcome) found in hudl/open-data's coverage
#' of Shearer's career. Returns zero rows, not an error, if none are found --
#' the absence is the finding (see docs/methodology.md).
#'
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A tibble.
#' @export
ingest_statsbomb_shearer_shots <- function(cfg) {
  src <- cfg$sources$hudl_open_data
  matches_url <- glue("{src$base_url}/data/matches/{src$competition_id}/{src$season_id}.json")
  matches_dest <- file.path(cfg$paths$raw, glue("hudl_matches_{src$competition_id}_{src$season_id}.json"))
  matches <- cache_fetch_json(matches_url, matches_dest, "hudl_open_data", cfg, notes = "matches")

  match_ids <- map_int(matches, ~ .x$match_id)

  map(match_ids, function(mid) {
    events_url <- glue("{src$base_url}/data/events/{mid}.json")
    events_dest <- file.path(cfg$paths$raw, glue("hudl_events_{mid}.json"))
    events <- cache_fetch_json(events_url, events_dest, "hudl_open_data", cfg, notes = glue("events:{mid}"))

    shots <- Filter(function(e) {
      !is.null(e$type$name) && e$type$name == "Shot" &&
        !is.null(e$player$name) && e$player$name == cfg$project$player_name
    }, events)

    if (length(shots) == 0) return(tibble())

    map(shots, function(s) tibble(
      match_id = mid,
      minute = s$minute,
      second = s$second,
      x = s$location[[1]],
      y = s$location[[2]],
      body_part = s$shot$body_part$name %||% NA_character_,
      outcome = s$shot$outcome$name %||% NA_character_,
      xg = s$shot$statsbomb_xg %||% NA_real_
    )) |> list_rbind()
  }) |> list_rbind()
}

`%||%` <- function(x, y) if (is.null(x)) y else x
