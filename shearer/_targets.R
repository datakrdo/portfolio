# {targets} pipeline — replaces make.R. Same flow (ingest -> build ->
# validate -> analyse -> figures -> reports -> parquet for the Shiny app),
# but each step is now a cached target: touch R/11_figures.R and re-run
# tar_make(), and only the figure/report targets downstream of it rerun —
# every ingest and Wikipedia-parsing target (the slow part) is skipped.
#
# The Shiny app's contract is unchanged: it still reads only
# data/processed/*.parquet, which are declared here as explicit file targets.

library(targets)

tar_option_set(
  packages = c("shearer", "dplyr", "arrow", "ggplot2", "gt", "ggsoccer", "glue", "scales",
               "knitr", "purrr", "stringr", "tibble", "tidyr", "pointblank",
               "httr2", "fs", "readr", "jsonlite", "rvest", "xml2"),
  format = "rds"
)

devtools::load_all(quiet = TRUE)

# Writes `obj` to `path` via `writer` and returns `path` — the one shared body
# for every parquet/figure/report file target below, so each of those targets
# is a one-liner and `format = "file"` always sees the thing it's tracking.
write_target <- function(obj, path, writer) {
  dir_create(path_dir(path))
  writer(obj, path)
  path
}

list(
  tar_target(cfg, load_config()),

  # --- ingest, build, validate ----------------------------------------------
  tar_target(matches, build_matches(cfg)),

  # FBref is the primary source of goal_events as of Phase 5 — fetched via
  # the Internet Archive (fbref.com itself returns a Cloudflare challenge,
  # see THIRD_PARTY_NOTICES.md). Each is its own target so the ~200 network
  # fetches (14 match logs + ~185 match reports + player pages) are cached
  # independently and rerun only when their own inputs change.
  tar_target(fbref_match_logs, ingest_fbref_match_logs(cfg) |> dplyr::filter(comp == "Premier League")),
  tar_target(fbref_goal_events, ingest_fbref_goal_events(cfg, fbref_match_logs)),
  tar_target(fbref_player_ids_map, fbref_player_ids(cfg)),
  tar_target(fbref_player_seasons, ingest_fbref_player_seasons(cfg, fbref_player_ids_map)),

  tar_target(goal_events_raw, build_goal_events(cfg, matches, fbref_goal_events)),

  # Independent per-match reading of the same career, from Wikipedia's
  # club-season articles — no shared fetch, parser or intermediate table with
  # FBref or Transfermarkt. Computed before the gate so the gate's report can include it.
  tar_target(wiki_crosscheck_raw, ingest_wikipedia_goal_crosscheck(cfg)),
  tar_target(wiki_crosscheck, crosscheck_goal_counts(wiki_crosscheck_raw, goal_events_raw)),

  # The §14 gate. Returns the pointblank agent; every downstream target names
  # it (even unused) so targets' static analysis makes them depend on it —
  # a validation failure here stops the whole rest of the pipeline.
  tar_target(span_mismatches, historical_table_span_mismatches(all_time_scorers, fbref_player_seasons)),
  tar_target(validation_agent, validate_goal_events(goal_events_raw, cfg, wiki_crosscheck, span_mismatches)),
  tar_target(goal_events, { force(validation_agent); classify_goal_context(goal_events_raw, cfg) }),
  tar_target(goal_events_checked, { stopifnot_validated_goal_events(goal_events, cfg); goal_events }),

  # --- analysis ---------------------------------------------------------
  tar_target(season_summary_tbl, season_summary(goal_events_checked, fbref_player_seasons, cfg)),
  tar_target(goals_by_opponent_tbl, goals_by_opponent(goal_events_checked)),
  tar_target(goal_type_breakdown_tbl, goal_type_breakdown(goal_events_checked)),
  tar_target(context_breakdown_tbl, context_breakdown(goal_events_checked)),
  tar_target(penalty_summary_tbl, penalty_summary(goal_events_checked)),

  # --- Phase 7: clutch-context comparison across the 10 comparators --------
  # The 9 non-Shearer comparators' own ~1,400 match-report fetches, entirely
  # separate from fbref_match_logs/fbref_goal_events above -- Shearer's
  # 260-goal gate never touches this branch. Each comparator's PL seasons come
  # from fbref_player_seasons, not hardcoded. cache_fetch_wayback() skips
  # files already on disk, so re-running this target after a partial failure
  # only fetches what's still missing.
  tar_target(
    comparator_context_breakdowns,
    lapply(setdiff(names(fbref_player_ids_map), cfg$project$player_name), function(player) {
      comparator_context_breakdown_for(player, fbref_player_ids_map[[player]], cfg, fbref_player_seasons)
    })
  ),
  tar_target(
    comparator_context_breakdown_tbl,
    dplyr::bind_rows(
      context_breakdown_tbl |> dplyr::mutate(player = cfg$project$player_name, .before = 1),
      dplyr::bind_rows(comparator_context_breakdowns)
    )
  ),
  tar_target(
    comparator_coverage_gaps,
    unlist(lapply(comparator_context_breakdowns, attr, which = "coverage_gaps"), use.names = FALSE)
  ),

  tar_target(top_scorers, ingest_season_top_scorers(cfg)),
  tar_target(contemporary_summary_tbl, contemporary_summary(top_scorers, cfg)),
  tar_target(contemporary_overview_tbl, contemporary_overview(contemporary_summary_tbl)),

  tar_target(footballdata_results, ingest_footballdata_results(cfg)),
  tar_target(league_scoring_trend_tbl, league_scoring_trend(footballdata_results)),

  # Season count, appearance total and per-season goals, all against
  # Shearer's own Wikipedia infobox — independent of the Transfermarkt goal
  # log. validate_appearances() returns its input, so this is both the gate
  # and the table in one target.
  tar_target(
    shearer_appearances_tbl,
    validate_appearances(ingest_shearer_appearances(cfg), season_summary_tbl, cfg)
  ),

  tar_target(all_time_scorers, ingest_all_time_scorers(cfg)),
  tar_target(historical_table_tbl, historical_table(all_time_scorers, cfg, league_scoring_trend_tbl, fbref_player_seasons)),
  tar_target(
    season_era_tbl,
    season_era_index(season_summary_tbl, shearer_appearances_tbl, league_scoring_trend_tbl)
  ),
  tar_target(shots, ingest_statsbomb_shearer_shots(cfg)),

  # --- figures ------------------------------------------------------------
  tar_target(
    fig_trajectory_png,
    write_target(fig_trajectory(season_summary_tbl, cfg), "output/figures/01_trajectory.png",
                 \(p, path) ggsave(path, p, width = 9, height = 5.5, dpi = 150)),
    format = "file"
  ),
  tar_target(
    fig_goal_types_png,
    write_target(fig_goal_types(goal_type_breakdown_tbl), "output/figures/04_goal_types.png",
                 \(p, path) ggsave(path, p, width = 8, height = 4, dpi = 150)),
    format = "file"
  ),
  tar_target(
    fig_context_png,
    write_target(fig_context(context_breakdown_tbl), "output/figures/05_context.png",
                 \(p, path) ggsave(path, p, width = 8, height = 4.5, dpi = 150)),
    format = "file"
  ),
  tar_target(
    fig_comparators_png,
    write_target(fig_comparators(historical_table_tbl, cfg), "output/figures/06_comparators.png",
                 \(p, path) ggsave(path, p, width = 10, height = 8.5, dpi = 150)),
    format = "file"
  ),
  tar_target(
    fig_statsbomb_shots_png,
    write_target(fig_statsbomb_shots(shots), "output/figures/07_statsbomb_shots.png",
                 \(p, path) ggsave(path, p, width = 8, height = 5.5, dpi = 150)),
    format = "file"
  ),

  # --- reports --------------------------------------------------------------
  tar_target(
    coverage_report_md,
    write_coverage_report(goal_type_breakdown_tbl, contemporary_overview_tbl,
                           file.path(cfg$paths$docs, "coverage.md"),
                           comparator_coverage_gaps = comparator_coverage_gaps),
    format = "file"
  ),
  tar_target(
    data_dictionary_md,
    write_data_dictionary(list(matches = matches, shearer_goal_events = goal_events_checked),
                           file.path(cfg$paths$docs, "data_dictionary.md")),
    format = "file"
  ),
  tar_target(
    methodology_md,
    write_methodology_report(cfg, file.path(cfg$paths$docs, "methodology.md")),
    format = "file"
  ),
  tar_target(
    limitations_md,
    write_limitations_report(cfg, file.path(cfg$paths$docs, "limitations.md")),
    format = "file"
  ),

  # --- parquet for the Shiny app --------------------------------------------
  # FBref (via the Internet Archive) is the primary per-goal source as of
  # Phase 5; goal_type alone still comes from Transfermarkt (see R/06) —
  # recorded as metadata for the app's event table, not derived from anything else.
  tar_target(goal_events_for_app, goal_events_checked |> mutate(source = "FBref", .after = match_id)),

  tar_target(matches_parquet, write_target(matches, "data/processed/matches.parquet", write_parquet), format = "file"),
  tar_target(goal_events_parquet, write_target(goal_events_for_app, "data/processed/shearer_goal_events.parquet", write_parquet), format = "file"),
  tar_target(season_summary_parquet, write_target(season_summary_tbl, "data/processed/season_summary.parquet", write_parquet), format = "file"),
  tar_target(goals_by_opponent_parquet, write_target(goals_by_opponent_tbl, "data/processed/goals_by_opponent.parquet", write_parquet), format = "file"),
  tar_target(goal_type_breakdown_parquet, write_target(goal_type_breakdown_tbl, "data/processed/goal_type_breakdown.parquet", write_parquet), format = "file"),
  tar_target(context_breakdown_parquet, write_target(context_breakdown_tbl, "data/processed/context_breakdown.parquet", write_parquet), format = "file"),
  tar_target(comparator_context_breakdown_parquet, write_target(comparator_context_breakdown_tbl, "data/processed/comparator_context_breakdown.parquet", write_parquet), format = "file"),
  tar_target(penalty_summary_parquet, write_target(penalty_summary_tbl, "data/processed/penalty_summary.parquet", write_parquet), format = "file"),
  tar_target(contemporary_summary_parquet, write_target(contemporary_summary_tbl, "data/processed/contemporary_summary.parquet", write_parquet), format = "file"),
  tar_target(contemporary_overview_parquet, write_target(contemporary_overview_tbl, "data/processed/contemporary_overview.parquet", write_parquet), format = "file"),
  tar_target(historical_table_parquet, write_target(historical_table_tbl, "data/processed/historical_table.parquet", write_parquet), format = "file"),
  tar_target(shots_parquet, write_target(shots, "data/processed/statsbomb_shots.parquet", write_parquet), format = "file"),
  tar_target(league_scoring_trend_parquet, write_target(league_scoring_trend_tbl, "data/processed/league_scoring_trend.parquet", write_parquet), format = "file"),
  tar_target(shearer_appearances_parquet, write_target(shearer_appearances_tbl, "data/processed/shearer_appearances.parquet", write_parquet), format = "file"),
  tar_target(season_era_parquet, write_target(season_era_tbl, "data/processed/season_era_index.parquet", write_parquet), format = "file"),
  tar_target(wiki_crosscheck_parquet, write_target(wiki_crosscheck, "data/processed/wikipedia_crosscheck.parquet", write_parquet), format = "file")
)
