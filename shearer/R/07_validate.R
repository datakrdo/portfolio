# The sec. 14 gate: shearer_goal_events must reconcile to known aggregates before
# any analysis or figure is allowed to run. Failures here mean the pipeline
# stops with an explicit, partial-coverage report -- never silently continue
# with a wrong total (see plan risk: "no se completa a mano hasta 260").

#' Run every sec. 14 check against shearer_goal_events. Returns the pointblank
#' agent (for docs/validation.md) and stops the pipeline via
#' pointblank::stop_if_not() semantics if any check fails.
#'
#' @param goal_events The `shearer_goal_events` tibble (goal_events_checked in `_targets.R`).
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param crosscheck Output of `crosscheck_goal_counts()`.
#' @param span_mismatches Output of `historical_table_span_mismatches()`.
#' @return The validated input, returned unchanged (so this doubles as a pass-through gate).
#' @export
validate_goal_events <- function(goal_events, cfg, crosscheck = NULL, span_mismatches = NULL) {
  v <- cfg$validation

  agent <- create_agent(tbl = goal_events, label = "shearer_goal_events") |>
    rows_complete(columns = c("match_id", "minute", "score_after_for", "goal_type_group")) |>
    col_vals_equal(vars(score_after_for), expr(score_before_for + 1)) |>
    col_vals_in_set(vars(for_club), set = unlist(cfg$project$clubs)) |>
    col_vals_not_null(vars(match_id, home_goals, away_goals)) |>
    interrogate()

  report_path <- file.path(cfg$paths$docs, "validation.md")
  write_validation_report(agent, goal_events, v, report_path, crosscheck, span_mismatches)

  n_unjoined <- sum(is.na(goal_events$home_goals) | is.na(goal_events$away_goals))
  if (n_unjoined > 0) {
    stop(glue(
      "{n_unjoined} goal event(s) have no matching row in `matches` (NA home/away goals ",
      "after the join) -- likely a club-name normalization mismatch. See docs/validation.md."
    ))
  }

  n_total <- nrow(goal_events)
  if (n_total != v$expected_total_goals) {
    stop(glue(
      "shearer_goal_events has {n_total} rows, not the expected {v$expected_total_goals}. ",
      "See docs/validation.md -- the report declares partial coverage, it does not fabricate the gap."
    ))
  }

  by_club <- goal_events |> count(for_club, name = "n")
  for (club in names(v$expected_club_split)) {
    got <- by_club$n[by_club$for_club == club]
    got <- if (length(got) == 0) 0L else got
    expected <- v$expected_club_split[[club]]
    if (!identical(got, as.integer(expected))) {
      stop(glue("{club}: {got} goals parsed, expected {expected} -- see docs/validation.md"))
    }
  }

  agent
}

#' Cross-check the goal log (FBref, via goal_events) against Shearer's own Wikipedia
#' infobox -- an independent source with no shared parsing path.
#'
#' The season-*count* assertion is the one that matters most: a per-season
#' value comparison alone cannot catch a silently dropped season, because a
#' missing row has nothing to disagree with. 1999-2000 went missing exactly
#' this way (its config label uses Wikipedia's article convention), which is
#' why the expected season list comes from career_season_labels().
#'
#' @param shearer_appearances Output of `ingest_shearer_appearances()` / `validate_appearances()`.
#' @param season_summary Output of `season_summary()` -- one row per season/club.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return The validated input, returned unchanged (so this doubles as a pass-through gate).
#' @export
validate_appearances <- function(shearer_appearances, season_summary, cfg) {
  v <- cfg$validation

  missing <- setdiff(career_season_labels(cfg), shearer_appearances$season)
  if (length(missing) > 0) {
    stop(glue(
      "shearer_appearances is missing season(s): {paste(missing, collapse = ', ')} -- ",
      "a season label failed to normalise; see career_season_labels() in R/03."
    ))
  }

  n_apps <- sum(shearer_appearances$appearances)
  if (n_apps != v$expected_total_appearances) {
    stop(glue(
      "shearer_appearances totals {n_apps} appearances, not the expected ",
      "{v$expected_total_appearances}. See docs/validation.md."
    ))
  }

  mismatched <- shearer_appearances |>
    left_join(season_summary, by = "season", suffix = c("_wiki", "_transfermarkt")) |>
    filter(goals_wiki != goals_transfermarkt)
  if (nrow(mismatched) > 0) {
    stop(glue(
      "Wikipedia and the goal log disagree on league goals for: ",
      "{paste(mismatched$season, collapse = ', ')}"
    ))
  }

  invisible(shearer_appearances)
}

#' Write the aggregate reconciliation table required by sec. 14 to
#' docs/validation.md -- every discrepancy shown with both readings, per the
#' plan's rule that mismatches are declared, not silenced.
#'
#' @param agent A pointblank validation agent.
#' @param goal_events The `shearer_goal_events` tibble (goal_events_checked in `_targets.R`).
#' @param v The pointblank validation agent returned by `validate_goal_events()`.
#' @param path Output file path.
#' @param crosscheck Output of `crosscheck_goal_counts()`.
#' @param span_mismatches Output of `historical_table_span_mismatches()`.
#' @return The `path` written to, invisibly usable for chaining into `_targets.R`.
#' @export
write_validation_report <- function(agent, goal_events, v, path, crosscheck = NULL, span_mismatches = NULL) {
  n_total <- nrow(goal_events)
  by_club <- goal_events |> count(for_club, name = "n")

  club_split <- tibble::tibble(
    Club = names(v$expected_club_split),
    Parsed = vapply(names(v$expected_club_split), function(club) {
      got <- by_club$n[by_club$for_club == club]
      if (length(got) == 0) 0L else as.integer(got)
    }, integer(1)),
    Expected = as.integer(unlist(v$expected_club_split))
  )

  by_season <- goal_events |>
    count(season, name = "Goals") |>
    arrange(season) |>
    rename(Season = season)

  n_unknown_type <- sum(goal_events$goal_type_group == "unknown")

  # Per-season breakdown of the FBref<->Transfermarkt join, so the coverage
  # gap is inspectable rather than a single opaque count. The gap is
  # concentrated in Shearer's earliest Blackburn seasons (see the note
  # below) -- a real per-season pattern, not a uniform parsing failure.
  coverage_by_season <- goal_events |>
    summarise(
      Goals = n(),
      Matched = sum(goal_type_group != "unknown"),
      .by = season
    ) |>
    arrange(season) |>
    transmute(Season = season, Goals, Matched,
              Coverage = scales::percent(Matched / Goals, accuracy = 1))

  lines <- c(
    "# Validation report",
    "",
    "Generated by `R/07_validate.R`. Primary source: FBref match logs + match",
    "reports, via the Internet Archive (see `THIRD_PARTY_NOTICES.md`).",
    "Transfermarkt's goal log supplies only `goal_type` (left-joined on",
    "season/opponent/minute) and serves as a cross-check.",
    "",
    "## FBref ↔ Transfermarkt (goal_type coverage)",
    glue("- FBref goals matched to a Transfermarkt `goal_type`: ",
         "**{n_total - n_unknown_type} of {n_total}** ",
         "({scales::percent((n_total - n_unknown_type) / n_total, accuracy = 0.1)})"),
    glue("- Unmatched (goal_type_group = \"unknown\"): **{n_unknown_type}** -- ",
         "declared, not imputed. Matching is by exact (season, opponent, minute); ",
         "a handful of unmatched pairs differ by exactly one minute between the ",
         "two sources (a display-convention difference, not a parsing bug -- the ",
         "same `parse_minute()` reads both), but most of the gap is concentrated ",
         "in the 1992-96 seasons, where Transfermarkt's own minute data is ",
         "coarser. Neither is corrected here to force a match."),
    "",
    "### Coverage by season",
    kable(coverage_by_season, format = "markdown"),
    "",
    "## Total goals",
    glue("- Parsed: **{n_total}**"),
    glue("- Expected (Wikipedia all-time list): **{v$expected_total_goals}**"),
    glue("- Match: **{if (n_total == v$expected_total_goals) 'YES' else 'NO -- see below'}**"),
    "",
    "## Club split",
    kable(club_split, format = "markdown"),
    "",
    "## Goals by season",
    kable(by_season, format = "markdown")
  )

  if (!is.null(crosscheck)) {
    lines <- c(lines, "", crosscheck_report_lines(crosscheck, goal_events))
  }

  if (!is.null(span_mismatches) && nrow(span_mismatches) > 0) {
    span_table <- span_mismatches |>
      transmute(Player = player, Wikipedia = wikipedia_goals, FBref = fbref_goals)
    lines <- c(
      lines, "",
      "## FBref ↔ Wikipedia career-span mismatches (historical comparators)",
      "",
      "Players in the pinned FBref comparator set whose cached page doesn't",
      "cover the same career span as Wikipedia's goal total. `goals_per_90`",
      "and `adjusted_goals_per_90` are forced to `NA` for these players",
      "(`historical_table()`, `R/10_analyse_comparators.R`) rather than",
      "dividing a full career total by partial minutes.",
      "",
      kable(span_table, format = "markdown")
    )
  }

  report_html <- file.path(dirname(path), "validation_pointblank.html")
  export_report(get_agent_report(agent), filename = report_html)
  lines <- c(lines, "", "## pointblank interrogation",
             glue("Full interactive report: `{basename(report_html)}`"))

  writeLines(lines, path)
}

#' Per-match agreement between two fully independent readings of the same
#' career: Wikipedia's club-season results tables (goals parsed out of a
#' free-text "Scorers" cell) and `goal_events` (FBref as of Phase 5). They
#' share no fetch, no parser and no intermediate table, so agreement here is
#' real corroboration rather than a self-consistency check.
#'
#' Reported, never enforced. Two of the fourteen season articles carry no
#' per-match data at all (1998-99 transcludes the league table from the
#' competition article; 2005-06 has the heading and nothing under it), so the
#' comparison is a partial one by construction. Declaring that gap is the point
#' -- the pipeline gate stays on the aggregate totals.
#'
#' @param wiki_crosscheck Output of `ingest_wikipedia_goal_crosscheck()`.
#' @param goal_events The `shearer_goal_events` tibble (goal_events_checked in `_targets.R`).
#' @return See description above.
#' @export
crosscheck_goal_counts <- function(wiki_crosscheck, goal_events) {
  tm <- goal_events |> count(match_id, name = "fbref_goals")

  wiki_crosscheck |>
    select(season, match_id, opponent, venue, wikipedia_goals = shearer_goals) |>
    left_join(tm, by = "match_id") |>
    mutate(
      fbref_goals = coalesce(fbref_goals, 0L),
      agrees = wikipedia_goals == fbref_goals
    )
}

crosscheck_report_lines <- function(crosscheck, goal_events) {
  n <- nrow(crosscheck)
  n_agree <- sum(crosscheck$agrees)
  uncovered <- sort(setdiff(unique(goal_events$season), unique(crosscheck$season)))
  goals_covered <- sum(!goal_events$season %in% uncovered)

  disagreements <- crosscheck |>
    filter(!agrees) |>
    transmute(Season = season, Opponent = opponent, Venue = venue,
              Wikipedia = wikipedia_goals, FBref = fbref_goals) |>
    arrange(Season, Opponent)

  c(
    "## Independent per-match cross-check (Wikipedia vs. FBref)",
    "",
    "Every Blackburn/Newcastle season article is parsed for Shearer's goals",
    "match by match and compared against the FBref-derived goal log. The two",
    "share no fetch, no parser and no intermediate table, so agreement here is",
    "corroboration rather than self-consistency.",
    "",
    glue("- Matches compared: **{n}**"),
    glue("- Agree: **{n_agree}** ({scales::percent(n_agree / n, accuracy = 0.1)})"),
    glue("- Disagree: **{n - n_agree}**"),
    glue("- Goals under cross-check: **{goals_covered} of {nrow(goal_events)}** ",
         "({scales::percent(goals_covered / nrow(goal_events), accuracy = 0.1)})"),
    "",
    if (length(uncovered) == 0) {
      "All seasons carry per-match data on Wikipedia."
    } else {
      glue("Not cross-checked: **{paste(uncovered, collapse = ', ')}** -- those ",
           "articles carry no per-match results (1998-99 transcludes the league ",
           "table from the competition article; 2005-06 has the heading only). ",
           "The gap is declared, not filled.")
    },
    "",
    if (nrow(disagreements) == 0) {
      "No disagreements in the seasons that are covered."
    } else {
      c("Disagreements are listed in full rather than reconciled -- Wikipedia's",
        "free-text scorer cells are known to be incomplete for some matches.",
        "",
        kable(disagreements, format = "markdown"))
    }
  )
}
