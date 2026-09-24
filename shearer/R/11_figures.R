# Every function takes an already-derived tibble from 09_analyse.R /
# 10_analyse_comparators.R / 04b_ingest_statsbomb.R -- nothing here computes a
# number, it only draws what those modules already validated.

# SHEARER_COLOUR / NEUTRAL_COLOUR live in R/00_shared.R (also used by app/app.R)

CAPTION_SOURCE <- "Source: Wikipedia, football-data.co.uk, Transfermarkt, StatsBomb/Hudl (see data/raw/source_manifest.csv)."

#' Goals per season, coloured by club (in `cfg$project$clubs` order -- the
#' first club gets the accent colour, every other club is neutral), with
#' each club's peak season labelled.
#'
#' @param season_summary Output of `season_summary()` -- one row per season/club.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A ggplot (or gt table) object.
#' @export
fig_trajectory <- function(season_summary, cfg) {
  peaks <- season_summary |> slice_max(goals, n = 1, by = for_club)

  clubs <- cfg$project$clubs
  club_colours <- setNames(c(SHEARER_COLOUR, rep(NEUTRAL_COLOUR, length(clubs) - 1)), clubs)

  ggplot(season_summary, aes(season, goals, colour = for_club, group = for_club)) +
    geom_line() +
    geom_point() +
    geom_text(data = peaks, aes(label = goals), vjust = -1.3, show.legend = FALSE) +
    scale_colour_manual(values = club_colours) +
    labs(
      title = "Alan Shearer -- Premier League goals by season",
      subtitle = "1992-93 to 2005-06, Blackburn Rovers and Newcastle United",
      x = NULL, y = "Goals", colour = "Club", caption = CAPTION_SOURCE
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
}

#' Stacked bar of goal-type shares, "unknown" shown alongside classified types.
#'
#' @param goal_type_breakdown Output of `goal_type_breakdown()`.
#' @return A ggplot (or gt table) object.
#' @export
fig_goal_types <- function(goal_type_breakdown) {
  ggplot(goal_type_breakdown, aes(x = 1, y = n, fill = goal_type_group)) +
    geom_col(position = "stack") +
    geom_text(aes(label = glue::glue("{goal_type_group}\n{n} ({scales::percent(share, accuracy = 1)})")),
              position = position_stack(vjust = 0.5), size = 3) +
    coord_flip() +
    labs(
      title = "Alan Shearer -- goal type, known and unknown coverage declared",
      subtitle = glue::glue("n = {goal_type_breakdown$n_total[[1]]}; 'unknown' is Transfermarkt's 'Not reported', not imputed"),
      x = NULL, y = "Goals", fill = "Type", caption = CAPTION_SOURCE
    ) +
    theme_minimal() +
    theme(axis.text.y = element_blank(), axis.ticks.y = element_blank())
}

#' Bars of goal context (equaliser / go_ahead / reduce_deficit / extend_lead).
#'
#' @param context_breakdown Output of `context_breakdown()`.
#' @return A ggplot (or gt table) object.
#' @export
fig_context <- function(context_breakdown) {
  ordered <- context_breakdown |> mutate(context = factor(context, levels = context[order(-n)]))

  ggplot(ordered, aes(context, n)) +
    geom_col(fill = SHEARER_COLOUR) +
    geom_text(aes(label = n), vjust = -0.5) +
    labs(
      title = "Alan Shearer -- context of each goal at the moment scored",
      x = NULL, y = "Goals", caption = CAPTION_SOURCE
    ) +
    theme_minimal()
}

#' Shearer vs. the ten highest career scorers, raw rate and era-adjusted rate
#' on the same row -- the same top-10-by-goals subset the dashboard's
#' Comparators tab uses (`top10_bands` in `app/app.R`).
#'
#' The hollow point is goals per appearance as recorded; the filled one is that
#' rate divided by the scoring environment of the seasons the player actually
#' played in. The segment between them is the whole adjustment, drawn rather
#' than asserted, so a reader can see it is small -- which is the finding. Rank
#' is by the adjusted rate; goals/90 is deliberately absent (minutes are not
#' verifiable for 1992-2006).
#'
#' @param historical_table Output of `historical_table()`.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @return A ggplot (or gt table) object.
#' @export
fig_comparators <- function(historical_table, cfg) {
  ranked <- historical_table |>
    filter(!is.na(adjusted_goals_per_90)) |>
    slice_max(goals, n = 10, with_ties = FALSE) |>
    select(-adjusted_goals_per_90) |>
    mutate(label = glue::glue("{player} ({goals})")) |>
    arrange(adjusted_goals_per_appearance) |>
    mutate(label = factor(label, levels = label))

  shearer <- ranked |> filter(player == cfg$project$player_name)

  ggplot(ranked, aes(y = label)) +
    geom_segment(aes(x = goals_per_appearance, xend = adjusted_goals_per_appearance,
                     yend = label, colour = highlight),
                 arrow = arrow(length = unit(0.06, "in"), type = "closed")) +
    geom_point(aes(x = goals_per_appearance), shape = 21, fill = "white",
               colour = NEUTRAL_COLOUR, size = 1.8) +
    geom_point(aes(x = adjusted_goals_per_appearance, colour = highlight), size = 2.2) +
    geom_text(data = shearer,
              aes(x = adjusted_goals_per_appearance,
                  label = glue::glue("  era index {round(era_index, 3)} across {seasons_matched} seasons")),
              hjust = 0, size = 3, colour = SHEARER_COLOUR) +
    scale_colour_manual(values = c(`TRUE` = SHEARER_COLOUR, `FALSE` = NEUTRAL_COLOUR), guide = "none") +
    scale_x_continuous(expand = expansion(mult = c(0.02, 0.30))) +
    labs(
      title = "Era-adjusted scoring rate among the ten highest career scorers",
      subtitle = paste(
        "Hollow point: goals per appearance as recorded. Filled point: the same rate divided",
        "by\nthe league's goals per match over that player's own seasons. Career goals in",
        "brackets.\nThe adjustment is real but small: it narrows the gap to the modern",
        "scorers, not the order."
      ),
      x = "Goals per appearance", y = NULL, caption = CAPTION_SOURCE
    ) +
    theme_minimal()
}

#' The 2 StatsBomb-tracked Shearer shots (2003/04, Arsenal's matches only) on
#' a pitch -- evidence of the limit, not a shot map. Handles 0 rows.
#'
#' @param shots Output of `ingest_statsbomb_shearer_shots()`.
#' @return A ggplot (or gt table) object.
#' @export
fig_statsbomb_shots <- function(shots) {
  ggplot(shots, aes(x, y)) +
    annotate_pitch(dimensions = pitch_statsbomb, colour = "grey60", fill = "white") +
    geom_point(colour = SHEARER_COLOUR, size = 4) +
    coord_flip(xlim = c(0, 120), ylim = c(0, 80)) +
    theme_pitch() +
    labs(
      title = glue::glue("All the spatial data on Shearer in open data: {nrow(shots)} shots, 0 goals"),
      subtitle = "StatsBomb/Hudl coverage of Shearer's career: Arsenal's 2003/04 matches only",
      caption = CAPTION_SOURCE
    )
}
