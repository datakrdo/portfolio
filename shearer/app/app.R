# Alan Shearer — Premier League goal-scoring career, 1992-93 to 2005-06.
#
# Reads only data/processed/*.parquet (written by _targets.R) and config.yaml
# — never recomputes a number. If data/processed/ is stale, load_shearer_data()
# (R/13_dataset.R) fails loudly at startup rather than rendering a half-built
# dashboard; re-run targets::tar_make() from the project root first.

suppressPackageStartupMessages({
  library(shiny)
  library(bslib)
  library(arrow)
  library(dplyr)
  library(reactable)
  library(plotly)
  library(glue)
  library(ggplot2)
  library(thematic)
  library(shearer)
})

# app.R's working directory is app/ when launched via shiny::runApp("app");
# the project root is one level up. A deployed bundle (deploy.R) instead
# flattens app.R and www/ to its own root alongside data/config/docs, so
# ROOT stays "." and WWW resolves relative to it rather than assuming an
# "app/" subfolder.
ROOT <- if (dir.exists("../data/processed")) ".." else "."
WWW <- if (dir.exists("app/www")) "app/www" else "www"
PROC <- file.path(ROOT, "data/processed")
DOCS <- file.path(ROOT, "docs")

cfg <- yaml::read_yaml(file.path(ROOT, "config/config.yaml"))
dat <- load_shearer_data(PROC, cfg)

PLAYER <- cfg$project$player_name

matches <- dat@matches
goal_events <- dat@goal_events
season_summary_tbl <- dat@season_summary
goal_type_breakdown_tbl <- dat@goal_type_breakdown
contemporary_summary_tbl <- dat@contemporary_summary
contemporary_overview_tbl <- dat@contemporary_overview
historical_table_tbl <- dat@historical_table
league_scoring_trend_tbl <- dat@league_scoring_trend
season_era_index_tbl <- dat@season_era_index
context_breakdown_tbl <- dat@context_breakdown
comparator_context_breakdown_tbl <- dat@comparator_context_breakdown

# Makes every ggplot pick up the active bslib theme (including the dark-mode
# toggle below) instead of a fixed theme_minimal() that would stay white.
thematic_shiny(font = "auto")

opponents <- sort(setdiff(unique(c(matches$home_team, matches$away_team)), unlist(cfg$project$clubs)))
all_seasons <- sort(unique(matches$season))

# Shearer's clutch-share rank among the 10 comparators (Phase 7) — one number
# per player, computed the same way as the Overview KPI (clutch_share()).
comparator_clutch_shares <- comparator_context_breakdown_tbl |>
  reframe(clutch_share = clutch_share(pick(context, n, share, n_total)), .by = player) |>
  arrange(desc(clutch_share))

# The 10 career comparators with FBref minutes on file, used by both the
# dumbbell chart and its insight -- computed once at startup since it has no
# reactive dependency (input$dark_mode only recolours the same rows).
top10_bands <- historical_table_tbl |>
  filter(!is.na(adjusted_goals_per_90)) |>
  slice_max(goals, n = 10, with_ties = FALSE) |>
  mutate(mid_year = (first_year + last_year) / 2)

season_era_index_tbl_known <- season_era_index_tbl |> filter(!is.na(adjusted_goals_per_90))

# Every chart's football-angle reading (R/14_insights.R) except the rival
# explorer's, which depends on the sidebar selection -- computed once here so
# toggling dark mode or resizing a card never re-derives them.
insight_trajectory <- trajectory_insight(season_summary_tbl)
insight_contemporary <- contemporary_insight(season_summary_tbl, contemporary_summary_tbl)
insight_dumbbell <- dumbbell_insight(top10_bands, PLAYER)
insight_season_line <- season_line_insight(season_era_index_tbl_known)
insight_goal_type <- goal_type_insight(goal_type_breakdown_tbl)
insight_context <- context_insight(context_breakdown_tbl)
insight_comparator_clutch <- comparator_clutch_insight(comparator_clutch_shares, PLAYER)

# fontawesome is installed; bsicons is not — use fontawesome for value_box showcases.
bsicons_or_fa <- function(name) fontawesome::fa(name, fill = "currentColor")

# CSS variables, not fixed hex, so these value boxes actually flip with the
# light/dark toggle (a fixed hex is baked into static HTML once at startup
# and never changes client-side). Only "Career goals" keeps SHEARER_COLOUR
# as a fixed accent, on purpose.
KPI_THEME <- value_box_theme(bg = "var(--bs-secondary-bg)", fg = "var(--bs-body-color)")

# A short, data-derived reading under a chart -- the football angle, not the
# methodology (that stays in the card header's ⓘ tooltip). One shape reused
# ~9 times rather than a bespoke div per chart.
insight_callout <- function(text) {
  div(class = "insight-callout", span(class = "insight-icon", "⚽"), text)
}

# ---- UI ---------------------------------------------------------------

# Amber marker for the "league top scorer" ⚽ glyph. A single hex fails in one
# theme or the other -- pure yellow (#F1C40F) has poor contrast against a
# light-mode white card; a darker amber goes muddy on a dark-mode card.
# leader_colour() picks the right one from input$dark_mode.
leader_colour <- function(dark) if (isTRUE(dark)) "#F7D774" else "#B7791F"

# reactable's default theme hardcodes a white background; it does not inherit
# bslib's CSS variables on its own. Resolving colours from CSS variables (not
# fixed hex) makes the tables follow the dark-mode toggle live, in the
# browser, with no server-side re-render.
bs_reactable_theme <- reactableTheme(
  color = "var(--bs-body-color)",
  backgroundColor = "transparent",
  borderColor = "var(--bs-border-color)",
  highlightColor = "var(--bs-secondary-bg)",
  inputStyle = list(backgroundColor = "var(--bs-body-bg)",
                     color = "var(--bs-body-color)",
                     borderColor = "var(--bs-border-color)"),
  searchInputStyle = list(backgroundColor = "var(--bs-body-bg)",
                           color = "var(--bs-body-color)"),
  selectStyle = list(backgroundColor = "var(--bs-body-bg)",
                      color = "var(--bs-body-color)"),
  pageButtonHoverStyle = list(backgroundColor = "var(--bs-secondary-bg)")
)

# Native plotly charts (spider, donut) aren't ggplots, so thematic_shiny()
# doesn't theme them -- resolve their colours from input$dark_mode instead.
plotly_bs <- function(p, dark) {
  text_colour <- if (dark) "#dee2e6" else "#212529"
  p |> layout(
    paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
    font = list(color = text_colour),
    legend = list(bgcolor = "rgba(0,0,0,0)", font = list(color = text_colour))
  )
}

# thematic_shiny() bakes gridline colour from the app's theme once at
# startup, so it doesn't follow the client-side toggle either -- same fix as
# plotly_bs(), applied to the ggplot before it's converted.
grid_theme <- function(dark) {
  # A translucent overlay rather than a fixed grey: it stays equally subtle
  # against either page background instead of needing a separate hand-picked
  # shade per mode.
  theme(panel.grid.major = element_line(colour = if (dark) "rgba(255,255,255,0.10)" else "rgba(0,0,0,0.08)"),
        panel.grid.minor = element_blank())
}

# bslib theme, extended with the dashboard's own rules via bs_add_rules() --
# passing tags$style() as a direct child of page_navbar() (the previous
# approach) triggers a "navigation containers expect nav_panel()" warning at
# startup, since page_navbar() treats every top-level child as a nav item.
app_theme <- bs_theme(version = 5, preset = "shiny", primary = SHEARER_COLOUR,
                       base_font = font_collection("system-ui", "-apple-system", "Segoe UI", "Roboto", "sans-serif")) |>
  bs_add_rules(sass::sass_file(file.path(WWW, "styles.scss")))

ui <- page_navbar(
  title = "Alan Shearer — the Premier League's all-time top scorer",
  theme = app_theme,
  fillable = FALSE,

  nav_panel(
    "Overview",
    layout_column_wrap(
      width = 1 / 6,
      value_box("Career goals", nrow(goal_events), showcase = bsicons_or_fa("futbol"),
                 theme = KPI_THEME),
      value_box("Seasons", length(all_seasons), showcase = bsicons_or_fa("calendar"),
                 theme = KPI_THEME),
      value_box("Goals per 90 minutes",
                 round(nrow(goal_events) / (sum(season_summary_tbl$minutes) / 90), 2),
                 showcase = bsicons_or_fa("stopwatch"),
                 theme = KPI_THEME),
      value_box("League top scorer", contemporary_overview_tbl$times_led, showcase = bsicons_or_fa("trophy"),
                 theme = KPI_THEME),
      value_box("Clutch goals",
                 scales::percent(clutch_share(context_breakdown_tbl), accuracy = 1),
                 showcase = bsicons_or_fa("bolt"),
                 theme = KPI_THEME),
      value_box("Mins per goal/assist",
                 round(historical_table_tbl$minutes_per_goal_or_assist[historical_table_tbl$player == PLAYER], 0),
                 showcase = bsicons_or_fa("bullseye"),
                 theme = KPI_THEME)
    ),
    card(
      full_screen = TRUE,
      card_header(
        "Goals by season and club",
        tooltip(
          bsicons_or_fa("circle-info"),
          "Goals per season, by club. The two footnoted low seasons are injury years, not a form slump."
        )
      ),
      plotlyOutput("trajectory_plot", height = 420),
      insight_callout(insight_trajectory),
      card_footer(
        tags$small(
          tags$p(
            "The two low points — 2 goals in 1997–98 and 5 goals in 2000–01 — were injury-driven, not a form slump: ",
            "17 and 19 league appearances that season respectively, versus 28+ in every other Newcastle season."
          ),
          tags$ul(
            tags$li(
              "1997–98: an ankle ligament injury from a pre-season friendly at Goodison Park sidelined him for most of the season ",
              "(", tags$em("The Athletic"), ", 2019)."
            ),
            tags$li(
              "2000–01: an \"injury-hit and frustrating season\", after retiring from international football post-Euro 2000 to focus on recovery ",
              "(", tags$em("The Daily Telegraph"), ", 27 Feb 2000)."
            )
          )
        )
      )
    )
  ),

  nav_panel(
    "Comparators",
    navset_card_tab(
      full_screen = TRUE,
      nav_panel(
        "Shearer vs. the season's top scorer",
        plotlyOutput("contemporary_plot", height = 420),
        insight_callout(insight_contemporary)
      ),
      nav_panel(
        "Career comparison (era-adjusted)",
        card_header(
          "One row per career scorer among the top 10 all-time",
          tooltip(
            bsicons_or_fa("circle-info"),
            "Hollow point: raw goals per 90 minutes. Filled point: that rate divided by the",
            "player's career-average era index — the segment shows the size of the adjustment.",
            "Rows are ordered by the adjusted rate; a player can rank differently on the raw",
            "vs. adjusted figure, which is the point of showing both."
          )
        ),
        plotlyOutput("career_dumbbell_plot", height = 460),
        insight_callout(insight_dumbbell)
      ),
      nav_panel(
        "Shearer season-by-season",
        card_header(
          "Era-adjusted rate, season by season",
          tooltip(
            bsicons_or_fa("circle-info"),
            "Shearer's own goals per 90 minutes each season (real per-match minutes, from FBref),",
            "divided by that season's league-wide goals per match — the era adjustment applied at",
            "season granularity rather than averaged over his whole career."
          )
        ),
        plotlyOutput("season_line_plot", height = 420),
        insight_callout(insight_season_line)
      )
    )
  ),

  nav_panel(
    "Goal types & context",
    layout_columns(
      card(
        full_screen = TRUE,
        card_header("Goal type — coverage declared, not imputed"),
        plotlyOutput("goal_type_plot"),
        insight_callout(insight_goal_type)
      ),
      card(
        full_screen = TRUE,
        card_header("Context at the moment scored"),
        plotlyOutput("context_plot"),
        insight_callout(insight_context)
      )
    ),
    card(
      full_screen = TRUE,
      card_header(
        "Clutch context share across the 10 comparators",
        tooltip(
          bsicons_or_fa("circle-info"),
          "Share of each player's own goals that were an equaliser or put their team ahead —",
          "computed the same way as Shearer's own KPI above, from that player's own match reports",
          "(see docs/coverage.md for any report the Internet Archive had no capture of)."
        )
      ),
      plotlyOutput("comparator_context_plot", height = 420),
      insight_callout(insight_comparator_clutch)
    )
  ),

  nav_panel(
    "Rival explorer",
    layout_sidebar(
      sidebar = sidebar(
        selectInput("rival", "Opponent",
                    choices = opponents,
                    selected = opponents[[1]]),
        sliderInput("season_range", "Season range",
                    min = 1, max = length(all_seasons), value = c(1, length(all_seasons)),
                    step = 1, ticks = FALSE),
        uiOutput("season_range_label"),
        helpText("Season index maps to:", paste(all_seasons, collapse = ", "))
      ),
      card(full_screen = TRUE, plotlyOutput("rival_plot", height = 320)),
      uiOutput("rival_insight"),
      uiOutput("rival_summary"),
      reactableOutput("rival_table")
    )
  ),

  nav_panel(
    "Tables",
    navset_card_tab(
      nav_panel(
        "Top 10 all-time scorers",
        card_header(
          "The ten highest career scorers on the Premier League's 100+ goals list",
          tooltip(
            bsicons_or_fa("circle-info"),
            "25 more sit below 100 goals and aren't shown here. Era index: the mean league-wide",
            "goals/match over each player's own seasons, divided by the mean across every season",
            "in this dataset (1993–2025). Below 1 means a lower-scoring era than the overall",
            "average. 'Adj. Goals/App' and 'Adj. Goals/90' divide the raw rate by that index.",
            "Goals/90 and Min/G+A (minutes per goal or assist) use real per-match minutes from",
            "FBref — blank when a player's cached FBref page doesn't cover the same career span",
            "as this list, a gap declared in docs/validation.md rather than guessed. Clutch %:",
            "share of that player's own goals that were an equaliser or put their team ahead",
            glue("(Phase 7 comparator match reports). {PLAYER}'s row is always highlighted.")
          )
        ),
        reactableOutput("historical_dt")
      ),
      nav_panel("Every goal", reactableOutput("event_table"))
    )
  ),

  nav_panel(
    "Data & limitations",
    layout_columns(
      col_widths = c(6, 6),
      card(card_header("Limitations"), uiOutput("limitations_md")),
      card(card_header("Methodology"), uiOutput("methodology_md"))
    )
  ),

  nav_spacer(),
  nav_item(input_dark_mode(id = "dark_mode"))
)

# ---- Server -------------------------------------------------------------

server <- function(input, output, session) {

  output$trajectory_plot <- renderPlotly({
    clubs <- cfg$project$clubs
    club_colours <- setNames(c(SHEARER_COLOUR, rep(NEUTRAL_COLOUR, length(clubs) - 1)), clubs)
    traj <- season_summary_tbl |>
      arrange(season) |>
      left_join(contemporary_summary_tbl |> select(season, is_leader), by = "season") |>
      mutate(
        is_leader = coalesce(is_leader, FALSE),
        tip = glue("{season} · {for_club}: {goals} goals")
      )
    leaders <- traj |> mutate(tip = glue("{season}: {PLAYER} was the top scorer")) |> filter(is_leader)
    # Bespoke aes (neutral line, points coloured by club) — not the
    # single-series-or-single-colour shape plot_series() covers.
    p <- ggplot(traj, aes(season, goals, group = 1, text = tip)) +
      geom_line(colour = NEUTRAL_COLOUR) +
      geom_point(aes(colour = for_club), size = 2) +
      geom_point(data = leaders, shape = 21, size = 6, stroke = 1.5,
                 fill = leader_colour(identical(input$dark_mode, "dark")), colour = "black") +
      scale_colour_manual(values = club_colours) +
      labs(x = NULL, y = "Goals", colour = "Club",
           subtitle = glue("● = {PLAYER} was that season's Premier League top scorer")) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      grid_theme(identical(input$dark_mode, "dark"))
    ggplotly(p, tooltip = "text") |> plotly_bs(identical(input$dark_mode, "dark"))
  }) |> bindCache(input$dark_mode)

  season_window <- reactive({
    all_seasons[input$season_range[1]:input$season_range[2]]
  })

  output$season_range_label <- renderUI({
    helpText(glue("{season_window()[1]} to {season_window()[length(season_window())]}"))
  })

  rival_matches <- reactive({
    club <- input$rival
    matches |>
      filter(home_team == club | away_team == club, season %in% season_window()) |>
      left_join(
        goal_events |> filter(opponent == club) |> count(match_id, name = "shearer_goals"),
        by = "match_id"
      ) |>
      mutate(shearer_goals = coalesce(shearer_goals, 0L)) |>
      arrange(date)
  })

  output$rival_plot <- renderPlotly({
    rm <- rival_matches()
    if (nrow(rm) == 0) return(plotly_empty(type = "scatter", mode = "markers"))
    rm <- rm |> mutate(
      scored = shearer_goals > 0,
      tip = glue("{date}: {home_team} {home_goals}-{away_goals} {away_team} · {PLAYER} {shearer_goals}")
    )
    p <- ggplot(rm, aes(date, shearer_goals, text = tip)) +
      geom_segment(aes(xend = date, yend = 0), colour = NEUTRAL_COLOUR) +
      geom_point(aes(colour = scored), size = 2) +
      scale_colour_manual(values = c(`TRUE` = SHEARER_COLOUR, `FALSE` = NEUTRAL_COLOUR), guide = "none") +
      scale_y_continuous(breaks = 0:max(1L, rm$shearer_goals), limits = c(0, NA)) +
      labs(x = NULL, y = "Goals", subtitle = glue("{PLAYER}'s goals vs. {input$rival}, per meeting")) +
      grid_theme(identical(input$dark_mode, "dark"))
    ggplotly(p, tooltip = "text") |> plotly_bs(identical(input$dark_mode, "dark"))
  }) |> bindCache(input$rival, input$season_range, input$dark_mode)

  output$rival_insight <- renderUI({
    insight_callout(rival_insight(rival_matches(), input$rival))
  })

  output$rival_summary <- renderUI({
    rm <- rival_matches()
    n <- nrow(rm)
    x <- sum(rm$shearer_goals > 0)
    if (n == 0) return(NULL)
    wci <- wilson_ci(x, n)
    bci <- beta_binomial_ci(x, n)
    tagList(
      p(glue(
        "Scored in {x}/{n} meetings ({scales::percent(x / n, accuracy = 1)}) — ",
        "Wilson 95% CI [{scales::percent(wci[['lower']], accuracy = 1)}, {scales::percent(wci[['upper']], accuracy = 1)}], ",
        "Beta-Binomial 95% credible interval [{scales::percent(bci[['lower']], accuracy = 1)}, {scales::percent(bci[['upper']], accuracy = 1)}]."
      )),
      helpText(
        "The Wilson interval is a frequentist coverage statement about the procedure; the",
        "Beta-Binomial interval (Jeffreys prior) is a direct probability statement about this",
        "opponent's rate. They agree closely here and diverge most when x is 0 or n — small-n",
        "intervals, not flat rates. See Data & limitations."
      )
    )
  })

  output$rival_table <- renderReactable({
    rival_matches() |>
      transmute(Season = season, Date = date, Home = home_team, Away = away_team,
                Score = glue("{home_goals}-{away_goals}"), `Shearer goals` = shearer_goals) |>
      reactable(sortable = TRUE, highlight = TRUE, compact = TRUE, defaultPageSize = 10, theme = bs_reactable_theme)
  })

  output$goal_type_plot <- renderPlotly({
    tipped <- goal_type_breakdown_tbl |>
      mutate(tip = glue("{goal_type_group}: {n} ({scales::percent(share, accuracy = 1)})"))
    p <- plot_ly(
      tipped, labels = ~goal_type_group, values = ~n, text = ~tip, hovertext = ~tip,
      type = "pie", hole = 0.55, textinfo = "label+percent",
      marker = list(colors = c(SHEARER_COLOUR, NEUTRAL_COLOUR, "#A9A9A9", "#D3D3D3"))
    ) |>
      layout(showlegend = FALSE,
             annotations = list(text = glue("{sum(tipped$n)} goals"), showarrow = FALSE, font = list(size = 16)))
    plotly_bs(p, identical(input$dark_mode, "dark"))
  }) |> bindCache(input$dark_mode)

  output$context_plot <- renderPlotly({
    ctx <- context_breakdown_tbl |> arrange(desc(n))
    ctx_closed <- ctx |> bind_rows(ctx[1, ])
    p <- plot_ly(
      ctx_closed, type = "scatterpolar", mode = "lines+markers", fill = "toself",
      r = ~n, theta = ~context, line = list(color = SHEARER_COLOUR),
      fillcolor = "rgba(200,16,46,0.25)", marker = list(color = SHEARER_COLOUR),
      hovertext = ~glue("{context}: {n} ({scales::percent(share, accuracy = 1)})"),
      hoverinfo = "text"
    ) |>
      layout(polar = list(radialaxis = list(visible = TRUE, rangemode = "tozero")), showlegend = FALSE)
    plotly_bs(p, identical(input$dark_mode, "dark"))
  }) |> bindCache(input$dark_mode)

  output$comparator_context_plot <- renderPlotly({
    bars <- comparator_context_breakdown_tbl |>
      mutate(
        player = factor(player, levels = rev(comparator_clutch_shares$player)),
        context = factor(context, levels = c("go_ahead", "equaliser", "extend_lead", "reduce_deficit")),
        is_shearer = player == cfg$project$player_name,
        tip = glue("{player} — {context}: {n} ({scales::percent(share, accuracy = 1)})")
      )
    p <- ggplot(bars, aes(x = share, y = player, fill = context, text = tip, colour = is_shearer)) +
      geom_col(position = position_stack(reverse = TRUE), linewidth = 0.8) +
      scale_x_continuous(labels = scales::percent) +
      scale_colour_manual(values = c(`TRUE` = "black", `FALSE` = "transparent"), guide = "none") +
      labs(x = "Share of career goals", y = NULL, fill = "Context")
    ggplotly(p, tooltip = "text") |> plotly_bs(identical(input$dark_mode, "dark"))
  }) |> bindCache(input$dark_mode)

  output$contemporary_plot <- renderPlotly({
    # Shearer's own goal total (season_summary_tbl) is always known; the top
    # scorer's total (contemporary_summary_tbl$leader_goals) is too — neither
    # depends on Shearer having made that season's top-10, so both lines are
    # continuous across all 14 seasons (his top-10 rank is still shown via ✱).
    long <- season_summary_tbl |>
      select(season, `Shearer` = goals) |>
      left_join(contemporary_summary_tbl |> select(season, `Season's top scorer` = leader_goals, leader_name, is_leader),
                by = "season") |>
      tidyr::pivot_longer(c(Shearer, `Season's top scorer`), names_to = "series", values_to = "goals") |>
      mutate(tip = if_else(
        series == "Season's top scorer",
        glue("{season} · {leader_name} ({goals} goals)"),
        glue("{season} · {PLAYER}: {goals} goals")
      ))
    leaders <- long |> filter(series == "Shearer", is_leader) |>
      mutate(tip = glue("{season}: {PLAYER} was the top scorer"))
    p <- plot_series(long, season, goals, tip, colour = series, group = series) +
      geom_point(data = leaders, shape = 21, size = 6, stroke = 1.5,
                 fill = leader_colour(identical(input$dark_mode, "dark")), colour = "black") +
      scale_colour_manual(values = c("Shearer" = SHEARER_COLOUR, "Season's top scorer" = NEUTRAL_COLOUR)) +
      labs(colour = NULL,
           subtitle = glue("{PLAYER}'s season total vs. that season's actual top scorer — ● marks the seasons he was the top scorer")) +
      grid_theme(identical(input$dark_mode, "dark"))
    ggplotly(p, tooltip = "text") |> plotly_bs(identical(input$dark_mode, "dark"))
  }) |> bindCache(input$dark_mode)

  output$career_dumbbell_plot <- renderPlotly({
    bands <- top10_bands |>
      arrange(adjusted_goals_per_90) |>
      mutate(
        player = factor(player, levels = player), is_shearer = player == PLAYER,
        tip_raw = glue("{player}: {round(goals_per_90, 2)} goals/90 (raw)"),
        tip_adjusted = glue("{player}: {round(adjusted_goals_per_90, 2)} goals/90 (era-adjusted)")
      )

    p <- ggplot(bands, aes(y = player)) +
      geom_segment(aes(x = goals_per_90, xend = adjusted_goals_per_90, yend = player),
                   colour = NEUTRAL_COLOUR, linewidth = 1) +
      geom_point(aes(x = goals_per_90, text = tip_raw),
                 shape = 21, fill = "white", colour = NEUTRAL_COLOUR, size = 3) +
      geom_point(aes(x = adjusted_goals_per_90, colour = is_shearer, text = tip_adjusted), size = 3) +
      scale_colour_manual(values = c(`TRUE` = SHEARER_COLOUR, `FALSE` = NEUTRAL_COLOUR), guide = "none") +
      labs(x = "Goals per 90 minutes (hollow = raw, filled = era-adjusted)", y = NULL)
    ggplotly(p, tooltip = "text") |> plotly_bs(identical(input$dark_mode, "dark"))
  }) |> bindCache(input$dark_mode)

  output$season_line_plot <- renderPlotly({
    seasons <- season_era_index_tbl_known |>
      mutate(
        year = as.integer(substr(season, 1, 4)),
        tip = glue(
          "{season}: {round(goals_per_90, 2)} goals/90 ÷ era index {round(season_era_index, 2)} = ",
          "{round(adjusted_goals_per_90, 2)} (league ran {round(avg_goals_per_match, 2)} goals/match)"
        )
      )
    p <- ggplot(seasons, aes(year, adjusted_goals_per_90, group = 1, text = tip)) +
      geom_line(colour = SHEARER_COLOUR) +
      geom_point(colour = SHEARER_COLOUR, size = 2) +
      scale_x_continuous(breaks = seasons$year, labels = seasons$season) +
      labs(x = NULL, y = "Era-adjusted goals per 90 minutes") +
      grid_theme(identical(input$dark_mode, "dark"))
    ggplotly(p, tooltip = "text") |> plotly_bs(identical(input$dark_mode, "dark"))
  }) |> bindCache(input$dark_mode)

  output$historical_dt <- renderReactable({
    historical_table_tbl |>
      slice_max(goals, n = 10, with_ties = FALSE) |>
      left_join(comparator_clutch_shares, by = "player") |>
      transmute(Player = player, Goals = goals, Appearances = appearances,
                `Goals/App` = goals_per_appearance,
                `Goals/90` = goals_per_90,
                `Era index` = era_index,
                `Adj. Goals/App` = adjusted_goals_per_appearance,
                `Adj. Goals/90` = adjusted_goals_per_90,
                `Min/G+A` = minutes_per_goal_or_assist,
                `Clutch %` = clutch_share) |>
      reactable(
        sortable = TRUE, highlight = TRUE, compact = TRUE, defaultPageSize = 10,
        theme = bs_reactable_theme,
        defaultSorted = list(Goals = "desc"),
        # Shearer stays visibly marked regardless of sort order -- rowStyle
        # runs per row on the client, so this survives any column sort.
        rowStyle = JS(glue(
          "function(rowInfo) {{
             if (rowInfo.values['Player'] === '{PLAYER}') {{
               return {{ backgroundColor: 'rgba(var(--bs-primary-rgb), 0.15)', fontWeight: 'bold' }}
             }}
           }}"
        )),
        columns = list(
          `Goals/App` = colDef(format = colFormat(digits = 2)),
          `Goals/90` = colDef(format = colFormat(digits = 2)),
          `Era index` = colDef(format = colFormat(digits = 2)),
          `Adj. Goals/App` = colDef(format = colFormat(digits = 2)),
          `Adj. Goals/90` = colDef(format = colFormat(digits = 2)),
          `Min/G+A` = colDef(format = colFormat(digits = 0)),
          `Clutch %` = colDef(format = colFormat(percent = TRUE, digits = 0))
        )
      )
  })

  output$event_table <- renderReactable({
    goal_events |>
      transmute(Season = season, Club = for_club, Opponent = opponent, Minute = minute,
                Type = goal_type_group, Context = context, Source = source) |>
      reactable(
        sortable = TRUE, highlight = TRUE, compact = TRUE, filterable = TRUE, searchable = TRUE,
        defaultPageSize = 15, defaultSorted = list(Season = "asc"), theme = bs_reactable_theme
      )
  })

  render_md <- function(path) {
    if (!file.exists(path)) return(p(glue("{path} not found — run targets::tar_make() first.")))
    HTML(markdown::markdownToHTML(path, fragment.only = TRUE))
  }
  output$limitations_md <- renderUI(render_md(file.path(DOCS, "limitations.md")))
  output$methodology_md <- renderUI(render_md(file.path(DOCS, "methodology.md")))
}

shinyApp(ui, server)
