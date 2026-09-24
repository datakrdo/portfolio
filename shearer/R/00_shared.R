# Helpers shared between the pipeline (R/09 onward) and the Shiny app, which
# reads only data/processed/*.parquet and calls into these via library(shearer).

#' Newcastle/Blackburn-adjacent red, used only for Shearer.
#' @export
SHEARER_COLOUR <- "#C8102E"

#' Neutral grey used for everyone else in a chart -- kept clearly lighter than
#' SHEARER_COLOUR in luminance (not just hue) so the two stay distinguishable
#' for protan/deutan colour-blind readers, not only by red vs. grey.
#' @export
NEUTRAL_COLOUR <- "#8C8C8C"

#' Line-and-point time series with a plotly tooltip column -- the shape shared
#' by every season-indexed line plot in app/app.R (contemporary comparison,
#' league scoring trend). x/y/tip are embraced ({{ }}); colour/group are
#' optional and forwarded as quosures via rlang::enquo()/quo_is_null(), so a
#' caller can omit them (single series, solid `line_colour`) or map a column
#' *or* a literal (e.g. group = 1) exactly as they would write aes() by hand.
#' `line_colour` is ignored once `colour` is mapped -- a fixed and a mapped
#' colour on the same aesthetic can't coexist.
#'
#' @param data A data frame; `x`/`y`/`tip` columns are evaluated in its scope.
#' @param x Bare column for the x aesthetic; embraced with `{{ }}`.
#' @param y Bare column for the y aesthetic; embraced with `{{ }}`.
#' @param tip Bare column (or expression) supplying the plotly tooltip text; embraced with `{{ }}`.
#' @param colour Bare column for the `colour` aesthetic; embraced with `{{ }}`. Omit for a single-colour plot.
#' @param group Bare column (or literal, e.g. `1`) for the `group` aesthetic; embraced with `{{ }}`.
#' @param line_colour Fixed line/point colour used when `colour` is not mapped.
#' @return A ggplot object.
#' @export
plot_series <- function(data, x, y, tip, colour = NULL, group = NULL, line_colour = SHEARER_COLOUR) {
  colour_quo <- enquo(colour)
  group_quo <- enquo(group)
  mapped_colour <- !quo_is_null(colour_quo)
  mapping <- aes(x = {{ x }}, y = {{ y }}, text = {{ tip }})
  if (mapped_colour) mapping$colour <- colour_quo
  if (!quo_is_null(group_quo)) mapping$group <- group_quo
  p <- ggplot(data, mapping)
  p <- if (mapped_colour) p + geom_line() + geom_point(size = 1.5)
       else p + geom_line(colour = line_colour) + geom_point(colour = line_colour, size = 1.5)
  p + labs(x = NULL, y = "Goals") + theme(axis.text.x = element_text(angle = 45, hjust = 1))
}

#' Wilson score interval for a binomial proportion x/n -- appropriate where n
#' is small (e.g. a single opponent's match count) and a normal approximation
#' would extend past \[0, 1\].
#'
#' @param x Number of successes (e.g. matches scored in).
#' @param n Number of trials (e.g. matches played).
#' @param conf Confidence/credible level, e.g. `0.95`.
#' @return A one-row tibble with `estimate`, `lower`, `upper`.
#' @export
wilson_ci <- function(x, n, conf = 0.95) {
  if (n <= 0) stop("n must be > 0")
  z <- qnorm(1 - (1 - conf) / 2)
  p_hat <- x / n
  denom <- 1 + z^2 / n
  centre <- p_hat + z^2 / (2 * n)
  spread <- z * sqrt(p_hat * (1 - p_hat) / n + z^2 / (4 * n^2))
  tibble(
    estimate = p_hat,
    lower = pmax(0, (centre - spread) / denom),
    upper = pmin(1, (centre + spread) / denom)
  )
}

#' Beta-Binomial credible interval for a binomial proportion x/n -- the
#' Bayesian counterpart to wilson_ci(), same \[lower, upper\] shape so the two
#' are drop-in comparable.
#'
#' The Beta is conjugate to the Binomial: with prior Beta(a, b), the
#' posterior after observing x successes in n trials is exactly
#' Beta(x + a, n - x + b), so the interval is two qbeta() calls -- no MCMC,
#' no Stan. A one-parameter conjugate model has a closed-form posterior;
#' sampling it would add a compiler toolchain to CI for an identical answer.
#'
#' Default prior is Jeffreys' Beta(0.5, 0.5), the standard non-informative
#' prior for a proportion -- unlike a flat Beta(1, 1) or the Wilson interval's
#' normal approximation, it keeps the interval strictly inside (0, 1) even at
#' x = 0 or x = n, which is exactly where those alternatives are weakest.
#'
#' @param x Number of successes (e.g. matches scored in).
#' @param n Number of trials (e.g. matches played).
#' @param prior Beta prior shape parameters `c(a, b)`.
#' @param conf Confidence/credible level, e.g. `0.95`.
#' @return A one-row tibble with `estimate`, `lower`, `upper`.
#' @export
beta_binomial_ci <- function(x, n, prior = c(0.5, 0.5), conf = 0.95) {
  if (n <= 0) stop("n must be > 0")
  a_post <- x + prior[[1]]
  b_post <- n - x + prior[[2]]
  alpha <- 1 - conf
  tibble(
    estimate = a_post / (a_post + b_post),
    lower = qbeta(alpha / 2, a_post, b_post),
    upper = qbeta(1 - alpha / 2, a_post, b_post)
  )
}
