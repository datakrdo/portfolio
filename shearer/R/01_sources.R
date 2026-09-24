# The single point of contact with the network. Every other R/ module reads
# from data/raw/ or data/interim/ -- none of them calls httr2/rvest directly.
# That way every number in the report traces back to one row in
# data/raw/source_manifest.csv.

#' Fetch a URL exactly once and cache it under data/raw/.
#'
#' If `dest` already exists, the network is never touched and no new
#' manifest row is written -- the existing row is the provenance record.
#' On a real fetch, appends one row to `manifest_path` with the retrieval
#' timestamp, HTTP status, and byte count.
#'
#' @param url Full URL to fetch.
#' @param dest Path (relative to project root) to write the raw response to,
#'   e.g. "data/raw/transfermarkt_shearer_goals.html".
#' @param source_name Short label matching a `sources:` entry in
#'   config/config.yaml, e.g. "transfermarkt_goal_log".
#' @param cfg The parsed config list (from `load_config()`), used for the
#'   user agent and rate-limit pause.
#' @param notes Optional free-text note for the manifest row (e.g. season).
#' @return `dest`, invisibly.
#' @export
cache_fetch <- function(url, dest, source_name, cfg, notes = NA_character_) {
  dir_create(path_dir(dest))

  if (file_exists(dest)) {
    message(glue("[cached] {source_name}: {dest}"))
    return(invisible(dest))
  }

  Sys.sleep(cfg$fetch$time_pause_seconds)

  req <- request(url) |>
    req_user_agent(cfg$fetch$user_agent) |>
    req_retry(max_tries = cfg$fetch$max_retries)

  resp <- req_perform(req)
  status <- resp_status(resp)

  writeBin(resp_body_raw(resp), dest)

  append_manifest_row(
    source_name = source_name,
    url = url,
    http_status = status,
    bytes = file_size(dest),
    notes = notes,
    manifest_path = cfg$paths$manifest
  )

  message(glue("[fetched] {source_name}: {url} -> {dest} ({status})"))
  invisible(dest)
}

#' Fetch and parse a JSON API response (used for the Wikipedia API), caching
#' the raw JSON the same way `cache_fetch()` caches HTML/CSV.
#'
#' @param url Full URL to fetch.
#' @param dest Path (relative to project root) to write the raw response to.
#' @param source_name Short label matching a `sources:` entry in config/config.yaml, e.g. "transfermarkt_goal_log".
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param notes Optional free-text note for the manifest row.
#' @return See description above.
#' @export
cache_fetch_json <- function(url, dest, source_name, cfg, notes = NA_character_) {
  path <- cache_fetch(url, dest, source_name, cfg, notes)
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

append_manifest_row <- function(source_name, url, http_status, bytes, notes,
                                 manifest_path) {
  row <- data.frame(
    source_name = source_name,
    url = url,
    retrieved_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    http_status = http_status,
    bytes = bytes,
    notes = notes,
    stringsAsFactors = FALSE
  )
  write_csv(row, manifest_path, append = file_exists(manifest_path))
}

#' Resolve the Wayback Machine snapshot URL for `original_url` via the CDX
#' API, picking the snapshot with the largest `length` (the full-page
#' capture: FBref's `id_` raw mode was observed returning a truncated,
#' table-less 47 KB snapshot next to a 222 KB normal-mode one for the same
#' URL, so "biggest" is a real signal, not a tie-break).
#'
#' Some CDX entries turn out to 404 when actually fetched (a stale index,
#' not a real absence) -- `exclude_timestamps` lets `cache_fetch_wayback()`
#' ask for the next-largest snapshot instead of giving up after one miss.
#'
#' @param original_url The live URL to look up (e.g. an fbref.com page).
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param exclude_timestamps Character vector of snapshot timestamps already tried.
#' @return A `https://web.archive.org/web/{timestamp}/{original_url}` string, or `NA` if none left.
#' @export
wayback_snapshot <- function(original_url, cfg, exclude_timestamps = character(0)) {
  cdx_url <- glue(
    "http://web.archive.org/cdx/search/cdx?url={URLencode(original_url, reserved = TRUE)}",
    "&output=json&filter=statuscode:200"
  )
  req <- request(cdx_url) |>
    req_user_agent(cfg$fetch$user_agent) |>
    req_retry(max_tries = cfg$fetch$max_retries)
  resp <- req_perform(req)
  rows <- jsonlite::fromJSON(httr2::resp_body_string(resp), simplifyVector = TRUE)
  if (is.null(rows) || nrow(rows) < 2) return(NA_character_)

  header <- rows[1, ]
  body <- as.data.frame(rows[-1, , drop = FALSE], stringsAsFactors = FALSE)
  names(body) <- header
  body <- body[!body$timestamp %in% exclude_timestamps, ]
  if (nrow(body) == 0) return(NA_character_)
  # Most-recent snapshot, not largest raw content length: a player's FBref
  # page only grows as seasons are added, so recency is the right proxy for
  # "most complete" -- length picked a pre-retirement snapshot for at least
  # one player (Harry Kane) that undercounted his career goals.
  best <- body[which.max(body$timestamp), ]

  glue("https://web.archive.org/web/{best$timestamp}/{original_url}")
}

#' `cache_fetch()`, but resolved through the Wayback Machine: the manifest
#' records the *original* FBref URL (provenance points at the source, the
#' Archive is only the transport), and the CDX lookup plus fetch both retry
#' with backoff on 429/503 -- the Archive throws those often under load.
#'
#' @param original_url The live FBref URL this snapshot stands in for.
#' @param dest Path (relative to project root) to write the raw response to.
#' @param source_name Short label matching a `sources:` entry in config/config.yaml.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param notes Optional free-text note for the manifest row.
#' @return `dest`, invisibly.
#' @export
cache_fetch_wayback <- function(original_url, dest, source_name, cfg, notes = NA_character_) {
  if (file_exists(dest)) {
    message(glue("[cached] {source_name}: {dest}"))
    return(invisible(dest))
  }

  tried_timestamps <- character(0)
  path <- NULL
  snapshot_url <- NA_character_

  # A handful of CDX entries turn out to 404 when actually fetched (a stale
  # index, not a real absence) -- fall through to the next-largest snapshot
  # rather than giving up after the first miss.
  for (candidate in seq_len(cfg$fetch$max_retries)) {
    snapshot_url <- NA_character_
    for (attempt in seq_len(cfg$fetch$max_retries)) {
      snapshot_url <- tryCatch(
        wayback_snapshot(original_url, cfg, exclude_timestamps = tried_timestamps),
        error = function(e) NA_character_
      )
      if (!is.na(snapshot_url)) break
      Sys.sleep(cfg$fetch$time_pause_seconds * attempt)
    }
    if (is.na(snapshot_url)) break
    tried_timestamps <- c(tried_timestamps, str_extract(snapshot_url, "(?<=/web/)\\d+"))

    path <- tryCatch(
      cache_fetch(snapshot_url, dest, source_name, cfg, notes = coalesce(notes, original_url)),
      error = function(e) NULL
    )
    if (!is.null(path)) break
  }
  if (is.null(path)) {
    stop(glue("No fetchable Wayback snapshot found for {original_url}"))
  }
  strip_wayback_toolbar(path)

  # The manifest row cache_fetch() wrote points at the archive.org URL;
  # overwrite it with the original FBref URL, which is what "provenance"
  # means here -- the Archive is transport, not source.
  manifest <- read_csv(cfg$paths$manifest, show_col_types = FALSE)
  is_new_row <- manifest$url == snapshot_url
  if (any(is_new_row)) {
    manifest$url[max(which(is_new_row))] <- original_url
    manifest$notes[max(which(is_new_row))] <- coalesce(notes, manifest$notes[max(which(is_new_row))])
    write_csv(manifest, cfg$paths$manifest)
  }

  invisible(path)
}

#' Live fetch through a headless, real Chrome (`chromote`), for the small,
#' confirmed subset of fbref.com pages the Wayback Machine never captured at
#' all (checked via `wayback_snapshot()` returning `NA` with no CDX rows under
#' any status code -- not a transient miss). fbref.com issues a Cloudflare
#' Managed Challenge on a plain `httr2` request; a real browser clears it.
#' One `ChromoteSession` is started lazily and reused across calls in a run
#' (starting Chrome per-call would be ~28x slower for no benefit).
#'
#' @param url The live fbref.com URL to fetch.
#' @param dest Path (relative to project root) to write the rendered HTML to.
#' @param source_name Short label matching a `sources:` entry in config/config.yaml.
#' @param cfg Parsed config list (from `load_config()` / config/config.yaml).
#' @param notes Optional free-text note for the manifest row.
#' @return `dest`, invisibly.
#' @export
cache_fetch_chromote <- function(url, dest, source_name, cfg, notes = NA_character_) {
  dir_create(path_dir(dest))

  if (file_exists(dest)) {
    message(glue("[cached] {source_name}: {dest}"))
    return(invisible(dest))
  }

  Sys.sleep(cfg$fetch$time_pause_seconds)

  # The session's underlying Chrome process may be hung (not dead) after a
  # failed navigate -- is_alive() alone won't catch that, so a failure forces
  # a fresh browser before retrying. A relaunch can also hit a transient
  # "Cannot find an available port" from a debugging-port race (observed
  # 2026-09-20, ~7/28 fetches) -- a short backoff and a couple more attempts
  # clears it; it's not a real content failure.
  html <- NULL
  last_error <- NULL
  for (attempt in seq_len(3)) {
    html <- tryCatch(
      fbref_chromote_fetch_html(fbref_chromote_session(fresh = attempt > 1), url),
      error = function(e) {
        last_error <<- e
        NULL
      }
    )
    if (!is.null(html)) break
    Sys.sleep(2 * attempt)
  }
  if (is.null(html)) stop(last_error)
  writeLines(html, dest)

  append_manifest_row(
    source_name = source_name,
    url = url,
    http_status = 200L,
    bytes = file_size(dest),
    notes = coalesce(notes, "direct fetch via chromote (no Wayback snapshot exists)"),
    manifest_path = cfg$paths$manifest
  )

  message(glue("[fetched-live] {source_name}: {url} -> {dest}"))
  invisible(dest)
}

#' Navigate `session` to `url` and return the rendered `<html>` as text, with
#' a hard timeout -- a hung navigate (rather than an error) is exactly the
#' failure mode that leaked orphaned Chrome processes before. fbref.com's
#' Cloudflare Managed Challenge takes a few seconds to clear even in a real
#' (non-headless) browser -- confirmed 2026-09-20 that reading the DOM
#' immediately after `loadEventFired` still shows the "Just a moment..."
#' interstitial; waiting resolves it.
#'
#' @param session A `ChromoteSession`.
#' @param url The URL to fetch.
#' @return The page's outer HTML, as a string.
fbref_chromote_fetch_html <- function(session, url) {
  session$Page$navigate(url, wait_ = TRUE, timeout_ = 30)
  session$Page$loadEventFired(wait_ = TRUE, timeout_ = 30)
  Sys.sleep(6)
  session$DOM$getOuterHTML(session$DOM$getDocument()$root$nodeId)$outerHTML
}

#' Lazily starts (and reuses) one real (non-headless) Chrome session for
#' `cache_fetch_chromote()`. Headless Chrome (old and new `--headless` mode
#' alike, even with a patched `navigator.webdriver`) never clears fbref.com's
#' Cloudflare Managed Challenge -- confirmed 2026-09-20. A real Chrome window
#' does. Non-headless mode also refuses to open a remote-debugging port
#' against the default profile (a Chrome security restriction), so it needs
#' its own `--user-data-dir`. That profile is **persistent, not per-run**
#' (`.chrome-profile/` at the project root, gitignored): once Cloudflare
#' issues a clearance cookie into it, every later fetch -- in this run or a
#' future one -- reuses that cookie instead of re-triggering the challenge.
#' A fresh throwaway profile per launch (the original design) meant every
#' single fetch re-triggered the challenge from scratch, which is what forced
#' manual interaction on every browser window. `--no-sandbox` is required
#' under this project's dev container (no user namespaces); real sandboxing
#' isn't a concern here since every page fetched is a known, read-only
#' fbref.com match report.
#'
#' @param fresh Kill any existing session/browser and relaunch (reusing the
#'   same persistent profile) -- used after a failed fetch, since a hung
#'   Chrome process may still report `is_alive() == TRUE`.
#' @return A `ChromoteSession`.
fbref_chromote_session <- local({
  session <- NULL
  user_data_dir <- ".chrome-profile"
  function(fresh = FALSE) {
    if (fresh && !is.null(session)) {
      # `Chromote` has no `$stop()` method -- only `$close()` -- so this used
      # to silently no-op (wrapped in try()), leaving the old Chrome process
      # alive and its `SingletonLock` in `user_data_dir` orphaned, which made
      # every subsequent relaunch against the same profile fail with
      # "Cannot find an available port" (confirmed 2026-09-20).
      try(session$parent$close(wait = 5), silent = TRUE)
      unlink(file.path(user_data_dir, c("SingletonLock", "SingletonCookie", "SingletonSocket")))
      session <<- NULL
    }
    if (is.null(session) || !session$is_alive()) {
      dir_create(user_data_dir)
      # chromote always prepends its own --headless flag (old mode) with no
      # way to omit it via public args -- overridden here for the lifetime
      # of the browser launch only.
      orig_headless <- chromote:::chrome_headless_mode
      assignInNamespace("chrome_headless_mode", function() character(0), ns = "chromote")
      on.exit(assignInNamespace("chrome_headless_mode", orig_headless, ns = "chromote"))
      browser <- chromote::Chrome$new(args = c(
        chromote::get_chrome_args(), "--no-sandbox",
        paste0("--user-data-dir=", user_data_dir), "--window-size=1280,900",
        "--no-first-run", "--no-default-browser-check"
      ))
      session <<- chromote::Chromote$new(browser = browser)$new_session()
    }
    session
  }
})

#' Strip the Wayback Machine's injected toolbar (`#wm-ipp-base` and its
#' `<script>`/`<style>` siblings) out of a cached snapshot in place, so
#' downstream `read_html()` calls see the same markup the original page had.
#'
#' @param path Path to a cached HTML file (as written by `cache_fetch_wayback()`).
#' @return `path`, invisibly.
strip_wayback_toolbar <- function(path) {
  h <- read_html(path)
  xml2::xml_remove(html_elements(h, "#wm-ipp-base, script[src*='web-static.archive.org']"))
  writeLines(as.character(h), path)
  invisible(path)
}

#' Load config/config.yaml as a nested list.
#'
#' @param path Output file path.
#' @return See description above.
#' @export
load_config <- function(path = "config/config.yaml") {
  yaml::read_yaml(path)
}
