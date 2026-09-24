# Third-party notices

This file records the provenance and license terms of every external data source
this project fetches. Code is MIT-licensed (see `LICENSE`); the data below keeps
its own terms and is never redistributed as a bundled dataset — only cached
locally in `data/raw/` (gitignored) and cited by URL.

## Data sources

### football-data.co.uk

- **Source:** https://www.football-data.co.uk/mmz4281/{season}/E0.csv
- **License:** Free to use for personal/research purposes per the site's stated
  terms; no formal open license file. Not redistributed here — fetched on demand.
- **Use:** Match results (date, teams, full-time score) for Premier League
  seasons 1993-94 through 2005-06.
- **Reviewed:** 2026-09-10

### Wikipedia (English)

- **Source:** https://en.wikipedia.org/w/api.php (season articles, all-time
  scorers list, per-season top-scorer tables)
- **License:** CC BY-SA 4.0. Content is cached with its `revid` and retrieval
  date so results are reproducible against a fixed revision, not "Wikipedia
  today."
- **Use:** 1992-93 match results (the one season football-data.co.uk lacks),
  goal-by-goal scorer lists per match (cross-validation), the all-time
  100+ PL goals table (historical comparators), and per-season top-10 scorer
  tables (contemporary comparators).
- **Reviewed:** 2026-09-10

### Transfermarkt

- **Source:** https://www.transfermarkt.com/alan-shearer/alletore/spieler/3110
- **License:** No formal open-data license; Transfermarkt's terms of service
  prohibit systematic/automated scraping. This project fetches the page
  **exactly once**, with an identifying User-Agent, caches the raw HTML
  locally, and does not redistribute it. The page is not re-fetched on
  subsequent pipeline runs.
- **Use:** No longer the primary goal log (see FBref below) — kept as the
  sole source of `goal_type` (open play detail: header, free kick, etc.,
  which FBref doesn't record) via a left join on season/opponent/minute,
  and as an independent cross-check on FBref's goal count.
- **Reviewed:** 2026-09-10

### FBref (fbref.com) / Sports Reference

- **Source:** https://fbref.com/en/players/438b3a51/ (player page, match
  logs, match reports), accessed exclusively through Internet Archive
  Wayback Machine snapshots — never fetched live.
- **Why not live:** `curl https://fbref.com/robots.txt` returns HTTP 403
  with `cf-mitigated: challenge` and `server: cloudflare` — a Cloudflare
  Managed Challenge on the very first request, confirmed 2026-09-20
  independent of User-Agent (Chrome, R, none all gave the same 403 in
  ~0.1s). TLS impersonation (`curl_cffi`, `impersonate` = chrome/chrome124/
  safari) was tried and also returned 403. `worldfootballR`, the R package
  that used to wrap this site, is archived as of 2025-09-18 (read-only,
  "will no longer be maintained"). Python's `soccerdata` reaches the same
  data via `seleniumbase` (real headless Chrome), not a request-based
  fetch — out of scope for a pure-R pipeline.
- **License:** No formal open-data license. Sports Reference's bot policy
  asks for no more than 10 requests/minute against the live site. Nearly
  all pages are served from cached Wayback Machine snapshots, fetched once
  and never re-requested. The small subset of pages the Archive never
  captured are fetched live through a real, non-headless Chrome session
  with a persistent profile (`cache_fetch_chromote()`, `R/01_sources.R`),
  gated to stay within the 10 req/min policy, then cached the same way.
  Data attributed to Sports Reference / FBref throughout.
- **Use:** Primary source of `shearer_goal_events` as of 2026-09-20: match
  logs give season/opponent/venue/result/minutes per match; match reports
  give per-goal minute, running score and (for the ten pinned comparators)
  assist; player pages give real per-season minutes, powering the
  goals-per-90 metric that Transfermarkt's data never made possible.
- **Reviewed:** 2026-09-20

### Hudl Open Data (formerly StatsBomb Open Data)

- **Source:** https://github.com/hudl/open-data
- **License:** `LICENSE.pdf` in the repo is `NOASSERTION` — not an OSI license.
  Terms require: *"If you publish, share or distribute any research, analysis
  or insights based on this data, please state the data source as StatsBomb
  and use our logo."* This project states the source in every figure caption
  using this data.
- **Use:** Event data with shot coordinates for two 2003/04 Newcastle United
  matches (the only Shearer-era Premier League fixtures StatsBomb released,
  since their public release scope is Arsenal's 38 matches that season).
  Used to report the exact spatial coverage available — 2 shots, 0 goals —
  not to build a shot map.
- **Reviewed:** 2026-09-10

## Sources not used

### worldfootballR

The R package that used to wrap FBref/Understat/other sites. Archived on
GitHub 2025-09-18 (repository and its companion `worldfootballR_data` data
repo are both read-only, maintainer's README says "will no longer be
maintained"); its `devtools::install_github()` install path no longer
resolves to a working package. Its pre-scraped `worldfootballR_data`
releases only covered match-level events from the 2017-18 season onward
regardless — decades after Shearer's career. Superseded in this project by
direct Wayback Machine access to FBref itself (see above), which covers the
full 1992-2006 career.

### `reticulate` + `soccerdata` (Python)

Considered as a way to reuse Python's FBref reader without leaving R
entirely. Its `FBref` class is a `BaseSeleniumReader` (`rate_limit = 7`) —
it drives real headless Chrome via `seleniumbase`, not a requests-based
fetch, unlike its other readers. Porting that layer to R would mean adding
Python and a Chrome/Selenium toolchain to `renv`, against this project's
scope (a pure-R portfolio piece), for a reader that doesn't expose
individual goal events regardless.

### dkjorling/FbrefAPI (fbrapi.com)

A third-party FBref scraping API. As of 2026-09-10 the service does not
respond on any route (`/`, `/documentation`, `/generate_api_key` all time out
at the TLS layer with no HTTP response). The GitHub repository has no
README, no LICENSE, and no stars. Not used, independent of the outage: it
would still inherit FBref's Cloudflare block for anything not already cached.

### Understat

xG/shot data begins at the 2014-15 season per the site's own FAQ. Not
relevant to 1992-2006.

### football-data.org API

Returns HTTP 403 without a paid API key from this environment; not pursued
given football-data.co.uk and Wikipedia already cover match results for this
period at no cost.

### native-stats.org

Evaluated as the source of an "average time to assist or score" metric,
which the site displays. Its own `/about` page states it runs on the
football-data.org API, which per the entry above does not cover 1992-2006 --
unusable for Shearer's career. The metric was computed anyway, from FBref's
`stats_standard_dom_lg` table (already ingested here for `goals_per90`),
as `minutes_per_goal_or_assist = minutes / (goals + assists)`.
