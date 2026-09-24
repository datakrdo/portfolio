# Deploys the Shiny dashboard to shinyapps.io as a self-contained bundle:
# app/, the processed parquet data, config, and the docs/*.md files the
# "Data & limitations" tab reads live. Never bundles data/raw (497 MB, and
# not needed -- the app only reads data/processed/*.parquet).
#
# One-time setup before running this:
#   rsconnect::setAccountInfo(name = ..., token = ..., secret = ...)
#   remotes::install_github("datakrdo/portfolio", subdir = "shearer")

# CI installs packages from the RSPM binary mirror under the repo name
# "RSPM" (via setup-r's use-public-rspm); rsconnect needs that name mapped
# to a real URL when it builds the manifest, or the server-side rebuild
# tries to fetch from the literal string "RSPM" and fails.
if (nzchar(Sys.getenv("RSPM"))) {
  options(repos = c(getOption("repos"), RSPM = Sys.getenv("RSPM")))
}

# app.R falls back to ROOT = "." when ../data/processed doesn't exist, so
# app.R is placed at the bundle root (not nested under app/) with data/,
# config/, docs/ as siblings -- matching that fallback layout.
bundle <- file.path(tempdir(), "shearer-deploy")
unlink(bundle, recursive = TRUE)
dir.create(bundle)

fs::file_copy("app/app.R", bundle)
fs::dir_copy("app/www", file.path(bundle, "www"))
fs::dir_copy("data/processed", file.path(bundle, "data/processed"))
fs::dir_copy("config", file.path(bundle, "config"))
fs::dir_create(file.path(bundle, "docs"))
fs::file_copy(Sys.glob("docs/*.md"), file.path(bundle, "docs"))

rsconnect::deployApp(
  appDir = bundle,
  appName = "shearer-260",
  forceUpdate = TRUE
)
