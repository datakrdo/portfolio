for (lib in .libPaths()) {
  for (pkg in list.dirs(lib, recursive = FALSE, full.names = FALSE)) {
    rds_path <- file.path(lib, pkg, "Meta", "package.rds")
    if (!file.exists(rds_path)) next

    meta <- readRDS(rds_path)
    repo <- meta$DESCRIPTION["Repository"]
    if (is.na(repo) || repo != "RSPM") next

    meta$DESCRIPTION["Repository"] <- "CRAN"
    saveRDS(meta, rds_path)

    desc_path <- file.path(lib, pkg, "DESCRIPTION")
    lines <- readLines(desc_path)
    lines <- sub("^Repository: RSPM$", "Repository: CRAN", lines)
    writeLines(lines, desc_path)
  }
}
