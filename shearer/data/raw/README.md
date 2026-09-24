# data/raw/

Gitignored on purpose. Every file here is either a direct download of a
public dataset or a single cached fetch of a page whose terms of service
forbid redistribution (Transfermarkt).

Run `Rscript make.R` (or `targets::tar_make()`) from a clean checkout to
repopulate this directory. Nothing here is committed; `source_manifest.csv`
is the only file in this directory tracked in git — it's the audit trail of
what was fetched, from where, and when.
