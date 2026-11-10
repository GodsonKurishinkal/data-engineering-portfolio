# Changelog

Notable changes to this repository. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Added
- `site` workflow: HTML validation, stylesheet version-stamp check and internal link check on every pull request.
- `CLAUDE.md` with the working rules for this repository.
- This changelog.

### Changed
- Case studies and platform docs describe operational detail in general terms; per-run records removed from the reliability document; `create_oracle_extractor` takes the ODBC driver name as an argument.
- Education shows Diploma level with current-term courses; planned certification is Databricks Data Engineer Associate; phone number removed from public pages; résumé PDF regenerated from the résumé page.
- `tests` workflow now runs on every pull request, so it can be a required check.
- `scripts/bump-css-version.sh` works with GNU sed as well as BSD sed.

### Fixed
- `resume.html`: explicit `type` on the print button; redundant `role="main"` removed.
