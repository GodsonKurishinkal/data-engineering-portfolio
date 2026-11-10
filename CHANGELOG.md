# Changelog

Notable changes to this repository. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Added
- Section links in the top navigation on every page, and an "On this page" list on each case study.
- `site` workflow: HTML validation, stylesheet version-stamp check and internal link check on every pull request.
- `CLAUDE.md` with the working rules for this repository.
- This changelog.

### Changed
- Home page copy rewritten in plain language: headline, intro, section headings and contact line.
- Home page order: case studies now come directly after the introduction.
- Résumé link opens in the same tab. Phone layout puts the portrait above the headline.
- Page content no longer depends on the page script to become visible.
- Case studies and platform docs describe operational detail in general terms; per-run records removed from the reliability document; `create_oracle_extractor` takes the ODBC driver name as an argument.
- Education shows Diploma level with current-term courses; planned certification is Databricks Data Engineer Associate; phone number removed from public pages; résumé PDF regenerated from the résumé page.
- `tests` workflow now runs on every pull request, so it can be a required check.
- `scripts/bump-css-version.sh` works with GNU sed as well as BSD sed.

### Removed
- Static "All pipelines operational" footer text and decorative `//` marks.

### Fixed
- README project tree lists the orchestration case study; sitemap dates updated.
- `resume.html`: explicit `type` on the print button; redundant `role="main"` removed.
