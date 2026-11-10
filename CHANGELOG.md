# Changelog

Notable changes to this repository. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Added
- `site` workflow: HTML validation, stylesheet version-stamp check and internal link check on every pull request.
- `CLAUDE.md` with the working rules for this repository.
- This changelog.

### Changed
- `tests` workflow now runs on every pull request, so it can be a required check.
- `scripts/bump-css-version.sh` works with GNU sed as well as BSD sed.

### Fixed
- `resume.html`: explicit `type` on the print button; redundant `role="main"` removed.
