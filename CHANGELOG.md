# Changelog

Notable changes to this repository. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Added
- One header on every page: a single 56px row with the name and three links ("Case studies", "Résumé", "Contact"). It marks the section you are in and fits a 320px phone without a menu button.
- A next step at the end of every page. Each case study ends with a link to the next one in a fixed order of six, plus "Previous", "All case studies" and "Email". The last case study leads to the résumé, and the résumé ends with an Email button.
- "Case study N of 6" above each case-study title, and a "← All case studies" link at the top.
- On wide screens the "On this page" list of a case study stays beside the text and marks the section being read. On smaller screens, and with JavaScript off, it is a plain list at the top.
- A "Contact" section on the home page with Email, LinkedIn and GitHub.
- `scripts/check_site.py`, run by the `site` workflow: the header, skip link and footer are the same on all nine pages, the case-study order and next/previous links are right, each page has one main heading, nothing loads from another host and every internal link resolves.
- `site` workflow: HTML validation, stylesheet version-stamp check and internal link check on every pull request.
- `CLAUDE.md` with the working rules for this repository.
- This changelog.

### Changed
- Layout rebuilt around three screen ranges (phone, tablet, laptop and wider) with one left edge per page, one spacing scale and a 1120px maximum width. No page scrolls sideways from 320px up.
- Fonts are served from this site (`assets/fonts`); no page loads anything from another host.
- Home page: the six case studies are a numbered list in reading order, followed by a "Start reading" button. The hero has two buttons, "Read the case studies" and "Résumé".
- Case studies: decision cards show the tag above the title and the trade-off on its own line; card grids are at most two columns; code blocks scroll inside their own box and can be reached by keyboard.
- Résumé page uses the shared header and footer, with "Download PDF" and "Print" in a row above the sheet. The printed résumé is unchanged at two A4 pages; the PDF was regenerated and now shows the current repository address.
- 404 page uses the shared header and footer and lists the six case studies in order.
- Links and buttons outside running text are at least 44px tall and at least 8px apart; links inside running text are underlined; keyboard focus shows the same outline everywhere, and a link reached by keyboard is scrolled clear of the fixed header.
- Repository renamed to `portfolio`; the site address is now `godsonkurishinkal.github.io/portfolio/`. All internal addresses updated.
- Dates removed from the decision records.
- Whole-site plain-language pass: home page, all six write-ups, résumé summary, repository README and the ETL component docs. Titles now describe what each thing is; the public code is presented as a component library.
- Home page shortened: results reduced to two before/after figures with a stated baseline; card metrics, the key-numbers row and the specialisations and tools sections removed.
- Write-ups state that work code is not public and snippets are simplified; figures without a definition or baseline removed.
- Home page copy rewritten in plain language: headline, intro, section headings and contact line.
- Home page order: case studies now come directly after the introduction.
- Résumé link opens in the same tab. Phone layout puts the portrait above the headline.
- Page content no longer depends on the page script to become visible.
- Case studies and platform docs describe operational detail in general terms; per-run records removed from the reliability document; `create_oracle_extractor` takes the ODBC driver name as an argument.
- Education shows Diploma level with current-term courses; planned certification is Databricks Data Engineer Associate; phone number removed from public pages; résumé PDF regenerated from the résumé page.
- `tests` workflow now runs on every pull request, so it can be a required check.
- `scripts/bump-css-version.sh` works with GNU sed as well as BSD sed.

### Removed
- Scroll-reveal: content no longer fades in while scrolling, and the script that drove it is gone.
- Header icons, tagline and per-section links; "Get in touch" in the hero; the "Related case studies" grids; the résumé toolbar; hover lift and shadows; unused stylesheet rules.
- `docs/performance-benchmarks.md`, `docs/reliability-metrics.md` and `docs/deployment-guide.md` from the ETL components: not reproducible from this repository, or describing scripts that are not in it.
- Closing call-to-action blocks on the write-ups.
- Static "All pipelines operational" footer text and decorative `//` marks.

### Fixed
- README project tree lists the orchestration case study; sitemap dates updated.
- `resume.html`: explicit `type` on the print button; redundant `role="main"` removed.
