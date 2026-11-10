# Working rules for this repository

These apply to anyone, human or AI, changing this repo.

## Content
- No employer material: no data, code, schemas, screenshots, table names, vendor names or internal identifiers from any employer. Examples use synthetic or openly licensed data.
- Never invent a number, result, testimonial or credential. A figure is either reproducible (give the command that regenerates it) or an account of production work (label it, with definition and baseline).
- Claim only what the code here demonstrates. Certifications appear once passed; work in progress is labelled as such.
- Check and record the licence of every dataset, font, icon, image and library before adding it.
- No secrets in git, logs or PR text. Use GitHub Actions secrets and a git-ignored `.env`.

## Writing and design
- Write the way an engineer writes: plain, honest, concise. Say what the thing is and what was done.
- No slogans, no sales language, no decorative marks. Polished and presentable, not embellished.

## Workflow
- `main` is protected and deploys the live site. Work on a branch, open a pull request, and let the repository owner merge. Do not merge or approve your own PR, force-push, or disable checks.
- One unit of work per branch. Conventional commit messages. Small PRs that say what changed and how it was verified.
- Record notable changes in `CHANGELOG.md`.
- Working notes and plans are kept outside this repository; do not add them here.

## Checks
CI runs on every pull request (`.github/workflows/`):
- `site`: HTML validation, site consistency (`scripts/check_site.py`), stylesheet version stamps, internal links.
- `tests`: pytest for `enterprise-data-platform/`.

Run locally before pushing:

```bash
npx --yes html-validate@9.7.1 index.html resume.html 404.html 'projects/*.html' enterprise-data-platform/index.html
python3 scripts/check_site.py      # after any change to an HTML page
bash scripts/bump-css-version.sh   # after any change to assets/css/*.css
```

`scripts/check_site.py` (Python 3, standard library only) fails when the pages drift apart. The header, skip link and footer are copied by hand into all nine pages, so it checks that they are identical once paths are resolved; that `aria-current` marks the right header link; that each case study has the right "Case study N of 6" line and a pager matching the fixed reading order; that every page has one `h1`; that nothing loads from another host; that no `reveal` class is left; and that every internal `href` and `src` resolves. When a case study is added, renamed or reordered, update the `CASE_STUDIES` list at the top of the script.

## Environment
Linux first. Pin versions and commit lock files for anything new.
