# Godson Kurishinkal Antony

Data and analytics engineer in Dubai. I build data pipelines and analytics models for retail supply chain.

- Site: https://godsonkurishinkal.github.io/portfolio/
- LinkedIn: https://linkedin.com/in/godsonkurishinkal
- Email: godson.kurishinkal@gmail.com

## What is in this repository

| Path | What it is |
|---|---|
| `index.html`, `resume.html`, `404.html` | The site's home page, résumé page and error page |
| `projects/` | Five write-ups of systems I built at work. The work code is not public, so these pages show simplified snippets |
| `enterprise-data-platform/` | ETL framework components: a public Python library with extractors, schema enforcement, loaders and data-quality detectors. See its [README](enterprise-data-platform/README.md) for what is tested and what is not |
| `assets/` | Stylesheets and images |
| `scripts/` | `bump-css-version.sh`, which stamps stylesheet links with a content hash |
| `.github/workflows/` | CI: HTML validation, link check and stylesheet stamps (`site`), pytest (`tests`) |

## Write-ups

- [Bronze/Silver/Gold lakehouse on Parquet](projects/medallion-lakehouse.html)
- [Job scheduling with install-time validation](projects/pipeline-orchestration.html)
- [Three-tier data-quality checks](projects/anomaly-detection.html)
- [Demand forecasting by demand pattern](projects/demand-forecasting.html)
- [Browser automation for systems without APIs](projects/rpa-bot-framework.html)
- [ETL framework components (public code)](enterprise-data-platform/)

Figures on the write-up pages are my own measurements of systems at work. The underlying data is not public.

## Working on the site

The site is plain HTML and CSS, served by GitHub Pages from `main`.

```bash
# validate the pages
npx --yes html-validate@9.7.1 index.html resume.html 404.html 'projects/*.html' enterprise-data-platform/index.html

# after changing assets/css/*.css
bash scripts/bump-css-version.sh
```

Changes go through a pull request; see [CLAUDE.md](CLAUDE.md) for the working rules and [CHANGELOG.md](CHANGELOG.md) for what changed.

## Licence

MIT. See [LICENSE](LICENSE).
