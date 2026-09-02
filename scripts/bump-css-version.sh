#!/usr/bin/env bash
# Re-stamp every <link> to app.css / tokens.css with the stylesheet's own
# content hash.
#
# Why: GitHub Pages serves HTML and CSS with `Cache-Control: max-age=600`
# and caches them independently. Without a version in the URL, a visitor can
# hold a fresh index.html next to a 10-minute-old app.css — new markup, old
# stylesheet — and any newly added section renders as unstyled raw text.
# A content-hashed URL makes that pairing impossible.
#
# Run after ANY change to assets/css/*.css, before committing.

set -euo pipefail
cd "$(dirname "$0")/.."

app=$(shasum -a 256 assets/css/app.css    | cut -c1-8)
tok=$(shasum -a 256 assets/css/tokens.css | cut -c1-8)

files=(
  index.html resume.html 404.html
  projects/*.html
  enterprise-data-platform/index.html
)

for f in "${files[@]}"; do
  [ -f "$f" ] || continue
  # Strip any existing ?v=... then re-stamp, so the script is idempotent.
  sed -i '' -E \
    -e "s|(href=\"[^\"]*assets/css/app\.css)(\?v=[0-9a-f]+)?\"|\1?v=${app}\"|g" \
    -e "s|(href=\"[^\"]*assets/css/tokens\.css)(\?v=[0-9a-f]+)?\"|\1?v=${tok}\"|g" \
    "$f"
done

echo "app.css    v=${app}"
echo "tokens.css v=${tok}"
echo "Stamped ${#files[@]} entries."
