#!/usr/bin/env python3
"""Consistency checks for the site's nine hand-written pages.

There is no build step and no shared template, so the header, footer and
reading order are copied into every page by hand. This script fails when
those copies drift apart.

Checks:
  1. Skip link, header and footer are identical on every page once link
     targets are resolved (pages sit at different depths, and 404.html uses
     root-absolute /portfolio/ paths).
  2. aria-current="page" in the header is on "Case studies" on the six
     case-study pages, on "Résumé" on the résumé, and absent on home and 404.
  3. Each case-study page says "Case study N of 6 · <label>" and its pager
     points at the right previous and next page with the right titles.
  4. Every page has exactly one <h1>.
  5. No external stylesheet, script, font or other sub-resource.
  6. No "reveal" class is left.
  7. Every internal href and src resolves to a file in the repository.

Standard library only. Run from anywhere:  python3 scripts/check_site.py
Exit code 0 when everything passes, 1 otherwise.
"""

from __future__ import annotations

import posixpath
import re
import sys
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parent.parent
PUBLISHED_PREFIX = "/portfolio/"

# The fixed reading order: (page, label, title).
CASE_STUDIES = [
    ("projects/medallion-lakehouse.html", "Data pipeline",
     "Bronze/Silver/Gold lakehouse on Parquet"),
    ("projects/pipeline-orchestration.html", "Scheduling",
     "Job scheduling with install-time validation"),
    ("projects/anomaly-detection.html", "Data quality",
     "Three-tier data-quality checks"),
    ("projects/demand-forecasting.html", "Forecasting",
     "Demand forecasting by demand pattern"),
    ("projects/rpa-bot-framework.html", "Automation",
     "Browser automation for systems without APIs"),
    ("enterprise-data-platform/index.html", "Public code",
     "ETL framework components (public code)"),
]
RESUME = "resume.html"
PAGES = ["index.html", RESUME, "404.html"] + [c[0] for c in CASE_STUDIES]
TOTAL = len(CASE_STUDIES)

NAV_LABELS = ["Case studies", "Résumé", "Contact"]
CURRENT = {page: "Case studies" for page, _, _ in CASE_STUDIES}
CURRENT[RESUME] = "Résumé"

VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link",
        "meta", "source", "track", "wbr"}
# Attributes that make the browser fetch something, per element.
FETCHING = {
    "script": ["src"], "img": ["src", "srcset"], "source": ["src", "srcset"],
    "iframe": ["src"], "video": ["src", "poster"], "audio": ["src"],
    "embed": ["src"], "object": ["data"], "track": ["src"],
}
FETCHING_LINK_RELS = {"stylesheet", "preload", "prefetch", "preconnect",
                      "dns-prefetch", "modulepreload", "icon",
                      "apple-touch-icon", "manifest"}
FONT_HOSTS = ("fonts.googleapis.com", "fonts.gstatic.com")


class Node:
    def __init__(self, tag, attrs, parent=None):
        self.tag = tag
        self.attrs = dict(attrs)
        self.parent = parent
        self.children = []  # Node or str

    def classes(self):
        return (self.attrs.get("class") or "").split()

    def walk(self):
        yield self
        for child in self.children:
            if isinstance(child, Node):
                yield from child.walk()

    def find_all(self, tag=None, cls=None):
        return [n for n in self.walk()
                if (tag is None or n.tag == tag)
                and (cls is None or cls in n.classes())]

    def find(self, tag=None, cls=None):
        found = self.find_all(tag, cls)
        return found[0] if found else None

    def text(self, skip_hidden=False):
        parts = []
        for child in self.children:
            if isinstance(child, str):
                parts.append(child)
            elif not (skip_hidden and child.attrs.get("aria-hidden") == "true"):
                parts.append(child.text(skip_hidden))
        return "".join(parts)


def clean(text):
    return " ".join(text.split())


class TreeBuilder(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = Node("#root", [])
        self.cur = self.root

    def handle_starttag(self, tag, attrs):
        node = Node(tag, attrs, self.cur)
        self.cur.children.append(node)
        if tag not in VOID:
            self.cur = node

    def handle_startendtag(self, tag, attrs):
        self.cur.children.append(Node(tag, attrs, self.cur))

    def handle_endtag(self, tag):
        node = self.cur
        while node is not self.root and node.tag != tag:
            node = node.parent
        if node is not self.root:
            self.cur = node.parent

    def handle_data(self, data):
        self.cur.children.append(data)


def parse(source):
    builder = TreeBuilder()
    builder.feed(source)
    builder.close()
    return builder.root


def resolve(page, url):
    """Return (kind, target, fragment) for a link found on `page`.

    kind is "external", "other" (mailto:, data:, ...) or "internal"; for
    internal links `target` is the repository-relative path, with a
    directory resolved to its index.html.
    """
    url = url.strip()
    parts = urlsplit(url)
    if parts.scheme in ("http", "https") or url.startswith("//"):
        return "external", url, ""
    if parts.scheme:
        return "other", url, ""
    path = unquote(parts.path)
    if path.startswith("/"):
        if path == PUBLISHED_PREFIX.rstrip("/"):
            path = PUBLISHED_PREFIX
        if not path.startswith(PUBLISHED_PREFIX):
            return "internal", "<outside " + PUBLISHED_PREFIX + "> " + path, parts.fragment
        target = path[len(PUBLISHED_PREFIX):]
    elif path == "":
        target = page
    else:
        target = posixpath.join(posixpath.dirname(page), path)
    is_dir = target == "" or target.endswith("/")
    target = posixpath.normpath(target) if target else "."
    if target.startswith(".."):
        return "internal", "<outside the repository> " + url, parts.fragment
    if is_dir or (ROOT / target).is_dir():
        target = "index.html" if target == "." else target + "/index.html"
    return "internal", target, parts.fragment


def shell_signature(page, node):
    """Serialise a shell element so that copies on different pages compare
    equal: link targets are resolved and aria-current is left out (it is
    checked on its own)."""
    out = []

    def visit(n):
        attrs = []
        for name, value in sorted(n.attrs.items()):
            if name == "aria-current":
                continue
            if name in ("href", "src") and value is not None:
                kind, target, fragment = resolve(page, value)
                if kind == "internal":
                    value = target + ("#" + fragment if fragment else "")
                    if target == page and fragment and n.tag == "a" \
                            and "skip-link" in n.classes():
                        value = "#" + fragment
            attrs.append("%s=%r" % (name, value))
        out.append("<%s %s>" % (n.tag, " ".join(attrs)))
        for child in n.children:
            if isinstance(child, Node):
                visit(child)
            elif child.strip():
                out.append(clean(child))
        out.append("</%s>" % n.tag)

    visit(node)
    return "\n".join(out)


def first_difference(a, b):
    for line_a, line_b in zip(a.split("\n"), b.split("\n")):
        if line_a != line_b:
            return "expected %s, found %s" % (line_a, line_b)
    return "one copy has more markup than the other"


class Checker:
    def __init__(self):
        self.errors = []
        self.sources = {}
        self.trees = {}

    def fail(self, page, message):
        self.errors.append("%s: %s" % (page, message))

    def load(self):
        for page in PAGES:
            path = ROOT / page
            if not path.is_file():
                self.fail(page, "file is missing")
                continue
            self.sources[page] = path.read_text(encoding="utf-8")
            self.trees[page] = parse(self.sources[page])

    # 1 ---------------------------------------------------------------
    def check_shell(self):
        pieces = {
            "skip link": lambda t: t.find("a", "skip-link"),
            "header": lambda t: t.find("header", "site-header"),
            "footer": lambda t: t.find("footer", "site-footer"),
        }
        for name, finder in pieces.items():
            reference = None
            for page, tree in self.trees.items():
                node = finder(tree)
                if node is None:
                    self.fail(page, "%s is missing" % name)
                    continue
                signature = shell_signature(page, node)
                if reference is None:
                    reference = (page, signature)
                elif signature != reference[1]:
                    self.fail(page, "%s differs from %s: %s" % (
                        name, reference[0],
                        first_difference(reference[1], signature)))
        for page, tree in self.trees.items():
            body = tree.find("body")
            first = next((c for c in body.children if isinstance(c, Node)), None) if body else None
            if first is None or "skip-link" not in first.classes():
                self.fail(page, "the skip link is not the first element in <body>")

    # 2 ---------------------------------------------------------------
    def check_aria_current(self):
        for page, tree in self.trees.items():
            header = tree.find("header", "site-header")
            if header is None:
                continue
            links = [a for a in header.find_all("a") if "nav-brand" not in a.classes()]
            labels = [clean(a.text()) for a in links]
            if labels != NAV_LABELS:
                self.fail(page, "header links are %s, expected %s" % (labels, NAV_LABELS))
            marked = [(clean(a.text()), a.attrs["aria-current"])
                      for a in header.find_all("a") if "aria-current" in a.attrs]
            expected = [(CURRENT[page], "page")] if page in CURRENT else []
            if marked != expected:
                self.fail(page, "aria-current in the header is %s, expected %s" % (
                    marked or "absent", expected or "absent"))

    # 3 ---------------------------------------------------------------
    def check_reading_path(self):
        for index, (page, label, title) in enumerate(CASE_STUDIES):
            tree = self.trees.get(page)
            if tree is None:
                continue
            number = index + 1

            header = tree.find("header", "case-header")
            eyebrow = header.find("p", "case-category") if header else None
            expected = "Case study %d of %d · %s" % (number, TOTAL, label)
            found = clean(eyebrow.text()) if eyebrow else None
            if found != expected:
                self.fail(page, "eyebrow is %r, expected %r" % (found, expected))

            h1 = tree.find("h1")
            if h1 is not None and clean(h1.text()) != title:
                self.fail(page, "h1 is %r, expected %r" % (clean(h1.text()), title))

            back = header.find("a", "back-link") if header else None
            if back is None or resolve(page, back.attrs.get("href", ""))[1:] != ("index.html", "projects"):
                self.fail(page, "back link to index.html#projects is missing from the page header")

            pagers = tree.find_all("nav", "pager")
            if len(pagers) != 1:
                self.fail(page, "expected one <nav class=\"pager\">, found %d" % len(pagers))
                continue
            pager = pagers[0]
            if pager.attrs.get("aria-label") != "Case studies":
                self.fail(page, "pager aria-label is %r, expected 'Case studies'" % pager.attrs.get("aria-label"))

            if number < TOTAL:
                next_page, _, next_title = CASE_STUDIES[index + 1]
                next_label = "Next · Case study %d of %d" % (number + 1, TOTAL)
            else:
                next_page, next_title, next_label = RESUME, "Résumé", "Next"
            nxt = pager.find("a", "pager-next")
            if nxt is None:
                self.fail(page, "pager has no next link")
            else:
                target = resolve(page, nxt.attrs.get("href", ""))[1]
                if target != next_page:
                    self.fail(page, "pager next goes to %s, expected %s" % (target, next_page))
                lab = nxt.find("span", "pager-label")
                tit = nxt.find("span", "pager-title")
                if lab is None or clean(lab.text()) != next_label:
                    self.fail(page, "pager next label is %r, expected %r" % (
                        clean(lab.text()) if lab else None, next_label))
                if tit is None or clean(tit.text()) != next_title:
                    self.fail(page, "pager next title is %r, expected %r" % (
                        clean(tit.text()) if tit else None, next_title))

            links = [a for a in pager.find_all("a") if "pager-next" not in a.classes()]
            texts = [clean(a.text(skip_hidden=True)) for a in links]
            previous = [(a, t) for a, t in zip(links, texts) if t.startswith("Previous")]
            if index == 0:
                if previous:
                    self.fail(page, "the first case study must not have a previous link")
            else:
                prev_page, _, prev_title = CASE_STUDIES[index - 1]
                if len(previous) != 1:
                    self.fail(page, "expected one previous link in the pager, found %d" % len(previous))
                else:
                    anchor, text = previous[0]
                    target = resolve(page, anchor.attrs.get("href", ""))[1]
                    if target != prev_page:
                        self.fail(page, "pager previous goes to %s, expected %s" % (target, prev_page))
                    if text != "Previous: " + prev_title:
                        self.fail(page, "pager previous reads %r, expected %r" % (text, "Previous: " + prev_title))
            rest = [t for t in texts if not t.startswith("Previous")]
            if rest != ["All case studies", "Email"]:
                self.fail(page, "pager text links are %s, expected ['All case studies', 'Email']" % rest)

        # The same order wherever the six are listed.
        expected_list = [(p, t) for p, _, t in CASE_STUDIES]
        lists = {"index.html": ("ol", "projects-grid"), "404.html": ("ol", "nf-list")}
        for page, (tag, cls) in lists.items():
            tree = self.trees.get(page)
            if tree is None:
                continue
            holder = tree.find(tag, cls)
            if holder is None:
                self.fail(page, "case-study list <%s class=\"%s\"> is missing" % (tag, cls))
                continue
            found = [(resolve(page, a.attrs.get("href", ""))[1], clean(a.text(skip_hidden=True)))
                     for a in holder.find_all("a")]
            if found != expected_list:
                self.fail(page, "case-study list does not match the fixed order: %s" % found)

    # 4 ---------------------------------------------------------------
    def check_h1(self):
        for page, tree in self.trees.items():
            count = len(tree.find_all("h1"))
            if count != 1:
                self.fail(page, "has %d <h1> elements, expected exactly one" % count)

    # 5 ---------------------------------------------------------------
    def check_no_external_resources(self):
        for page, tree in self.trees.items():
            source = self.sources[page]
            for host in FONT_HOSTS:
                if host in source:
                    self.fail(page, "references %s" % host)
            for node in tree.walk():
                names = list(FETCHING.get(node.tag, []))
                if node.tag == "link":
                    rels = set((node.attrs.get("rel") or "").lower().split())
                    if rels & FETCHING_LINK_RELS:
                        names = ["href"]
                for name in names:
                    value = node.attrs.get(name)
                    if not value:
                        continue
                    for candidate in ([c.strip().split(" ")[0] for c in value.split(",")]
                                      if name == "srcset" else [value]):
                        if resolve(page, candidate)[0] == "external":
                            self.fail(page, "<%s %s> loads an external resource: %s" % (
                                node.tag, name, candidate))
                if node.tag == "style":
                    css = node.text()
                    for match in re.finditer(r"@import[^;]*|url\(\s*['\"]?(?:https?:)?//[^)]*\)", css):
                        if re.search(r"(?:https?:)?//", match.group(0)):
                            self.fail(page, "inline style loads an external resource: %s" % match.group(0))
        for sheet in sorted((ROOT / "assets" / "css").glob("*.css")):
            css = re.sub(r"/\*.*?\*/", "", sheet.read_text(encoding="utf-8"), flags=re.S)
            name = sheet.relative_to(ROOT).as_posix()
            for match in re.finditer(r"@import[^;]*;|url\(\s*['\"]?([^'\")]+)['\"]?\s*\)", css):
                ref = match.group(1)
                if ref is None or re.match(r"(?:https?:)?//", ref):
                    self.fail(name, "loads an external resource: %s" % match.group(0))
                elif not ref.startswith("data:"):
                    target = (sheet.parent / unquote(ref.split("#")[0].split("?")[0])).resolve()
                    if not target.is_file():
                        self.fail(name, "url(%s) does not resolve to a file" % ref)

    # 6 ---------------------------------------------------------------
    def check_no_reveal(self):
        for page, tree in self.trees.items():
            count = sum(1 for n in tree.walk() if "reveal" in n.classes())
            if count:
                self.fail(page, "%d element(s) still have the \"reveal\" class" % count)
            if re.search(r"\.reveal\b|classList\.add\(['\"]js['\"]\)", self.sources[page]):
                self.fail(page, "scroll-reveal style or script is still present")
        for sheet in sorted((ROOT / "assets" / "css").glob("*.css")):
            if re.search(r"\.reveal\b", sheet.read_text(encoding="utf-8")):
                self.fail(sheet.relative_to(ROOT).as_posix(), "still has a .reveal rule")

    # 7 ---------------------------------------------------------------
    def check_internal_links(self):
        ids = {}

        def ids_of(target):
            if target not in ids:
                if target in self.trees:
                    tree = self.trees[target]
                elif target.endswith(".html"):
                    tree = parse((ROOT / target).read_text(encoding="utf-8"))
                else:
                    tree = None
                ids[target] = None if tree is None else {
                    n.attrs["id"] for n in tree.walk() if n.attrs.get("id")}
            return ids[target]

        for page, tree in self.trees.items():
            for node in tree.walk():
                for name in ("href", "src"):
                    value = node.attrs.get(name)
                    if value is None:
                        continue
                    kind, target, fragment = resolve(page, value)
                    if kind != "internal":
                        continue
                    if not (ROOT / target).is_file():
                        self.fail(page, "%s=\"%s\" does not resolve to a file (%s)" % (name, value, target))
                        continue
                    if fragment and name == "href":
                        known = ids_of(target)
                        if known is not None and unquote(fragment) not in known:
                            self.fail(page, "href=\"%s\": no element with id \"%s\" in %s" % (value, fragment, target))

    def run(self):
        self.load()
        checks = [
            ("shell identical on all pages", self.check_shell),
            ("aria-current placement", self.check_aria_current),
            ("reading path and pagers", self.check_reading_path),
            ("one h1 per page", self.check_h1),
            ("no external resources", self.check_no_external_resources),
            ("no scroll-reveal left", self.check_no_reveal),
            ("internal links resolve", self.check_internal_links),
        ]
        failed = bool(self.errors)
        for message in self.errors:
            print("FAIL  " + message)
        for name, check in checks:
            before = len(self.errors)
            check()
            new = self.errors[before:]
            print("%s  %s" % ("FAIL" if new else "ok  ", name))
            for message in new:
                print("      " + message)
            failed = failed or bool(new)
        if failed:
            print("\n%d problem(s) found." % len(self.errors))
            return 1
        print("\nAll checks passed on %d pages." % len(self.trees))
        return 0


if __name__ == "__main__":
    sys.exit(Checker().run())
