# ADR-006: Use RPA for Legacy System Extraction

## Status
**Accepted** (May 2024)

## Context

Four source systems carry data the platform needs. Two of them have no
programmatic interface of any kind:

- **WMS** — a Java-applet warehouse system. No API, no direct database access
  granted, export only through the UI.
- **POS sales portal** — a vendor reporting portal whose only output is a
  browser-triggered Excel download.

The existing process was a person, every morning, logging into four systems and
performing a specific click-and-export sequence in each — roughly four hours
before any analysis could begin, with silent breakage whenever someone
mis-clicked.

**Options on the table:**

1. Keep the manual export ritual indefinitely.
2. Get the vendors to expose APIs — quoted at 6+ months, no commitment.
3. Automate the clicks.

## Decision

Build a **shared RPA framework** — Selenium for browser-based systems, PyAutoGUI
for the Java applet — behind the same `BaseExtractor` interface every other
source uses ([ADR-005](005-config-driven-pipelines.md)).

The bots are treated as extractors, not as a separate category of automation:
they log in, extract, persist to Bronze, and clean up. Retry, screenshot-on-
failure, credential handling and alerting are inherited from the base class.

**Explicitly accepted principle: land raw, transform later.** A bot's only job is
to get the bytes into Bronze. No parsing, no cleaning, no business logic in the
bot — because bots are the most fragile component and the least pleasant place to
debug a transform.

## Consequences

**Easier:**
- Four systems extract unattended before 06:30, with no human in the loop.
- The manual morning ritual is eliminated, along with its silent error class.
- RPA sources look exactly like database sources to everything downstream.

**Harder:**
- **UI fragility is real and permanent.** A vendor redesign breaks a bot; this
  happened in production (December 2025, WMS login page, 45 minutes to fix).
  Mitigated with semantic selectors, fallback locators, and page validation
  before every action.
- **Credential handling** needs care — service accounts and environment
  variables, never inline.
- **Serialised execution** — the bots run on a shared Windows host, so one bot's
  flakiness can delay another.

## Alternatives Considered

| Alternative | Why rejected |
|-------------|--------------|
| **Wait for vendor APIs** | 6+ months quoted with no commitment, against a daily cost being paid now. |
| **Direct database access** | Requested and declined by the WMS vendor; would have voided support. |
| **Keep manual exports** | Four hours a day, indefinitely, with an error mode nobody could detect. |
| **Screen-scraping the rendered HTML only** | Insufficient — the WMS is a Java applet with no DOM to scrape. |

## Revisit If

The vendors ship APIs, or either system is replaced. RPA is the correct answer to
this constraint, not a preference — the moment the constraint lifts, this ADR
should be superseded.
