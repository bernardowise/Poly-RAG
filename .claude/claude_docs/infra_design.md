Feature	What it is
CLAUDE.md	Project instructions auto-loaded into context
Rules	.claude/rules/ — directory-scoped behavior/security rules
Skills	.claude/skills/ — reusable slash commands with frontmatter
Hooks	Shell/HTTP handlers for 30+ lifecycle events (PreToolUse, PostToolUse, SessionStart, etc.)
MCP Servers	Structured tool access via Model Context Protocol (local stdio, HTTP, WebSocket)
Agents / AGENTS.md	Subagent definitions — scoped file access, tool availability, spawnable
Memory	Auto-persisted context across sessions (.claude/memory/)
Plugins	Bundles of skills + hooks + MCP servers, distributable via GitHub/URL
Workflows	Multi-step parallel/sequential operations, bundled or custom
Worktrees	Git worktree isolation per task or subagent
Settings	Hierarchical config (user → project → workspace) in .claude/settings.json
Permission Modes	plan, acceptEdits, dontAsk, auto, bypassPermissions
Output Styles	Persistent response formatting rules

---

# Data Curation: Market Verticals

**Decision (2026-08-13):** Poly-RAG does not ingest all active Polymarket markets
indiscriminately. Markets and news are filtered/tagged at ingestion time into three
verticals, chosen for high volatility, abundant correlatable web text, and strict
resolution rules — properties that make them good RAG material versus noisy pop-culture
markets that rely on gossip and unstructured chisme.

**Architecture:** single pipeline, tagged data — not physically separated storage per
vertical. Markets/articles often span multiple verticals (e.g. an AI-regulation market is
both geopolitics and regulatory-tech), so a rigid split would force arbitrary
classification calls and duplicate filtering logic three times. Instead, every ingested
item carries a `verticals` array field (a market/article can belong to more than one),
and both Polymarket and news land in the same S3 partition scheme
(`s3://bucket/<source>/YYYY-MM-DD/HH.json`) regardless of vertical.

## The three verticals and their keyword filters

Filters match against Polymarket's `question`/`description` fields and news article
titles/descriptions. Case-insensitive substring match, OR'd within a vertical.

### 1. Macro / Central Banks
High correlation with financial news; moves on Fed decisions, CPI prints, etc.

Keywords: `Fed`, `Federal Reserve`, `interest rate`, `inflation`, `CPI`, `unemployment`,
`recession`, `GDP`, `Trump`, `Truth Social`

Note: Trump/Truth Social included here (and cross-tagged into geopolitics) because his
posts move markets directly and fast — tariffs, Fed picks, etc. — not just as a political
figure but as a de facto market-moving news source in his own right.

### 2. Geopolitics / Elections
High volatility from real-time news events — a tweet or diplomatic statement can move
prices sharply.

Keywords: `election`, `president`, `war`, `ceasefire`, `sanctions`, `tariff`, `NATO`,
`invasion`, `Trump`, `Putin`, `Xi`, `Ukraine`, `Taiwan`, `China`

### 3. Regulatory / Tech
Specialist niche — long, technical resolution rules (FDA trial status, antitrust case
progress) where a RAG's summarization value is highest.

Keywords: `FDA`, `antitrust`, `lawsuit`, `SEC`, `regulation`, `approval`, `ban`,
`AI regulation`, `Google`, `Apple`, `Meta`, `OpenAI`, `Anthropic`, `SpaceX`

## Out of scope

Markets that don't match any vertical (pop culture, sports, entertainment awards, etc.)
are not ingested. Reason: poor RAG material — resolution depends on gossip/subjective
criteria, and there's little structured, correlatable web text to retrieve against.

## News feed strategy

Multiple curated RSS feeds, roughly aligned per vertical, rather than one generic feed
filtered post-hoc. Confirmed working (2026-08-13, via curl with a browser User-Agent —
several feeds reject the default curl UA, e.g. CNBC returns 403):

| Feed | URL | Vertical(es) |
|---|---|---|
| BBC World | `feeds.bbci.co.uk/news/world/rss.xml` | Geopolitics |
| BBC Business | `feeds.bbci.co.uk/news/business/rss.xml` | Macro |
| CBC Business | `cbc.ca/webfeed/rss/rss-business` | Macro |
| CBC Top Stories | `cbc.ca/webfeed/rss/rss-topstories` | Geopolitics |
| NYT World | `rss.nytimes.com/services/xml/rss/nyt/World.xml` | Geopolitics |
| NYT Opinion | `rss.nytimes.com/services/xml/rss/nyt/Opinion.xml` | Macro + Geopolitics (editorial sentiment) |
| NYT Technology | `rss.nytimes.com/services/xml/rss/nyt/Technology.xml` | Regulatory/Tech |
| CNN Top Stories | `rss.cnn.com/rss/cnn_topstories.rss` | Geopolitics |
| CNN World | `rss.cnn.com/rss/cnn_world.rss` | Geopolitics |
| France 24 English | `france24.com/en/rss` | Geopolitics |

10 feeds total. Note the vertical weighting is heavy on geopolitics and light on
regulatory/tech (NYT Technology is the only dedicated source there) — worth revisiting
if regulatory-tech markets end up underserved by news matching in Day 4.

Note: CBC's RSS copyright notice states "FOR PERSONAL USE ONLY" — acceptable for this
personal learning project; would need review if this project were ever made public/commercial.

Latinus (Mexican outlet) was evaluated but has no accessible standard RSS feed
(`/rss` and `/feed/` variants either 404 or redirect to HTML, not XML) — excluded.
Spanish-language coverage remains a gap; candidates for later (El Financiero, Reforma)
not yet evaluated.

Each ingested article is tagged with its source vertical(s) at ingestion time (same
single-pipeline, tagged-data approach as Polymarket markets — see above).

