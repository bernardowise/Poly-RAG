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


