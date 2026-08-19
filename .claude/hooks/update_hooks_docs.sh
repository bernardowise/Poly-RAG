#!/usr/bin/env bash
# Parse .claude/settings.json and regenerate the "Active Hooks" section of claude_docs/hooks.md
# Preserves hand-written intro, philosophy, and future candidates sections.

set -e

SETTINGS="/workspaces/Poly-RAG/.claude/settings.json"
DOCS="/workspaces/Poly-RAG/.claude/claude_docs/hooks.md"

if [[ ! -f "$SETTINGS" ]]; then
  echo "Error: $SETTINGS not found" >&2
  exit 1
fi

if [[ ! -f "$DOCS" ]]; then
  echo "Error: $DOCS not found" >&2
  exit 1
fi

# Extract preamble (up to and including "## Active Hooks")
PREAMBLE=$(sed -n '1,/^## Active Hooks$/p' "$DOCS")

# Extract philosophy section (from "## Hook Design Philosophy" to end)
PHILOSOPHY=$(sed -n '/^## Hook Design Philosophy$/,$p' "$DOCS")

# Generate Active Hooks section from settings.json using jq and bash
generate_hooks() {
  local settings_json="$1"

  # Use jq to extract hook data. Two matcher shapes exist in this repo:
  # FileChanged entries use "matchers" (array, our own convention); PreToolUse
  # entries use "matcher" (single string, Claude Code's standard field for
  # that event). Normalize both to an array before rendering so one template
  # covers both -- without this, PreToolUse entries (matchers == null) crash
  # the whole regeneration and truncate hooks.md (see incident 2026-08-19).
  jq -r '.hooks | to_entries[] |
    .key as $event |
    .value[] |
    ((.matchers // [.matcher]) | map(select(. != null))) as $matchers |
    .hooks[0] as $handler |
    "### \($event): \($matchers[0] // "*")\n\n**Event:** `\($event)`\n**Trigger:** \($matchers | join(", "))\n**Handler:** Shell command\n**Script:** `\($handler.command)`\n\n**Purpose:**\n(Add description here)\n"
  ' "$settings_json"
}

# Reconstruct the full file
{
  echo "$PREAMBLE"
  echo ""
  generate_hooks "$SETTINGS"
  echo ""
  echo "$PHILOSOPHY"
} > "$DOCS"

echo "✓ hooks.md updated from settings.json"
