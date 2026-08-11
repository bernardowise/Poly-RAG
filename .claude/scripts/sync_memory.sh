#!/usr/bin/env bash
# Bidirectional union sync between project memory_mirror and Claude's internal memory store.
# Union semantics: files are only added/updated, never deleted from either side.

INTERNAL="/home/codespace/.claude/projects/-workspaces-Poly-RAG/memory"
MIRROR="/workspaces/Poly-RAG/.claude/claude_docs/memory_mirror"

mkdir -p "$INTERNAL" "$MIRROR"

# Internal → mirror (new/updated files from Claude's memory)
rsync -a --update "$INTERNAL/" "$MIRROR/"

# Mirror → internal (new/updated files committed to the repo)
rsync -a --update "$MIRROR/" "$INTERNAL/"
