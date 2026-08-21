## Overview

Hooks are event-triggered handlers that execute automatically at specific lifecycle moments. They enable automation, validation, and state synchronization without manual intervention.

**Configuration location:** `.claude/settings.json`

---

## Active Hooks

### PreToolUse: Bash

**Event:** `PreToolUse`
**Trigger:** Bash
**Handler:** Shell command
**Script:** `/workspaces/Poly-RAG/.claude/hooks/block_lambda_invoke.sh`

**Purpose:**
(Add description here)

### PreToolUse: Write

**Event:** `PreToolUse`
**Trigger:** Write
**Handler:** Shell command
**Script:** `/workspaces/Poly-RAG/.claude/hooks/block_new_repo_files.sh`

**Purpose:**
(Add description here)

### PreToolUse: Agent

**Event:** `PreToolUse`
**Trigger:** Agent
**Handler:** Shell command
**Script:** `/workspaces/Poly-RAG/.claude/hooks/block_agent_spawn.sh`

**Purpose:**
(Add description here)

### PreToolUse: AskUserQuestion

**Event:** `PreToolUse`
**Trigger:** AskUserQuestion
**Handler:** Shell command
**Script:** `/workspaces/Poly-RAG/.claude/hooks/block_ask_user_question.sh`

**Purpose:**
(Add description here)

### FileChanged: .claude/claude_docs/memory_mirror/**

**Event:** `FileChanged`
**Trigger:** .claude/claude_docs/memory_mirror/**, /home/codespace/.claude/projects/-workspaces-Poly-RAG/memory/**
**Handler:** Shell command
**Script:** `/workspaces/Poly-RAG/.claude/hooks/sync_memory.sh`

**Purpose:**
(Add description here)

### FileChanged: .claude/settings.json

**Event:** `FileChanged`
**Trigger:** .claude/settings.json
**Handler:** Shell command
**Script:** `/workspaces/Poly-RAG/.claude/hooks/update_hooks_docs.sh`

**Purpose:**
(Add description here)


## Hook Design Philosophy

- **Event-driven over polling:** Hooks fire on file change, not on session boundaries
- **Union semantics:** No deletions, only updates/additions, to preserve history
- **Minimal overhead:** Single rsync call per file change, not per tool execution
- **Transparency:** All hook scripts live in `.claude/hooks/` and are readable

---

## Future Hook Candidates

As the project grows, consider adding hooks for:

- **PostToolUse** — auto-format code after write, lint on completion (`PreToolUse` is
  already in use — see Active Hooks above)
- **UserPromptSubmit** — inject project context or validate prompts against rules
- **ConfigChange** — auto-validate `.claude/settings.json` syntax
- **SubagentStart** — initialize subagent-specific memory or logging
