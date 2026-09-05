#!/usr/bin/env bash
# Build the flattened Hugging Face Space layout from the repo's gradio_app/
# and retrieval/ directories into $1 (a staging dir). Called by the
# sync-space GitHub Action; can also be run locally to preview what gets
# pushed.
#
# The Space runs /app.py at its root, so gradio_app/app.py is flattened to
# the root and its imports rewritten: the sys.path bootstrap is dropped and
# `from gradio_app.live_logging` becomes `from live_logging`. retrieval/
# stays a subdirectory (its imports are already root-relative). Nothing
# else from the repo (lambdas/, terraform/, .claude/, ...) is copied.
set -euo pipefail

OUT="${1:?usage: build.sh <staging-dir>}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

rm -rf "$OUT"
mkdir -p "$OUT/retrieval"

# app.py -> root, flattened imports
python3 - "$ROOT/gradio_app/app.py" "$OUT/app.py" <<'PY'
import sys
src = open(sys.argv[1]).read()
src = src.replace(
    "sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))\n\n",
    "",
)
src = src.replace(
    "from gradio_app.live_logging import log_turn, new_session_id",
    "from live_logging import log_turn, new_session_id",
)
open(sys.argv[2], "w").write(src)
PY

cp "$ROOT/gradio_app/live_logging.py" "$OUT/live_logging.py"
cp "$ROOT/retrieval/query.py"        "$OUT/retrieval/query.py"
cp "$ROOT/requirements.txt"          "$OUT/requirements.txt"
cp "$ROOT/.github/space/README.md"   "$OUT/README.md"
# retrieval/ is imported as a namespace package (no __init__.py in the repo);
# add an empty one in the Space layout so `from retrieval.query import ...`
# resolves the same way regardless of the runner's sys.path quirks.
: > "$OUT/retrieval/__init__.py"

python3 -m py_compile "$OUT/app.py" "$OUT/live_logging.py" "$OUT/retrieval/query.py"
find "$OUT" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
echo "staging built at $OUT:"
find "$OUT" -type f | sed "s|$OUT/||" | sort
