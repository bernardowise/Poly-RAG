---
title: Poly-RAG
emoji: 📊
colorFrom: blue
colorTo: purple
sdk: gradio
sdk_version: 6.26.0
app_file: app.py
pinned: false
---

# Poly-RAG -- retrieval evaluation UI

Internal instrument for evaluating retrieval + synthesis quality over the
Poly-RAG corpus (token usage, latency, cost per turn, in-line LLM-judge
scoring, per-turn logging to S3). Not a consumer chatbot -- every model and
context parameter is exposed on purpose.

This Space is generated from the `gradio_app/` + `retrieval/` directories of
the main project repo (https://github.com/bernardowise/Poly-RAG) by a GitHub
Action on every push to `main`. Do not edit files here directly -- changes
are overwritten on the next sync. Edit the source in the repo instead.
