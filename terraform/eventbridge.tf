# Strict ingestion chaining (2026-08-16, see tech_debt.md "Strict Ingestion
# Chaining"): only ingest_polymarket is triggered by a timer. It invokes
# ingest_news directly on completion, ingest_news invokes ingest_comments
# once its full cycle (not just one batch) is done, and ingest_comments
# invokes send_digest -- each stage only starts once the one before it has
# fully written its data. News/Comments depend on the registry
# ingest_polymarket writes (open markets, questions, comment_entity_type),
# so running them on independent timers risked reading a stale registry
# from the prior cycle -- confirmed in practice (2026-08-16): a market
# newly tracked by Polymarket had zero News coverage because News's timer
# fired before Polymarket had a chance to add it to the registry that cycle.
resource "aws_cloudwatch_event_rule" "ingest_polymarket_schedule" {
  name                = "poly-rag-ingest-polymarket-schedule"
  description         = "Trigger poly-rag-ingest-polymarket every 12h (00:00 and 12:00 UTC) -- sole entry point for the ingestion chain"
  schedule_expression = "cron(0 0,12 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "ingest_polymarket_target" {
  rule = aws_cloudwatch_event_rule.ingest_polymarket_schedule.name
  arn  = aws_lambda_function.ingest_polymarket.arn
}

resource "aws_lambda_permission" "allow_eventbridge_polymarket" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_polymarket.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingest_polymarket_schedule.arn
}
