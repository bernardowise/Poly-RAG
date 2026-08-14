resource "aws_cloudwatch_event_rule" "ingest_polymarket_schedule" {
  name                = "poly-rag-ingest-polymarket-schedule"
  description         = "Trigger poly-rag-ingest-polymarket every 12h (00:00 and 12:00 UTC)"
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

resource "aws_cloudwatch_event_rule" "ingest_news_schedule" {
  name                = "poly-rag-ingest-news-schedule"
  description         = "Trigger poly-rag-ingest-news every 12h (00:00 and 12:00 UTC)"
  schedule_expression = "cron(0 0,12 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "ingest_news_target" {
  rule = aws_cloudwatch_event_rule.ingest_news_schedule.name
  arn  = aws_lambda_function.ingest_news.arn
}

resource "aws_lambda_permission" "allow_eventbridge_news" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_news.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingest_news_schedule.arn
}

resource "aws_cloudwatch_event_rule" "ingest_bluesky_schedule" {
  name                = "poly-rag-ingest-bluesky-schedule"
  description         = "Trigger poly-rag-ingest-bluesky every 12h (00:00 and 12:00 UTC)"
  schedule_expression = "cron(0 0,12 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "ingest_bluesky_target" {
  rule = aws_cloudwatch_event_rule.ingest_bluesky_schedule.name
  arn  = aws_lambda_function.ingest_bluesky.arn
}

resource "aws_lambda_permission" "allow_eventbridge_bluesky" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_bluesky.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingest_bluesky_schedule.arn
}
