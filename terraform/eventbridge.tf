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

resource "aws_cloudwatch_event_rule" "send_digest_schedule" {
  name        = "poly-rag-send-digest-schedule"
  description = "Trigger poly-rag-send-digest 5min after each 12h ingestion cycle (00:05 and 12:05 UTC)"
  # 5-minute offset gives the 3 ingestion Lambdas time to finish writing to S3
  schedule_expression = "cron(5 0,12 * * ? *)"
  state                = "ENABLED"
}

resource "aws_cloudwatch_event_target" "send_digest_target" {
  rule = aws_cloudwatch_event_rule.send_digest_schedule.name
  arn  = aws_lambda_function.send_digest.arn
}

resource "aws_lambda_permission" "allow_eventbridge_send_digest" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.send_digest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.send_digest_schedule.arn
}
