# Strict ingestion chaining (2026-08-16, see tech_debt.md "Strict Ingestion
# Chaining"): the chain has a real unattended-failure mode -- if a single
# News batch times out, its _batch<offset>.json never gets written, the
# merge never completes, and nothing downstream (Comments, digest) ever
# runs for that cycle, silently. This watchdog runs every 10 min, detects a
# stuck cycle (batches missing after RETRY_AFTER_MINUTES), and re-invokes
# ingest_news for just the missing offsets.

resource "aws_iam_role" "watchdog_role" {
  name               = "poly-rag-watchdog-role"
  description        = "Execution role for the Poly-RAG stuck-batch watchdog Lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "watchdog_permissions" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:us-east-1:369970405415:*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.poly_rag_data.arn, "${aws_s3_bucket.poly_rag_data.arn}/*"]
  }

  statement {
    # Scoped to ingest_news only, not "*" -- the watchdog's sole job is
    # retrying missing News batches.
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.ingest_news.arn]
  }
}

resource "aws_iam_role_policy" "watchdog_permissions" {
  name   = "poly-rag-watchdog-permissions"
  role   = aws_iam_role.watchdog_role.id
  policy = data.aws_iam_policy_document.watchdog_permissions.json
}

data "archive_file" "watchdog_ingest_news" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/watchdog_ingest_news/handler.py"
  output_path = "${path.module}/build/watchdog_ingest_news.zip"
}

resource "aws_lambda_function" "watchdog_ingest_news" {
  function_name = "poly-rag-watchdog-ingest-news"
  role          = aws_iam_role.watchdog_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  timeout       = 30
  memory_size   = 128

  filename         = data.archive_file.watchdog_ingest_news.output_path
  source_code_hash = data.archive_file.watchdog_ingest_news.output_base64sha256

  environment {
    variables = {
      S3_BUCKET        = aws_s3_bucket.poly_rag_data.bucket
      NEWS_LAMBDA_NAME = aws_lambda_function.ingest_news.function_name
    }
  }
}

resource "aws_cloudwatch_event_rule" "watchdog_schedule" {
  name                = "poly-rag-watchdog-schedule"
  description         = "Check every 10 min for a stuck News cycle and retry missing batches"
  schedule_expression = "rate(10 minutes)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "watchdog_target" {
  rule = aws_cloudwatch_event_rule.watchdog_schedule.name
  arn  = aws_lambda_function.watchdog_ingest_news.arn
}

resource "aws_lambda_permission" "allow_eventbridge_watchdog" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.watchdog_ingest_news.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.watchdog_schedule.arn
}
