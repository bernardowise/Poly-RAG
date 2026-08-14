data "archive_file" "ingest_polymarket" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/ingest_polymarket/handler.py"
  output_path = "${path.module}/build/ingest_polymarket.zip"
}

resource "aws_lambda_function" "ingest_polymarket" {
  function_name = "poly-rag-ingest-polymarket"
  role          = aws_iam_role.ingest_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  timeout       = 60
  memory_size   = 256

  filename         = data.archive_file.ingest_polymarket.output_path
  source_code_hash = data.archive_file.ingest_polymarket.output_base64sha256

  environment {
    variables = {
      S3_BUCKET          = aws_s3_bucket.poly_rag_data.bucket
      METRICS_TABLE      = aws_dynamodb_table.architecture_metrics.name
      BEDROCK_MODEL_ID   = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
      USE_LLM_ENRICHMENT = "true"
    }
  }
}

data "archive_file" "ingest_news" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/ingest_news/handler.py"
  output_path = "${path.module}/build/ingest_news.zip"
}

resource "aws_lambda_function" "ingest_news" {
  function_name = "poly-rag-ingest-news"
  role          = aws_iam_role.ingest_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  timeout       = 60
  memory_size   = 256

  filename         = data.archive_file.ingest_news.output_path
  source_code_hash = data.archive_file.ingest_news.output_base64sha256

  environment {
    variables = {
      S3_BUCKET          = aws_s3_bucket.poly_rag_data.bucket
      METRICS_TABLE      = aws_dynamodb_table.architecture_metrics.name
      BEDROCK_MODEL_ID   = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
      USE_LLM_ENRICHMENT = "true"
    }
  }
}

data "archive_file" "ingest_bluesky" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/ingest_bluesky/handler.py"
  output_path = "${path.module}/build/ingest_bluesky.zip"
}

resource "aws_lambda_function" "ingest_bluesky" {
  function_name = "poly-rag-ingest-bluesky"
  role          = aws_iam_role.ingest_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  timeout       = 60
  memory_size   = 256

  filename         = data.archive_file.ingest_bluesky.output_path
  source_code_hash = data.archive_file.ingest_bluesky.output_base64sha256

  environment {
    variables = {
      S3_BUCKET          = aws_s3_bucket.poly_rag_data.bucket
      METRICS_TABLE      = aws_dynamodb_table.architecture_metrics.name
      BEDROCK_MODEL_ID   = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
      USE_LLM_ENRICHMENT = "true"
      # BLUESKY_HANDLE and BLUESKY_APP_PASSWORD are set manually via CLI/console,
      # not tracked here -- secrets never belong in Terraform state or .tf files
      # committed to git. See .secrets (gitignored) for the actual values.
    }
  }

  lifecycle {
    ignore_changes = [environment[0].variables["BLUESKY_HANDLE"], environment[0].variables["BLUESKY_APP_PASSWORD"]]
  }
}
