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
  # 600s (vs 60s for the other ingestion Lambdas): the redesigned pipeline
  # paginates up to 500 candidates, runs up to ~25 sequential Bedrock batch
  # calls for verifiability classification, and does a read-modify-write S3
  # odds snapshot per tracked market -- the bootstrap cycle in particular
  # needs real headroom the first time the registry is empty.
  timeout     = 600
  memory_size = 256

  filename         = data.archive_file.ingest_polymarket.output_path
  source_code_hash = data.archive_file.ingest_polymarket.output_base64sha256

  environment {
    variables = {
      S3_BUCKET          = aws_s3_bucket.poly_rag_data.bucket
      METRICS_TABLE      = aws_dynamodb_table.architecture_metrics.name
      BEDROCK_MODEL_ID   = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
      USE_LLM_ENRICHMENT = "true"
      NEXT_LAMBDA_NAME   = aws_lambda_function.ingest_news.function_name
    }
  }
}

# ingest_news needs external dependencies (trafilatura, googlenewsdecoder --
# see tech_debt.md "News Source Redesign") that aren't preinstalled in the
# Lambda runtime like boto3 is, so its zip must include them. pip install
# runs into a staging dir before archive_file zips it -- triggered on every
# apply whose handler.py content changed (the hash keeps this from
# re-running needlessly).
resource "null_resource" "ingest_news_deps" {
  triggers = {
    handler_hash = filesha256("${path.module}/../lambdas/ingest_news/handler.py")
  }

  provisioner "local-exec" {
    command = <<-EOT
      rm -rf ${path.module}/build/ingest_news_pkg
      mkdir -p ${path.module}/build/ingest_news_pkg
      cp ${path.module}/../lambdas/ingest_news/handler.py ${path.module}/build/ingest_news_pkg/
      pip install -q --target ${path.module}/build/ingest_news_pkg trafilatura googlenewsdecoder
    EOT
  }
}

data "archive_file" "ingest_news" {
  type        = "zip"
  source_dir  = "${path.module}/build/ingest_news_pkg"
  output_path = "${path.module}/build/ingest_news.zip"
  depends_on  = [null_resource.ingest_news_deps]
}

resource "aws_lambda_function" "ingest_news" {
  function_name = "poly-rag-ingest-news"
  role          = aws_iam_role.ingest_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # 900s (Lambda's hard maximum): measured ~21s/market end-to-end (search +
  # decode + extract). Each invocation only processes one BATCH_SIZE=35
  # batch (~735s worst case) then self-invokes for the next batch (see
  # handler module docstring) -- 900s gives margin over the 735s estimate
  # for Bedrock enrichment + S3 I/O on top.
  timeout     = 900
  memory_size = 512

  filename         = data.archive_file.ingest_news.output_path
  source_code_hash = data.archive_file.ingest_news.output_base64sha256

  environment {
    variables = {
      S3_BUCKET             = aws_s3_bucket.poly_rag_data.bucket
      METRICS_TABLE         = aws_dynamodb_table.architecture_metrics.name
      REGISTRY_TABLE        = aws_dynamodb_table.market_registry.name
      PROCESSED_URLS_TABLE  = aws_dynamodb_table.processed_urls.name
      DOMAIN_FAILURES_TABLE = aws_dynamodb_table.domain_failures.name
      BEDROCK_MODEL_ID      = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
      USE_LLM_ENRICHMENT    = "true"
      NEXT_LAMBDA_NAME      = aws_lambda_function.ingest_comments.function_name
    }
  }

  depends_on = [null_resource.ingest_news_deps]
}

data "archive_file" "ingest_comments" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/ingest_comments/handler.py"
  output_path = "${path.module}/build/ingest_comments.zip"
}

resource "aws_lambda_function" "ingest_comments" {
  function_name = "poly-rag-ingest-comments"
  role          = aws_iam_role.ingest_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # Replaces ingest_bluesky (2026-08-16, see tech_debt.md "Comments Source
  # Replaces Bluesky") -- one comments fetch per DISTINCT event/series id in
  # the registry (grouped, not per market -- a shared series is fetched
  # once even if many open markets point at it), a small fraction of the
  # 228-market registry's worth of external HTTP calls. Public, unauthenticated
  # Gamma API endpoint, same domain already used for market data.
  timeout     = 300
  memory_size = 256

  filename         = data.archive_file.ingest_comments.output_path
  source_code_hash = data.archive_file.ingest_comments.output_base64sha256

  environment {
    variables = {
      S3_BUCKET          = aws_s3_bucket.poly_rag_data.bucket
      METRICS_TABLE      = aws_dynamodb_table.architecture_metrics.name
      REGISTRY_TABLE     = aws_dynamodb_table.market_registry.name
      BEDROCK_MODEL_ID   = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
      USE_LLM_ENRICHMENT = "true"
      NEXT_LAMBDA_NAME   = aws_lambda_function.send_digest.function_name
    }
  }
}

data "archive_file" "send_digest" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/send_digest/handler.py"
  output_path = "${path.module}/build/send_digest.zip"
}

resource "aws_lambda_function" "send_digest" {
  function_name = "poly-rag-send-digest"
  role          = aws_iam_role.send_digest_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # 30s -> 60s (2026-08-16, bespoke digest redesign): now scans the open
  # registry + reads one odds S3 object per open market for volatility
  # ranking, plus one Bedrock call for the executive summary -- more work
  # than the old flat concatenation of 3 existing llm_summary fields.
  timeout     = 60
  memory_size = 128

  filename         = data.archive_file.send_digest.output_path
  source_code_hash = data.archive_file.send_digest.output_base64sha256

  environment {
    variables = {
      S3_BUCKET        = aws_s3_bucket.poly_rag_data.bucket
      SES_SENDER       = "bernardolw@gmail.com"
      SES_RECIPIENT    = "bernardolw@gmail.com"
      REGISTRY_TABLE   = aws_dynamodb_table.market_registry.name
      METRICS_TABLE    = aws_dynamodb_table.architecture_metrics.name
      BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
    }
  }
}
