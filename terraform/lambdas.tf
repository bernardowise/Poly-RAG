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
  # 300s (up from 60s): per-market Google News search + URL decode + article
  # extraction across up to ~230 open markets, sequential (see handler
  # module docstring).
  timeout     = 300
  memory_size = 512

  filename         = data.archive_file.ingest_news.output_path
  source_code_hash = data.archive_file.ingest_news.output_base64sha256

  environment {
    variables = {
      S3_BUCKET            = aws_s3_bucket.poly_rag_data.bucket
      METRICS_TABLE        = aws_dynamodb_table.architecture_metrics.name
      REGISTRY_TABLE       = aws_dynamodb_table.market_registry.name
      PROCESSED_URLS_TABLE = aws_dynamodb_table.processed_urls.name
      BEDROCK_MODEL_ID     = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
      USE_LLM_ENRICHMENT   = "true"
    }
  }

  depends_on = [null_resource.ingest_news_deps]
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
  # 600s (up from 60s): the redesigned pipeline queries searchPosts once per
  # open market in the registry (full coverage, not a top-N subset -- see
  # handler module docstring), which is 500+ sequential external HTTP calls
  # per cycle instead of the old 3 fixed vertical queries.
  timeout     = 600
  memory_size = 256

  filename         = data.archive_file.ingest_bluesky.output_path
  source_code_hash = data.archive_file.ingest_bluesky.output_base64sha256

  environment {
    variables = {
      S3_BUCKET          = aws_s3_bucket.poly_rag_data.bucket
      METRICS_TABLE      = aws_dynamodb_table.architecture_metrics.name
      REGISTRY_TABLE     = aws_dynamodb_table.market_registry.name
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
  timeout       = 30
  memory_size   = 128

  filename         = data.archive_file.send_digest.output_path
  source_code_hash = data.archive_file.send_digest.output_base64sha256

  environment {
    variables = {
      S3_BUCKET     = aws_s3_bucket.poly_rag_data.bucket
      SES_SENDER    = "bernardolw@gmail.com"
      SES_RECIPIENT = "bernardolw@gmail.com"
    }
  }
}
