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
      S3_BUCKET               = aws_s3_bucket.poly_rag_data.bucket
      METRICS_TABLE           = aws_dynamodb_table.architecture_metrics.name
      REGISTRY_TABLE          = aws_dynamodb_table.market_registry.name
      PROCESSED_URLS_TABLE    = aws_dynamodb_table.processed_urls.name
      DOMAIN_FAILURES_TABLE   = aws_dynamodb_table.domain_failures.name
      CYCLE_CHAIN_LOCKS_TABLE = aws_dynamodb_table.cycle_chain_locks.name
      BEDROCK_MODEL_ID        = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
      USE_LLM_ENRICHMENT      = "true"
      NEXT_LAMBDA_NAME        = aws_lambda_function.ingest_comments.function_name
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
      S3_BUCKET                 = aws_s3_bucket.poly_rag_data.bucket
      METRICS_TABLE             = aws_dynamodb_table.architecture_metrics.name
      REGISTRY_TABLE            = aws_dynamodb_table.market_registry.name
      PROCESSED_COMMENTS_TABLE  = aws_dynamodb_table.processed_comments.name
      BEDROCK_MODEL_ID          = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
      USE_LLM_ENRICHMENT        = "true"
      NEXT_LAMBDA_NAME          = aws_lambda_function.send_digest.function_name
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

# ---------------------------------------------------------------------------
# Fase 2 (embedding) -- added 2026-08-22. 9 Lambdas: embed_orchestrator (fans
# out chunking, kicks off the sequential embed chain), 4 chunking Lambdas
# (parallel, no shared resource risk), 4 embedding Lambdas (sequential --
# see embed_orchestrator/handler.py docstring for why parallel embedding was
# rejected: all 4 draw against the same Cohere TPM ceiling). All share
# embed_lambda_role (see iam_embed_lambda.tf) -- registry read-only, no
# Fase 1 dedup/lock table access, Bedrock scoped to Cohere Embed v4 only.
# ---------------------------------------------------------------------------

data "archive_file" "chunk_registry" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/chunk_registry/handler.py"
  output_path = "${path.module}/build/chunk_registry.zip"
}

resource "aws_lambda_function" "chunk_registry" {
  function_name = "poly-rag-chunk-registry"
  role          = aws_iam_role.embed_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # Full registry scan (~1,090 items today, boto3 only) filtered client-side
  # by first_seen -- fast, no external HTTP, no Bedrock call in this Lambda.
  timeout     = 60
  memory_size = 128

  filename         = data.archive_file.chunk_registry.output_path
  source_code_hash = data.archive_file.chunk_registry.output_base64sha256

  environment {
    variables = {
      S3_BUCKET      = aws_s3_bucket.poly_rag_data.bucket
      REGISTRY_TABLE = aws_dynamodb_table.market_registry.name
    }
  }
}

data "archive_file" "chunk_comments" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/chunk_comments/handler.py"
  output_path = "${path.module}/build/chunk_comments.zip"
}

resource "aws_lambda_function" "chunk_comments" {
  function_name = "poly-rag-chunk-comments"
  role          = aws_iam_role.embed_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # One cycle's comments payload (low hundreds of comments in steady state)
  # plus one registry scan for the entity map -- no external HTTP.
  timeout     = 60
  memory_size = 128

  filename         = data.archive_file.chunk_comments.output_path
  source_code_hash = data.archive_file.chunk_comments.output_base64sha256

  environment {
    variables = {
      S3_BUCKET      = aws_s3_bucket.poly_rag_data.bucket
      REGISTRY_TABLE = aws_dynamodb_table.market_registry.name
    }
  }
}

data "archive_file" "chunk_digest" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/chunk_digest/handler.py"
  output_path = "${path.module}/build/chunk_digest.zip"
}

resource "aws_lambda_function" "chunk_digest" {
  function_name = "poly-rag-chunk-digest"
  role          = aws_iam_role.embed_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # Exactly 1 chunk (the cycle's own digest) -- trivial, no Bedrock call
  # (deterministic template, see handler.py digest_to_text).
  timeout     = 30
  memory_size = 128

  filename         = data.archive_file.chunk_digest.output_path
  source_code_hash = data.archive_file.chunk_digest.output_base64sha256

  environment {
    variables = {
      S3_BUCKET = aws_s3_bucket.poly_rag_data.bucket
    }
  }
}

data "archive_file" "chunk_news_article" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/chunk_news_article/handler.py"
  output_path = "${path.module}/build/chunk_news_article.zip"
}

resource "aws_lambda_function" "chunk_news_article" {
  function_name = "poly-rag-chunk-news-article"
  role          = aws_iam_role.embed_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # One cycle's news payload (~700-900 articles in steady state), whole-
  # article chunking with overflow splitting -- pure CPU/string work, no
  # external HTTP, no Bedrock call in this Lambda (that's embed_news_article).
  timeout     = 60
  memory_size = 256

  filename         = data.archive_file.chunk_news_article.output_path
  source_code_hash = data.archive_file.chunk_news_article.output_base64sha256

  environment {
    variables = {
      S3_BUCKET = aws_s3_bucket.poly_rag_data.bucket
    }
  }
}

data "archive_file" "embed_digest" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/embed_digest/handler.py"
  output_path = "${path.module}/build/embed_digest.zip"
}

resource "aws_lambda_function" "embed_digest" {
  function_name = "poly-rag-embed-digest"
  role          = aws_iam_role.embed_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # 1 chunk, 1 Bedrock call -- trivial in steady state.
  timeout     = 60
  memory_size = 128

  filename         = data.archive_file.embed_digest.output_path
  source_code_hash = data.archive_file.embed_digest.output_base64sha256

  environment {
    variables = {
      S3_BUCKET               = aws_s3_bucket.poly_rag_data.bucket
      NEXT_LAMBDA_NAME        = aws_lambda_function.embed_comments.function_name
      EMBEDDING_METRICS_TABLE = aws_dynamodb_table.embedding_metrics.name
    }
  }
}

data "archive_file" "embed_comments" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/embed_comments/handler.py"
  output_path = "${path.module}/build/embed_comments.zip"
}

resource "aws_lambda_function" "embed_comments" {
  function_name = "poly-rag-embed-comments"
  role          = aws_iam_role.embed_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # Steady-state comments volume (~50-100/cycle) embeds in well under a
  # minute at the measured ~120K tok/min effective rate -- see
  # tech_debt.md for the real numbers from the 2026-08-21/22 bootstrap.
  timeout     = 120
  memory_size = 128

  filename         = data.archive_file.embed_comments.output_path
  source_code_hash = data.archive_file.embed_comments.output_base64sha256

  environment {
    variables = {
      S3_BUCKET               = aws_s3_bucket.poly_rag_data.bucket
      NEXT_LAMBDA_NAME        = aws_lambda_function.embed_registry.function_name
      EMBEDDING_METRICS_TABLE = aws_dynamodb_table.embedding_metrics.name
    }
  }
}

data "archive_file" "embed_registry" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/embed_registry/handler.py"
  output_path = "${path.module}/build/embed_registry.zip"
}

resource "aws_lambda_function" "embed_registry" {
  function_name = "poly-rag-embed-registry"
  role          = aws_iam_role.embed_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # Steady-state new-market volume (~50-90/cycle, see chunk_registry) embeds
  # in well under a minute.
  timeout     = 120
  memory_size = 128

  filename         = data.archive_file.embed_registry.output_path
  source_code_hash = data.archive_file.embed_registry.output_base64sha256

  environment {
    variables = {
      S3_BUCKET               = aws_s3_bucket.poly_rag_data.bucket
      NEXT_LAMBDA_NAME        = aws_lambda_function.embed_news_article.function_name
      EMBEDDING_METRICS_TABLE = aws_dynamodb_table.embedding_metrics.name
    }
  }
}

data "archive_file" "embed_news_article" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/embed_news_article/handler.py"
  output_path = "${path.module}/build/embed_news_article.zip"
}

resource "aws_lambda_function" "embed_news_article" {
  function_name = "poly-rag-embed-news-article"
  role          = aws_iam_role.embed_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # Last and heaviest stage in the embed chain -- news_article dominates
  # token volume (~96% of the 4-source total measured during the 13-cycle
  # bootstrap, see tech_debt.md). Steady-state cycle volume (~700-900
  # articles) is far smaller than that bootstrap, but 900s gives real
  # headroom given Bedrock pacing latency is the dominant cost, not CPU.
  # Invokes digest_metrics as its last act (split out 2026-08-22 -- the
  # report email used to be sent inline here, see digest_metrics/handler.py
  # for why that got its own Lambda). Fase 3 (write to LanceDB) is still out
  # of scope for THIS Lambda's own chaining -- see handler.py docstring.
  timeout     = 900
  memory_size = 256

  filename         = data.archive_file.embed_news_article.output_path
  source_code_hash = data.archive_file.embed_news_article.output_base64sha256

  environment {
    variables = {
      S3_BUCKET                  = aws_s3_bucket.poly_rag_data.bucket
      EMBEDDING_METRICS_TABLE    = aws_dynamodb_table.embedding_metrics.name
      DIGEST_METRICS_LAMBDA_NAME = aws_lambda_function.digest_metrics.function_name
    }
  }
}

data "archive_file" "digest_metrics" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/digest_metrics/handler.py"
  output_path = "${path.module}/build/digest_metrics.zip"
}

resource "aws_lambda_function" "digest_metrics" {
  function_name = "poly-rag-digest-metrics"
  role          = aws_iam_role.digest_metrics_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # Terminal stage of the whole cycle (Fase 1 + Fase 2) -- split out of
  # embed_news_article 2026-08-22, see handler.py docstring for why. Just 2
  # DynamoDB Scans + 1 SES send, no Bedrock call -- short timeout is enough.
  timeout     = 60
  memory_size = 128

  filename         = data.archive_file.digest_metrics.output_path
  source_code_hash = data.archive_file.digest_metrics.output_base64sha256

  environment {
    variables = {
      EMBEDDING_METRICS_TABLE    = aws_dynamodb_table.embedding_metrics.name
      ARCHITECTURE_METRICS_TABLE = aws_dynamodb_table.architecture_metrics.name
      SES_SENDER                 = "bernardolw@gmail.com"
      SES_RECIPIENT              = "bernardolw@gmail.com"
    }
  }
}

data "archive_file" "embed_orchestrator" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/embed_orchestrator/handler.py"
  output_path = "${path.module}/build/embed_orchestrator.zip"
}

resource "aws_lambda_function" "embed_orchestrator" {
  function_name = "poly-rag-embed-orchestrator"
  role          = aws_iam_role.embed_lambda_role.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  # Pure fan-out/dispatch -- 5 async lambda:InvokeFunction calls, no S3/
  # DynamoDB/Bedrock work of its own. Returns almost immediately.
  timeout     = 30
  memory_size = 128

  filename         = data.archive_file.embed_orchestrator.output_path
  source_code_hash = data.archive_file.embed_orchestrator.output_base64sha256

  environment {
    variables = {
      CHUNK_REGISTRY_NAME      = aws_lambda_function.chunk_registry.function_name
      CHUNK_COMMENTS_NAME      = aws_lambda_function.chunk_comments.function_name
      CHUNK_DIGEST_NAME        = aws_lambda_function.chunk_digest.function_name
      CHUNK_NEWS_ARTICLE_NAME  = aws_lambda_function.chunk_news_article.function_name
      FIRST_EMBED_LAMBDA_NAME  = aws_lambda_function.embed_digest.function_name
    }
  }
}
