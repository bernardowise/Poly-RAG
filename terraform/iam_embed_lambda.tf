# Fase 2 (embedding) execution role -- shared across all 9 Lambdas
# (embed_orchestrator + 4 chunking + 4 embedding), same pattern as
# ingest_lambda_role for Fase 1's 3 ingestion Lambdas. Connected to Fase 1 by
# send_digest invoking embed_orchestrator (see iam_send_digest.tf for that
# grant) -- everything from here down is a separate execution role, not an
# extension of ingest_lambda_role, since Fase 2 reads (never writes) the
# registry and never touches the Fase 1 dedup/lock tables at all.

resource "aws_iam_role" "embed_lambda_role" {
  name               = "poly-rag-embed-lambda-role"
  description        = "Execution role for Poly-RAG Fase 2 Lambdas (chunking + embedding orchestration)"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "embed_lambda_permissions" {
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
    # chunks/ and vectors/ writes, plus reads of news/comments/digest cycle
    # payloads that chunk_comments/chunk_news_article/chunk_digest consume.
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.poly_rag_data.arn}/*"]
  }

  statement {
    # Needed for the same reason as ingest_lambda_role -- listing checkpoint
    # parts under vectors/_checkpoints/<variant>/cohere/ to detect
    # already-embedded chunk_ids (already_embedded_ids), and to find the next
    # free part index.
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.poly_rag_data.arn]
  }

  statement {
    # Read-only. Fase 2 NEVER writes to the registry -- chunk_registry only
    # scans for first_seen > cycle_started_at (see chunk_registry handler.py
    # docstring), chunk_comments only reads comment_entity_id per market_id.
    # Deliberately narrower than ingest_lambda_role's registry grant, which
    # needs write access for Fase 1's own market tracking.
    effect    = "Allow"
    actions   = ["dynamodb:Scan"]
    resources = [aws_dynamodb_table.market_registry.arn]
  }

  statement {
    # The 4 embed Lambdas call Cohere Embed v4 via its cross-region inference
    # profile -- global.cohere.embed-v4:0, chosen 2026-08-22 after two real
    # daily-quota outages on the bare on-demand and us. cross-region routes
    # (see tech_debt.md, "Phase 2 Embedding Bootstrap", second correction).
    # Region wildcard on the foundation-model resource: the "global." profile
    # (unlike "us.") can route outside the 3 US regions we originally
    # enumerated, which caused a real AccessDeniedException on cycle 14
    # (2026-08-22, embed_digest) when Bedrock resolved it to a region we had
    # not granted.
    effect  = "Allow"
    actions = ["bedrock:InvokeModel"]
    resources = [
      "arn:aws:bedrock:us-east-1:369970405415:inference-profile/global.cohere.embed-v4:0",
      "arn:aws:bedrock:*::foundation-model/cohere.embed-v4:0",
    ]
  }

  statement {
    # Fase 2 cost/latency tracking (2026-08-22, see dynamodb.tf and
    # tech_debt.md, "Day 6" entry) -- the 4 embed Lambdas write one row per
    # Bedrock request here. Write-only: reading these rows back to build the
    # Fase 2 metrics report email is digest_metrics' job now (split out
    # 2026-08-22, see its own dedicated role below), not this shared role's.
    effect    = "Allow"
    actions   = ["dynamodb:PutItem"]
    resources = [aws_dynamodb_table.embedding_metrics.arn]
  }

  statement {
    # The whole Fase 2 chain: embed_orchestrator fans out to the 4 chunking
    # Lambdas and kicks off the embed chain at embed_digest; the embed chain
    # itself is sequential (embed_digest -> embed_comments -> embed_registry
    # -> embed_news_article), deliberately NOT parallel -- see
    # embed_orchestrator handler.py docstring for why parallel embedding
    # would recreate the News double-invocation race. embed_news_article also
    # invokes digest_metrics as its last act (split out 2026-08-22, see
    # digest_metrics/handler.py -- the report email used to be sent inline
    # here, now it's a separate terminal Lambda). Every target is listed
    # explicitly, none of this role's Lambdas gets "*".
    effect  = "Allow"
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.chunk_registry.arn,
      aws_lambda_function.chunk_comments.arn,
      aws_lambda_function.chunk_digest.arn,
      aws_lambda_function.chunk_news_article.arn,
      aws_lambda_function.embed_digest.arn,
      aws_lambda_function.embed_comments.arn,
      aws_lambda_function.embed_registry.arn,
      aws_lambda_function.embed_news_article.arn,
      aws_lambda_function.digest_metrics.arn,
    ]
  }
}

resource "aws_iam_role_policy" "embed_lambda_permissions" {
  name   = "poly-rag-embed-permissions"
  role   = aws_iam_role.embed_lambda_role.id
  policy = data.aws_iam_policy_document.embed_lambda_permissions.json
}

# digest_metrics execution role -- deliberately separate from
# embed_lambda_role (2026-08-22), not piled onto the shared Fase 2 role.
# Its job (read both metrics tables, send one report email) is orthogonal to
# chunking/embedding, and giving it its own role means its permissions are
# scoped exactly to what it needs, not inherited leftovers from a role built
# for a different set of Lambdas.
resource "aws_iam_role" "digest_metrics_lambda_role" {
  name               = "poly-rag-digest-metrics-role"
  description        = "Execution role for poly-rag-digest-metrics (Fase 1+2 cycle report email)"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "digest_metrics_permissions" {
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
    # Read-only, both metrics tables -- Fase 2's own (written by the 4 embed
    # Lambdas via embed_lambda_role's PutItem-only grant above) and Fase 1's
    # pre-existing architecture_metrics (written by the 4 ingestion Lambdas,
    # see ingest_lambda_role). Scan, not Query, on both: neither table's keys
    # support a cycle_started_at range query (see fetch_embedding_metrics/
    # fetch_architecture_metrics docstrings in digest_metrics/handler.py).
    effect = "Allow"
    actions = ["dynamodb:Scan"]
    resources = [
      aws_dynamodb_table.embedding_metrics.arn,
      aws_dynamodb_table.architecture_metrics.arn,
    ]
  }

  statement {
    effect    = "Allow"
    actions   = ["ses:SendEmail", "ses:SendRawEmail"]
    resources = ["*"]
  }

  statement {
    # digest_metrics invokes write_lancedb as the true last step of the
    # cycle, after the report email is sent (2026-08-22, see
    # write_lancedb/handler.py).
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.write_lancedb.arn]
  }
}

resource "aws_iam_role_policy" "digest_metrics_permissions" {
  name   = "poly-rag-digest-metrics-permissions"
  role   = aws_iam_role.digest_metrics_lambda_role.id
  policy = data.aws_iam_policy_document.digest_metrics_permissions.json
}
