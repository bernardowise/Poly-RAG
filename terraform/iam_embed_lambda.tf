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
    effect  = "Allow"
    actions = ["bedrock:InvokeModel"]
    resources = [
      "arn:aws:bedrock:us-east-1:369970405415:inference-profile/global.cohere.embed-v4:0",
      "arn:aws:bedrock:us-east-1::foundation-model/cohere.embed-v4:0",
      "arn:aws:bedrock:us-east-2::foundation-model/cohere.embed-v4:0",
      "arn:aws:bedrock:us-west-2::foundation-model/cohere.embed-v4:0",
    ]
  }

  statement {
    # Fase 2 cost/latency tracking (2026-08-22, see dynamodb.tf and
    # tech_debt.md, "Day 6" entry) -- the 4 embed Lambdas write one row per
    # Bedrock request here. Query added for embed_news_article specifically:
    # as the last stage of the sequential embed chain, it reads back every
    # row written this cycle (across all 4 sources) to build the Fase 2
    # metrics report email -- see embed_news_article/handler.py.
    effect = "Allow"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:Query",
    ]
    resources = [
      aws_dynamodb_table.embedding_metrics.arn,
      "${aws_dynamodb_table.embedding_metrics.arn}/index/*",
    ]
  }

  statement {
    # Read-only. embed_news_article's report email covers the WHOLE cycle,
    # not just Fase 2 -- see handler.py -- so it also reads Fase 1's existing
    # architecture_metrics table (already written by the 4 ingestion
    # Lambdas). Scan, not Query: that table's rows aren't keyed in a way that
    # supports a cycle_started_at prefix query (see ingest_lambda_role's
    # PutItem-only grant on the same table -- Fase 1 never needed to read it
    # back before now).
    effect    = "Allow"
    actions   = ["dynamodb:Scan"]
    resources = [aws_dynamodb_table.architecture_metrics.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["ses:SendEmail", "ses:SendRawEmail"]
    resources = ["*"]
  }

  statement {
    # The whole Fase 2 chain: embed_orchestrator fans out to the 4 chunking
    # Lambdas and kicks off the embed chain at embed_digest; the embed chain
    # itself is sequential (embed_digest -> embed_comments -> embed_registry
    # -> embed_news_article), deliberately NOT parallel -- see
    # embed_orchestrator handler.py docstring for why parallel embedding
    # would recreate the News double-invocation race. Every target is listed
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
    ]
  }
}

resource "aws_iam_role_policy" "embed_lambda_permissions" {
  name   = "poly-rag-embed-permissions"
  role   = aws_iam_role.embed_lambda_role.id
  policy = data.aws_iam_policy_document.embed_lambda_permissions.json
}
