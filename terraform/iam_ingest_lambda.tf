data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ingest_lambda_role" {
  name               = "poly-rag-ingest-lambda-role"
  description        = "Execution role for Poly-RAG ingestion Lambdas (Polymarket/News/Bluesky)"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "ingest_lambda_permissions" {
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
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.poly_rag_data.arn}/*"]
  }

  statement {
    # S3 needs ListBucket (bucket-level, not object-level) to correctly return
    # a 404 for a missing key -- without it, GetObject on a nonexistent odds/
    # snapshot file returns an opaque 403 AccessDenied instead of NoSuchKey,
    # which broke the read-modify-write pattern in append_odds_snapshot.
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.poly_rag_data.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["dynamodb:PutItem"]
    resources = [aws_dynamodb_table.architecture_metrics.arn]
  }

  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:Scan",
      "dynamodb:Query",
    ]
    resources = [aws_dynamodb_table.market_registry.arn]
  }

  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
    ]
    resources = [aws_dynamodb_table.processed_urls.arn]
  }

  statement {
    effect  = "Allow"
    actions = ["bedrock:InvokeModel"]
    resources = [
      "arn:aws:bedrock:us-east-1:369970405415:inference-profile/us.anthropic.claude-sonnet-4-5-20250929-v1:0",
      "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-sonnet-4-5-20250929-v1:0",
      "arn:aws:bedrock:us-east-2::foundation-model/anthropic.claude-sonnet-4-5-20250929-v1:0",
      "arn:aws:bedrock:us-west-2::foundation-model/anthropic.claude-sonnet-4-5-20250929-v1:0",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "aws-marketplace:ViewSubscriptions",
      "aws-marketplace:Subscribe",
    ]
    resources = ["*"]
  }

  statement {
    # ingest_news self-invokes to process the open-market registry in
    # batches (see tech_debt.md "News Source Redesign" update -- ~228
    # markets at ~21s each cannot fit in one invocation under Lambda's 900s
    # hard maximum). Scoped to itself only, not "*".
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.ingest_news.arn]
  }
}

resource "aws_iam_role_policy" "ingest_lambda_permissions" {
  name   = "poly-rag-ingest-permissions"
  role   = aws_iam_role.ingest_lambda_role.id
  policy = data.aws_iam_policy_document.ingest_lambda_permissions.json
}
