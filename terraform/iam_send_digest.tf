resource "aws_iam_role" "send_digest_role" {
  name               = "poly-rag-send-digest-role"
  description        = "Execution role for the Poly-RAG email digest Lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "send_digest_permissions" {
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
    # Bespoke digest redesign (2026-08-16): the digest is now also a data
    # artifact written to S3 (digest/YYYY-MM-DD/HH.json), not just an email
    # -- see tech_debt.md. Previously this role only ever read.
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.poly_rag_data.arn}/digest/*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["ses:SendEmail", "ses:SendRawEmail"]
    resources = ["*"]
  }

  statement {
    # Bespoke digest redesign (2026-08-16, see tech_debt.md): computing
    # top-volatility odds movements and the executive-summary synthesis
    # both need to scan/read the market registry.
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:Scan",
    ]
    resources = [aws_dynamodb_table.market_registry.arn]
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
}

resource "aws_iam_role_policy" "send_digest_permissions" {
  name   = "poly-rag-send-digest-permissions"
  role   = aws_iam_role.send_digest_role.id
  policy = data.aws_iam_policy_document.send_digest_permissions.json
}
