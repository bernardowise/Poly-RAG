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
    effect    = "Allow"
    actions   = ["ses:SendEmail", "ses:SendRawEmail"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "send_digest_permissions" {
  name   = "poly-rag-send-digest-permissions"
  role   = aws_iam_role.send_digest_role.id
  policy = data.aws_iam_policy_document.send_digest_permissions.json
}
