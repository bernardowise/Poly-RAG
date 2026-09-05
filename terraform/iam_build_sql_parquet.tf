# build_sql_parquet execution role -- Phase 4 (SQL layer), its own dedicated
# role (same reasoning as write_lancedb's / digest_metrics' roles): its job
# (scan the registry, read every odds/*.json, write sql/*.parquet) is a
# distinct concern from embedding or vector writes, so it gets scoped
# permissions instead of reusing a broader role.
resource "aws_iam_role" "build_sql_parquet_role" {
  name               = "poly-rag-build-sql-parquet-role"
  description        = "Execution role for poly-rag-build-sql-parquet (Phase 4, per-cycle SQL-layer Parquet refresh)"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "build_sql_parquet_permissions" {
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
    # Whole registry scan -> sql/markets.parquet.
    effect    = "Allow"
    actions   = ["dynamodb:Scan"]
    resources = ["arn:aws:dynamodb:us-east-1:369970405415:table/poly-rag-market-registry"]
  }

  statement {
    # Read every odds/<market_id>.json to rebuild the current month's
    # odds_snapshots partition. Read-only against odds/.
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.poly_rag_data.arn}/odds/*"]
  }

  statement {
    # Paginated list_objects_v2 over odds/ to enumerate the files, and over
    # sql/ is not needed (keys are computed), but kept symmetric.
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.poly_rag_data.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["odds/*", "sql/*"]
    }
  }

  statement {
    # Write sql/markets.parquet and sql/odds_snapshots/YYYY-MM.parquet.
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.poly_rag_data.arn}/sql/*"]
  }

  statement {
    # Fourth checkpoint email of the cycle (Phase 4), after write_lancedb's
    # Phase 3 report.
    effect    = "Allow"
    actions   = ["ses:SendEmail", "ses:SendRawEmail"]
    resources = ["*"]
  }

  # NOTE: no lambda:InvokeFunction statement yet -- this Lambda is the
  # terminal stage until rag_eval (Phase 5) exists. When it does, add an
  # InvokeFunction statement scoped to aws_lambda_function.rag_eval.arn and
  # set RAG_EVAL_LAMBDA_NAME in the environment block below.
}

resource "aws_iam_role_policy" "build_sql_parquet_permissions" {
  name   = "poly-rag-build-sql-parquet-permissions"
  role   = aws_iam_role.build_sql_parquet_role.id
  policy = data.aws_iam_policy_document.build_sql_parquet_permissions.json
}
