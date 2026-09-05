# write_lancedb execution role -- Fase 3, its own dedicated role (same
# reasoning as digest_metrics' role, see iam_embed_lambda.tf): its job (read
# chunks + vectors, write to LanceDB tables in S3) is a distinct concern from
# chunking/embedding, so it gets scoped permissions instead of inheriting
# leftovers from embed_lambda_role.
resource "aws_iam_role" "write_lancedb_role" {
  name               = "poly-rag-write-lancedb-role"
  description        = "Execution role for poly-rag-write-lancedb (Fase 3, per-cycle write to LanceDB)"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "write_lancedb_permissions" {
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
    # Read-only against chunks/ and vectors/ -- this Lambda never writes to
    # either, same read-only posture as the one-off script it's ported from.
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.poly_rag_data.arn}/chunks/*", "${aws_s3_bucket.poly_rag_data.arn}/vectors/*"]
  }

  statement {
    # vectors/ listing needed for load_vectors' paginated list_objects_v2
    # over the checkpoint prefix.
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.poly_rag_data.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["chunks/*", "vectors/*", "lancedb/*"]
    }
  }

  statement {
    # Read/write/delete under lancedb/ -- LanceDB is a columnar file format
    # written directly to S3 (not a managed service, see architecture_canon.md
    # "Vector Store Choice"), so every table operation (merge_insert, table
    # creation, manifest/transaction log updates) is real S3 object I/O
    # under this one prefix.
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.poly_rag_data.arn}/lancedb/*"]
  }

  statement {
    # Third checkpoint email of the cycle (2026-08-22) -- write_lancedb's own
    # per-source write report, separate from send_digest's market digest and
    # digest_metrics' cost report.
    effect    = "Allow"
    actions   = ["ses:SendEmail", "ses:SendRawEmail"]
    resources = ["*"]
  }

  statement {
    # write_lancedb invokes build_sql_parquet (Phase 4) as its last act
    # (2026-09-05), after the Phase 3 report email is sent.
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.build_sql_parquet.arn]
  }
}

resource "aws_iam_role_policy" "write_lancedb_permissions" {
  name   = "poly-rag-write-lancedb-permissions"
  role   = aws_iam_role.write_lancedb_role.id
  policy = data.aws_iam_policy_document.write_lancedb_permissions.json
}
