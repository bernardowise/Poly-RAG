resource "aws_s3_bucket" "poly_rag_data" {
  bucket = "poly-rag-369970405415"
}

resource "aws_s3_bucket_versioning" "poly_rag_data" {
  # Added 2026-08-17, deliberately BEFORE deleting the 329 obsolete
  # pre-2026-08-16 registry-linked odds files + the Day-1 test object --
  # user's explicit choice ("mas higienico aunque si sea recuperable"):
  # those deletes should be recoverable via version history (a deliberate
  # restore action), not gone from AWS entirely, while still being fully
  # removed from the LIVE pipeline and future RAG ingestion (which only
  # ever reads current/live object state, never old versions).
  bucket = aws_s3_bucket.poly_rag_data.id
  versioning_configuration {
    status = "Enabled"
  }
}
