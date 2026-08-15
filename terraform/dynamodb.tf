resource "aws_dynamodb_table" "architecture_metrics" {
  name         = "poly-rag-architecture-metrics"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }
}

resource "aws_dynamodb_table" "market_registry" {
  name         = "poly-rag-market-registry"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "market_id"

  attribute {
    name = "market_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "processed_urls" {
  # Dedup for Google News article extraction (see tech_debt.md, "News Source
  # Redesign") -- one item per real article URL (post googlenewsdecoder
  # resolution) ever successfully processed. No TTL: pay-per-request billing
  # doesn't charge for idle storage of small items, so permanent dedup is
  # simpler than picking an arbitrary expiry window. The article content
  # itself lives permanently in S3 regardless of what happens to this table.
  name         = "poly-rag-processed-urls"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "url"

  attribute {
    name = "url"
    type = "S"
  }
}
