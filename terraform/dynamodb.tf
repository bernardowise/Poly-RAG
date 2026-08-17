resource "aws_dynamodb_table" "architecture_metrics" {
  name         = "poly-rag-architecture-metrics"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  # Enabled 2026-08-17, same reasoning as S3 versioning (see s3.tf) -- applied
  # to all 4 tables for consistency, not just market_registry where the
  # immediate cleanup happened. Any accidental delete/overwrite from here on
  # is recoverable via a deliberate point-in-time restore, not gone outright.
  point_in_time_recovery {
    enabled = true
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

  point_in_time_recovery {
    enabled = true
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

  point_in_time_recovery {
    enabled = true
  }
}

resource "aws_dynamodb_table" "processed_comments" {
  # Dedup for Polymarket comment ingestion (added 2026-08-17) -- one item per
  # comment_id ever fetched. Same pattern/reasoning as processed_urls: without
  # this, fetch_comments re-pulls the same top-20-by-createdAt comments per
  # entity every cycle regardless of whether anything new was posted, and the
  # comments/HH.json payload re-includes comments already seen in prior
  # cycles. No TTL: pay-per-request doesn't charge for idle storage of small
  # items, so permanent dedup is simpler than an arbitrary expiry window.
  name         = "poly-rag-processed-comments"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "comment_id"

  attribute {
    name = "comment_id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }
}

resource "aws_dynamodb_table" "domain_failures" {
  # Dynamic blocklist for outlets that consistently fail extraction (see
  # tech_debt.md, "News Source Redesign" -- domains like egamersworld.com
  # confirmed failing 100% of the time across many distinct markets during
  # the 2026-08-15/16 production run). One item per domain: consecutive
  # failure count, reset to 0 on any success, domain skipped without
  # attempting the request once the count crosses BLOCKLIST_THRESHOLD in
  # the handler. Chosen over a hardcoded list so it adapts to real observed
  # failures instead of a list someone has to remember to update.
  name         = "poly-rag-domain-failures"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "domain"

  attribute {
    name = "domain"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }
}
