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

resource "aws_dynamodb_table" "cycle_chain_locks" {
  # Fixes the real double-trigger incident of 2026-08-19 (see tech_debt.md,
  # "PRIORIDAD 1"): merge_batch_payloads is idempotent for WRITING the final
  # cycle payload (any batch can safely overwrite it with the same result),
  # but had no guard on the NEXT step -- invoking Comments. With N parallel
  # News batches, more than one could see "all batch files exist" at once
  # and each invoked Comments, which cascaded to N digests/emails. One item
  # per cycle claims the right to advance the chain; ConditionExpression
  # attribute_not_exists(pk) in ingest_news means only the first writer wins,
  # everyone else gets ConditionalCheckFailedException and does nothing. No
  # TTL, same reasoning as processed_urls/processed_comments -- pay-per-
  # request doesn't charge for idle storage of a few small items per day.
  name         = "poly-rag-cycle-chain-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
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

resource "aws_dynamodb_table" "embedding_metrics" {
  # Fase 2 cost/latency tracking -- decided 2026-08-20 (see tech_debt.md,
  # "Day 6: A/B Tests as a Live, User-Facing Feature"), built 2026-08-22 once
  # the 8 Fase 2 Lambdas that need it actually existed. Deliberately a
  # SEPARATE table from architecture_metrics: Fase 1 writes one row per
  # Lambda invocation, but Fase 2 embedding writes N rows per invocation (one
  # per Bedrock request within a batch run), each carrying its own
  # source/embedding_model/tokens_in -- forcing that into architecture_metrics'
  # one-row-per-invocation shape would make both tables harder to query
  # cleanly. Same infra pattern as every other project table: pay-per-request,
  # PITR enabled.
  #
  # hash_key is a composite string (cycle_started_at#source#request_index),
  # not a generated UUID -- makes "all rows for this cycle" and "all rows for
  # this cycle+source" both directly queryable by key prefix without a GSI,
  # and keeps rows naturally ordered by insertion within a source.
  name         = "poly-rag-embedding-metrics"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }
}
