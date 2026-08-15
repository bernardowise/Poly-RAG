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
