resource "aws_dynamodb_table" "architecture_metrics" {
  name         = "poly-rag-architecture-metrics"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }
}
