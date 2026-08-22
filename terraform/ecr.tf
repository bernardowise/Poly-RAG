# ECR repo for write_lancedb, the only container-image Lambda in this
# project (2026-08-22) -- every other Lambda deploys via archive_file/zip.
# LanceDB's real dependency footprint is 339MB unzipped, over Lambda's 250MB
# zip/Layer limit (measured 2026-08-21, see tech_debt.md "Vector Store
# Choice"), so this one Lambda needs the Image package type instead.
resource "aws_ecr_repository" "write_lancedb" {
  name                 = "poly-rag-write-lancedb"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Keeps the repo from growing unbounded across redeploys -- only the most
# recent few images are ever needed (the Lambda always points at whichever
# tag/digest terraform last set), older ones are just storage cost.
resource "aws_ecr_lifecycle_policy" "write_lancedb" {
  repository = aws_ecr_repository.write_lancedb.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}
