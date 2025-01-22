# modules/sns/main.tf
resource "aws_sns_topic" "schema_changes" {
  name = "topdevs-${var.environment}-schema-changes"
}
