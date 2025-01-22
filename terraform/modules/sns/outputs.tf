
# modules/sns/outputs.tf
output "topic_arn" {
  value = aws_sns_topic.schema_changes.arn
}
