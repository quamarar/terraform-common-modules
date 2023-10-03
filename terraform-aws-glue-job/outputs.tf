output "id" {
  description = "Glue job ID"
  value       =  aws_glue_job.this.id
}

output "name" {
  description = "Glue job name"
  value       = aws_glue_job.this.name
}

output "arn" {
  description = "Glue job ARN"
  value       =  aws_glue_job.this.arn
}
