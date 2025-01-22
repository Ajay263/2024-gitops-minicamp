package terraform

import rego.v1

# Define allowed naming patterns
valid_bucket_pattern := "^nexabrands-[a-z]+-[a-z0-9-]+$"

valid_glue_pattern := "^nexabrands-[a-z]+-[a-z0-9-]+$"

# Resource limits
glue_job_limits := {
	"max_workers": 2,
	"max_timeout": 2880,
	"max_retries": 1,
	"allowed_worker_types": ["G.1X"],
	"allowed_command_names": ["glueetl", "pythonshell"], # Added pythonshell
}

# S3 bucket naming convention check (unchanged)
deny contains msg if {
	some resource in input.resource_changes
	resource.type == "aws_s3_bucket"
	bucket_name := resource.change.after.bucket
	not regex.match(valid_bucket_pattern, bucket_name)
	msg := sprintf(
		"S3 bucket '%s' name does not follow the required pattern. Must match: '%s'",
		[bucket_name, valid_bucket_pattern],
	)
}

# Glue crawler checks (unchanged)
deny contains msg if {
	some resource in input.resource_changes
	resource.type == "aws_glue_crawler"
	crawler := resource.change.after

	not crawler.schema_change_policy
	msg := sprintf(
		"Glue crawler '%s' must have a schema_change_policy defined",
		[resource.address],
	)
}

# Modified Glue job checks to handle both ETL and Python shell jobs
deny contains msg if {
	some resource in input.resource_changes
	resource.type == "aws_glue_job"
	job := resource.change.after

	# Check command name is in allowed list
	some command in job.command
	not command.name in glue_job_limits.allowed_command_names
	msg := sprintf(
		"Glue job '%s' uses invalid command name. Allowed types are %v, got %s",
		[resource.address, glue_job_limits.allowed_command_names, command.name],
	)
}

# Worker count check only for glueetl jobs
deny contains msg if {
	some resource in input.resource_changes
	resource.type == "aws_glue_job"
	job := resource.change.after

	# Only check worker count for glueetl jobs
	some command in job.command
	command.name == "glueetl"
	job.number_of_workers > glue_job_limits.max_workers
	msg := sprintf(
		"Glue job '%s' exceeds maximum allowed workers. Maximum is %d, got %d",
		[resource.address, glue_job_limits.max_workers, job.number_of_workers],
	)
}

# Timeout check for all jobs
deny contains msg if {
	some resource in input.resource_changes
	resource.type == "aws_glue_job"
	job := resource.change.after

	job.timeout > glue_job_limits.max_timeout
	msg := sprintf(
		"Glue job '%s' exceeds maximum allowed timeout. Maximum is %d minutes, got %d",
		[resource.address, glue_job_limits.max_timeout, job.timeout],
	)
}

# Max retries check for all jobs
deny contains msg if {
	some resource in input.resource_changes
	resource.type == "aws_glue_job"
	job := resource.change.after

	job.max_retries > glue_job_limits.max_retries
	msg := sprintf(
		"Glue job '%s' exceeds maximum allowed retries. Maximum is %d, got %d",
		[resource.address, glue_job_limits.max_retries, job.max_retries],
	)
}

# Worker type check only for glueetl jobs
deny contains msg if {
	some resource in input.resource_changes
	resource.type == "aws_glue_job"
	job := resource.change.after

	# Only check worker type for glueetl jobs
	some command in job.command
	command.name == "glueetl"
	not job.worker_type in glue_job_limits.allowed_worker_types
	msg := sprintf(
		"Glue job '%s' uses invalid worker type. Allowed types are %v, got %s",
		[resource.address, glue_job_limits.allowed_worker_types, job.worker_type],
	)
}

# Python version check for all jobs
deny contains msg if {
	some resource in input.resource_changes
	resource.type == "aws_glue_job"
	job := resource.change.after

	some command in job.command
	not command.python_version == "3"
	msg := sprintf(
		"Glue job '%s' must use Python version 3",
		[resource.address],
	)
}
