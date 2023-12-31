### security
variable "bucket_policy" {
  description = "Bucket-side access control setting"
  default     = {}
}

variable "canned_acl" {
  description = "Predefined access control rule. The default is 'private' to prevent all access"
  type        = string
  default     = "private"
}

variable "lifecycle_rules" {
  description = "A configuration of object lifecycle management"
  default     = []
}

variable "logging_rules" {
  description = "A configuration of bucket logging management"
  default     = []
}

variable "server_side_encryption" {
  description = "A configuration of server side encryption"
  default     = [{ sse_algorithm = "AES256" }]
}

variable "intelligent_tiering_archive_rules" {
  description = "A configuration of intelligent tiering archive management"
  default     = null
  validation {
    condition     = var.intelligent_tiering_archive_rules != {}
    error_message = "The intelligent_tiering_archive_rules must not be empty. Required at least one archive tier."
  }
}

variable "versioning" {
  description = "A configuration to enable object version control"
  type        = bool
  default     = false
}

variable "force_destroy" {
  description = "A boolean that indicates all objects should be deleted from the bucket so that the bucket can be destroyed without error"
  type        = bool
  default     = false
}

### description
variable "name" {
  description = "The logical name of the module instance"
  type        = string
}

### tags
variable "tags" {
  description = "The key-value maps for tagging"
  type        = map(string)
  default     = {}
}
