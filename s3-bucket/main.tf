module "s3_bucket" {
  source                                = "./terraform-aws-s3-bucket"
  create_bucket                         = true
  bucket                                = var.bucket_name
  attach_policy                         = true
  force_destroy                         = true
  policy                                = null
  attach_deny_insecure_transport_policy = true
  attach_require_latest_tls_policy      = true
  control_object_ownership              = true
  expected_bucket_owner                 = var.expected_bucket_owner
}
resource "aws_s3_object" "object" {
  bucket = module.s3_bucket.s3_bucket_id

  for_each = fileset("script/", "*")
  key      = each.value
  source   = "script/${each.value}"
  etag     = filemd5("script/${each.value}")

}

