variable "snowflake_account" {
  type        = string
  description = "Snowflake account identifier."
}

variable "snowflake_user" {
  type        = string
  description = "Snowflake user for provisioning."
}

variable "snowflake_password" {
  type        = string
  description = "Snowflake password for provisioning."
  sensitive   = true
}

variable "snowflake_role" {
  type        = string
  description = "Role used by Terraform."
  default     = "SYSADMIN"
}

variable "database_name" {
  type        = string
  description = "Security master database name."
  default     = "SECURITY_MASTER"
}

variable "warehouse_name" {
  type        = string
  description = "Warehouse used by ingestion and dbt."
  default     = "COMPUTE_WH"
}
