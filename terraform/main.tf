provider "snowflake" {
  account  = var.snowflake_account
  user     = var.snowflake_user
  password = var.snowflake_password
  role     = var.snowflake_role
}

resource "snowflake_database" "security_master" {
  name    = var.database_name
  comment = "Security master and performance ledger warehouse."
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.security_master.name
  name     = "RAW"
  comment  = "Raw ingestion tables loaded by Airflow."
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.security_master.name
  name     = "STAGING"
  comment  = "dbt staging models."
}

resource "snowflake_schema" "intermediate" {
  database = snowflake_database.security_master.name
  name     = "INTERMEDIATE"
  comment  = "dbt intermediate golden-record models."
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.security_master.name
  name     = "MARTS"
  comment  = "dbt dimensional and fact models."
}

resource "snowflake_warehouse" "compute" {
  name           = var.warehouse_name
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
  initially_suspended = true
}
