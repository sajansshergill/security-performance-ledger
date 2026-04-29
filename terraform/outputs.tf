output "database_name" {
  value = snowflake_database.security_master.name
}

output "raw_schema_name" {
  value = snowflake_schema.raw.name
}

output "warehouse_name" {
  value = snowflake_warehouse.compute.name
}
