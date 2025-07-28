output "app_name" {
  description = "Name of the Heroku app"
  value       = heroku_app.duel_app.name
}

output "app_url" {
  description = "URL of the Heroku app"
  value       = "https://${heroku_app.duel_app.name}.herokuapp.com"
}

output "database_url" {
  description = "Database URL"
  value       = heroku_addon.database.config_var_values["DATABASE_URL"]
  sensitive   = true
}