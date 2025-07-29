terraform {
  required_providers {
    heroku = {
      source  = "heroku/heroku"
      version = "~> 5.0"
    }
  }
}

provider "heroku" {
  # Configuration options
}

# Variables
variable "project_name" {
  description = "duel_"
  type        = string
  default     = "duel-api"
}

variable "app_name" {
  description = "Heroku app name"
  type        = string
  default     = "duel-users-api"
}

variable "environment" {
  description = "Environment (staging/production)"
  type        = string
  default     = "production"
}

# Create Heroku app for NestJS API
resource "heroku_app" "nestjs_api" {
  name   = var.app_name
  region = "eu"
  stack  = "heroku-22"
  
  config_vars = {
    NODE_ENV = var.environment
    PROJECT_NAME = var.project_name
    MANAGED_BY = "terraform"
    
    # API Configuration
    PORT = "3000"
    API_VERSION = "v1"
    
    # CORS Configuration
    CORS_ORIGIN = "*"
    
    # Rate Limiting
    THROTTLE_TTL = "60"
    THROTTLE_LIMIT = "100"
    
    # Logging
    LOG_LEVEL = "info"
  }
  
  buildpacks = [
    "heroku/nodejs"
  ]
}

# PostgreSQL Database
resource "heroku_addon" "database" {
  app_id = heroku_app.nestjs_api.id
  plan   = "heroku-postgresql:essential-0"  # Upgrade to standard-0 for production
}

# Redis for caching and session management
resource "heroku_addon" "redis" {
  app_id = heroku_app.nestjs_api.id
  plan   = "heroku-redis:mini"  # Upgrade to premium for production
}

# Papertrail for logging (optional)
resource "heroku_addon" "papertrail" {
  app_id = heroku_app.nestjs_api.id
  plan   = "papertrail:choklad"  # Free tier
}

# New Relic for monitoring (optional)
resource "heroku_addon" "newrelic" {
  app_id = heroku_app.nestjs_api.id
  plan   = "newrelic:wayne"  # Free tier
}

# Attach existing database if you want to reuse it
# resource "heroku_addon_attachment" "existing_database" {
#   app_id  = heroku_app.nestjs_api.id
#   addon_id = "your-existing-postgres-addon-id"
#   name    = "DATABASE"
# }

# Formation (dyno configuration)
resource "heroku_formation" "web" {
  app_id   = heroku_app.nestjs_api.id
  type     = "web"
  quantity = 1
  size     = "eco"  # Free tier, upgrade to "basic" or "standard-1x" for production
}

# Domain configuration (optional)
# resource "heroku_domain" "api_domain" {
#   app_id   = heroku_app.nestjs_api.id
#   hostname = "api.yourdomain.com"
# }

# Pipeline for staging/production (optional)
resource "heroku_pipeline" "api_pipeline" {
  name = "${var.project_name}-pipeline"
}

resource "heroku_pipeline_coupling" "api_coupling" {
  app_id   = heroku_app.nestjs_api.id
  pipeline = heroku_pipeline.api_pipeline.id
  stage    = var.environment
}

# Outputs
output "app_name" {
  value = heroku_app.nestjs_api.name
}

output "app_url" {
  value = "https://${heroku_app.nestjs_api.name}.herokuapp.com"
}

output "database_url" {
  value = heroku_addon.database.config_vars["DATABASE_URL"]
  sensitive = true
}

output "redis_url" {
  value = heroku_addon.redis.config_vars["REDIS_URL"]
  sensitive = true
}