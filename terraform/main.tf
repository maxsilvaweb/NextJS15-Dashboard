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

# Create Heroku app
resource "heroku_app" "duel_app" {
  name   = "duel-users-advocacy-platform"  # Change this to something unique
  region = "eu"
  
  config_vars = {
    NODE_ENV = "production"
    PROJECT_NAME = var.project_name
    MANAGED_BY = "terraform"
  }
}

# Add PostgreSQL database
resource "heroku_addon" "database" {
  app_id = heroku_app.duel_app.id
  plan   = "heroku-postgresql:essential-0"  # Free tier
}

# Optional: Add Redis for caching
resource "heroku_addon" "redis" {
  app_id = heroku_app.duel_app.id
  plan   = "heroku-redis:mini"  # Paid tier, use "heroku-redis:hobby-dev" for free
}