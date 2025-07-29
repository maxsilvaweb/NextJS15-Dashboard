variable "project_name" {
  description = "duel_"
  type        = string
  default     = "duel"
}

variable "heroku_region" {
  description = "Heroku region"
  type        = string
  default     = "eu"  # or "eu" for Europe
}

variable "app_stack" {
  description = "Heroku stack"
  type        = string
  default     = "heroku-22"  # Latest stack
}