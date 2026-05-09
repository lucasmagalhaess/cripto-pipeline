terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file("cripto-pipeline-495818-1cf9ea4ec480.json")
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true
}

resource "google_storage_bucket" "functions_bucket" {
  name          = "${var.project_id}-functions"
  location      = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics"
  location   = var.region
}

resource "google_bigquery_table" "cripto_prices" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "cripto_prices"
  deletion_protection = false

  schema = jsonencode([
    { name = "coin_id", type = "STRING" },
    { name = "symbol", type = "STRING" },
    { name = "name", type = "STRING" },
    { name = "price_usd", type = "FLOAT" },
    { name = "price_brl", type = "FLOAT" },
    { name = "market_cap_usd", type = "FLOAT" },
    { name = "volume_24h_usd", type = "FLOAT" },
    { name = "price_change_24h", type = "FLOAT" },
    { name = "price_change_7d", type = "FLOAT" },
    { name = "ath_usd", type = "FLOAT" },
    { name = "ath_change_percent", type = "FLOAT" },
    { name = "extraction_date", type = "STRING" },
    { name = "extraction_timestamp", type = "STRING" }
  ])
}
