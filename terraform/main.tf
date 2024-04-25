terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project     = var.projectId
  region      = var.region
  zone        = var.zone
  credentials = var.service_account_key_path
}

resource "google_bigquery_dataset" "nyc_citibike_data" {
  dataset_id                 = var.bq_dataset_name
  delete_contents_on_destroy = true
}
