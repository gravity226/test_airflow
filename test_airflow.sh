# Set an environment Variable in Airflow
gcloud composer environments run miastro \
  --project=single-portal-216120 \
  --location us-central1 variables -- --set Tommy Developer


# Generate 2 variables
gcloud composer environments run miastro \
  --project=single-portal-216120 \
  --location us-central1 variables -- --set gcp_project single-portal-216120

gcloud composer environments run miastro \
  --project=single-portal-216120 \
  --location us-central1 variables -- --set gcs_bucket gs://single-portal

gcloud composer environments run miastro \
  --project=single-portal-216120 \
  --location us-central1 variables -- --set gce_zone us-central1-f

# Get variables
gcloud composer environments run miastro \
  --project=single-portal-216120 \
  --location us-central1 variables -- --get gcs_bucket


# gcloud composer environments update miastro \
#   --project=single-portal-216120 \
#   --location us-central1 \
#   --update-env-variables=gcs_bucket=gs://single-portal,gcp_project=project-id,gce_zone=us-central1-f

