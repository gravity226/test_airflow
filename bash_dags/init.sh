echo "ProjectId"
echo "-------------"
echo $gcp_project
echo "-------------"

# Delete Workflow if exists
gcloud beta dataproc workflow-templates delete wf-airflow \
  --project single-portal-216120 -q

# Create a Workflow Template
gcloud beta dataproc workflow-templates --project=single-portal-216120 \
  create wf-airflow

# Add a Managed Cluster to the Template
gcloud beta dataproc workflow-templates set-managed-cluster wf-airflow \
  --project=single-portal-216120 \
  --num-workers=0 \
  --num-masters=1 \
  --zone=us-central1-a \
  --master-machine-type n1-standard-1 \
  --worker-machine-type n1-standard-1 \
  --initialization-actions gs://single-portal/init-env/init-actions.sh \
  --cluster-name estimate-pi

# Adding Jobs to the Template
gcloud beta dataproc workflow-templates add-job pyspark \
  --step-id foo \
  --project single-portal-216120 \
  --workflow-template wf-airflow gs://single-portal/test_dataproc/estimate_pi.py
