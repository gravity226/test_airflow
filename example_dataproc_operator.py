from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator as CO
from airflow.contrib.operators.dataproc_operator import DataprocClusterDeleteOperator as DO
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator as PSO

default_args = {
    'owner': 'Tommy J',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 16),
    # 'email': ['tommy.martin@gettectonic.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    'schedule_interval': '@once',
}

dag = DAG('pi-df-wf', default_args=default_args)

create = CO(
  task_id='create-cluster',
  cluster_name='smooth-operator',
  project_id='single-portal-216120',
  num_workers=2,
  num_masters=1,
  master_machine_type='n1-standard-1',
  worker_machine_type='n1-standard-1',
  zone='us-central1-a',
  initialization_actions='gs://single-portal/init-env/init-actions.sh',
  dag=dag,
)

estimate_pi = PSO(
  task_id='estimate-pi',
  cluster_name='smooth-operator',
  project_id='single-portal-216120',
  main='gs://us-central1-tandem-jump-81e8a133-bucket/dags/python_scripts/estimate_pi.py',
  gcp_conn_id='google_cloud_default',
  dag=dag
)

delete = DO(
  task_id='delete-cluster',
  project_id='single-portal-216120',
  cluster_name='smooth-operator',
  dag=dag,
)

create >> estimate_pi >> delete
