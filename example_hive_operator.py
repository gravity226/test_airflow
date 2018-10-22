from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator as CO
from airflow.contrib.operators.dataproc_operator import DataprocClusterDeleteOperator as DO
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator as HiveOperator

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

dag = DAG('hive-wf', default_args=default_args)

scopes = [
  'https://www.googleapis.com/auth/devstorage.read_only',
  'https://www.googleapis.com/auth/logging.write',
  'https://www.googleapis.com/auth/monitoring.write',
  'https://www.googleapis.com/auth/pubsub',
  'https://www.googleapis.com/auth/service.management.readonly',
  'https://www.googleapis.com/auth/servicecontrol',
  'https://www.googleapis.com/auth/trace.append',
  'https://www.googleapis.com/auth/sqlservice.admin'
]

create = CO(
  task_id='create-cluster',
  cluster_name='smooth-operator',
  project_id='single-portal-216120',
  num_workers=2,
  num_masters=1,
  master_machine_type='n1-standard-1',
  worker_machine_type='n1-standard-1',
  zone='us-central1-a',
  properties={'hive:hive.metastore.warehouse.dir': 'gs://single-portal/hive-warehouse'}, # /entries
  service_account_scopes=scopes,
  metadata={'enable-cloud-sql-hive-metastore': 'false',
            'additional-cloud-sql-instances': 'single-portal-216120:us-central1:myinstance=tcp:3307'
            },
  dag=dag,
)

query = '''
COMMENT 'Employee details'
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED IN TEXT FILE
'''

t1 = HiveOperator(
  task_id='create-database',
  cluster_name='smooth-operator',
  project_id='single-portal-216120',
  # query='gs://us-central1-tandem-jump-1d5d11f4-bucket/dags/queries/hive.q',
  query='CREATE DATABASE IF NOT EXISTS userdb;',
  gcp_conn_id='google_cloud_default',
  dag=dag,
)

query = '''
CREATE TABLE IF NOT EXISTS employee ( eid int, name String,
salary String, destination String)
COMMENT 'Employee details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
'''

t2 = HiveOperator(
  task_id='create-table',
  cluster_name='smooth-operator',
  project_id='single-portal-216120',
  # query='gs://us-central1-tandem-jump-1d5d11f4-bucket/dags/queries/hive.q',
  query=query,
  gcp_conn_id='google_cloud_default',
  dag=dag,
)

delete = DO(
  task_id='delete-cluster',
  project_id='single-portal-216120',
  cluster_name='smooth-operator',
  # wait_for_downstream=True,
  dag=dag,
)

create >> t1 >> t2 >> delete
