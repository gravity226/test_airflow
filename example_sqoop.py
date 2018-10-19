from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator as CO
from airflow.contrib.operators.dataproc_operator import DataprocClusterDeleteOperator as DO
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator as PSO
from airflow.contrib.operators.dataproc_operator import DataProcHadoopOperator as HO

default_args = {
    'owner': 'Tommy J',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@once',
}

dag = DAG('sqoop-wf', default_args=default_args)

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
  cluster_name='sqoop-job',
  project_id='single-portal-216120',
  num_workers=2,
  num_masters=1,
  master_machine_type='n1-standard-1',
  worker_machine_type='n1-standard-1',
  zone='us-central1-a',
  init_actions_uris=['gs://single-portal/cloud-sql-proxy.sh'],
  properties={'hive:hive.metastore.warehouse.dir': 'gs://single-portal/hive-warehouse'},
  service_account_scopes=scopes,
  metadata={'enable-cloud-sql-hive-metastore': 'false',
            'additional-cloud-sql-instances': 'single-portal-216120:us-central1:myinstance=tcp:3307'
            },
  dag=dag,
)

t1 = HO(
  task_id='t1',
  cluster_name='sqoop-job', 
  project_id='single-portal-216120',
  gcp_conn_id='google_cloud_default',
  main_class='org.apache.sqoop.Sqoop',
  dataproc_hadoop_properties={
    'class': 'org.apache.sqoop.Sqoop',
    },
  dataproc_hadoop_jars=[
    'gs://single-portal/jars/sqoop-1.4.6-hadoop200.jar', 
    'gs://single-portal/jars/avro-tools-1.8.2-javadoc.jar', 
    'file:///usr/share/java/mysql-connector-java-5.1.42.jar',
    'file:///usr/lib/hive/lib/hive-exec.jar'
    ],
  arguments=[
    'import', 
    '-Dmapreduce.job.user.classpath.first=true',
    '--connect=jdbc:mysql://localhost:3307/guestbook',
    '--username=root',
    '--password=4321',
    '--table=entries',
    '--hive-import',
    ],
  dag=dag,
)

# from airflow.contrib.operators.sqoop_operator import SqoopOperator
# sqoop_mysql_export = SqoopOperator(conn_id='sqoop',
#                                    table='student',
#                                    username='root',
#                                    password='password',
#                                    driver='jdbc:mysql://mysql.example.com/testDb',
#                                    cmd_type='import')

delete = DO(
  task_id='delete-cluster',
  project_id='single-portal-216120',
  cluster_name='sqoop-job',
  dag=dag,
)

create >> t1 >> delete