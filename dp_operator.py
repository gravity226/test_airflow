from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocWorkflowTemplateInstantiateOperator as WF
from datetime import datetime, timedelta

default_args = {
    'owner': 'Tommy J',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 16),
    # 'email': ['tommy.martin@gettectonic.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=120),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    'schedule_interval': '@once',
}

dag = DAG('pi-df-wf', default_args=default_args)

t1 = WF(
    task_id='init',
    template_id='wf-airflow',
    project_id='single-portal-216120',
    dag=dag)

t1


# No Workflow Template

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
    'retries': 0,
    'retry_delay': timedelta(minutes=120),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    'schedule_interval': '@once',
}

dag = DAG('pi-df-wf', default_args=default_args)

create = CO(
    cluster_name='SmoothOperator',
    project_id='single-portal-216120',
    num_workers=0,
    num_masters=1,
    master_machine_type='n1-standard-1',
    zone='us-central1-a',
    initialization_actions='gs://single-portal/init-env/init-actions.sh',
)

delete = DO(
    project_id='single-portal-216120',
    cluster_name='SmoothOperator',
)

create >> delete
