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


from airflow.contrib.operators.dataproc_operator import




