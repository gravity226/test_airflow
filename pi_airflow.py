"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
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
    'schedule_interval': '@daily',
}

dag = DAG('pi-estimation', default_args=default_args)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='init',
    bash_command='./bash_dags/init.sh',
    dag=dag)

sleep1 = BashOperator(
    task_id='sleep1',
    bash_command='sleep 10',
    dag=dag)

t2 = BashOperator(
    task_id='run',
    bash_command='./bash_dags/run.sh',
    retries=0,
    dag=dag)

sleep2 = BashOperator(
    task_id='sleep2',
    bash_command='sleep 10',
    dag=dag)

t3 = BashOperator(
    task_id='cleanup',
    bash_command='./bash_dags/cleanup.sh',
    dag=dag)

t1 >> sleep1 >> t2 >> sleep2 >> t3
# t2.set_upstream(t1)
# t3.set_upstream(t1)
