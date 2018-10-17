import datetime as dt
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import HttpSensor
from airflow.models import Variable
import json

args = {
    'owner': 'apireporting',
    'depends_on_past': False,
    'start_date': dt.datetime(2018, 02, 11),
    'retries': 1,
    'email': ['apireporting@adswerve.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG('weekly-dbm-pull', default_args=args, schedule_interval='0 17 * * 0', catchup=False)

def getDatesforPull():
    """
    Determine the last full DBM week (sun - sat)
    :return:
    """
    input=dt.date.today()
    start_delta = dt.timedelta(weeks=1)
    last = input-start_delta
    last = last.toordinal()
    sunday = last - (last % 7)
    saturday = sunday + 6
    return dt.date.fromordinal(sunday), dt.date.fromordinal(saturday)

def setPartnerEndPoint(ds, **kwargs):
    """
    Build the name of the endpoint and set the Airflow shared "statusEndpoint" variable
    """
    Variable.set('weekly_dbm_partner_pull-statusEndpoint', '/Partner/' + json.loads(kwargs['ti'].xcom_pull(task_ids='weekly_dbm_partner_pull'))['job-id'])

def setSyncEndPoint(ds, **kwargs):
    """
    Build the name of the endpoint and set the Airflow shared "statusEndpoint" variable
    """
    Variable.set('weekly_dbm_advertiser_sync-statusEndpoint', '/Sync/' + json.loads(kwargs['ti'].xcom_pull(task_ids='weekly_dbm_advertiser_sync'))['job-id'])

def responseCheck(response):
    """
    Evaluate the response from the job status call. done or not.
    """
    if json.loads(response.content)['job-status'] == 'COMPLETE':
        return True
    if json.loads(response.content)['job-status'] == 'INPROGRESS':
        return False
    if json.loads(response.content)['job-status'] == 'NEW':
        raise ValueError('Partner Data Pull Job does not exist')
    else:
        raise ValueError('Partner Data Pull Job has failed')

sun, sat = getDatesforPull()
startDate = sun.strftime('%Y-%m-%d')
endDate = sat.strftime('%Y-%m-%d')

# Update the advertiser list metadata
# Make the asynchronous call to the i2ap data job
t1 = SimpleHttpOperator(
    task_id='weekly_dbm_advertiser_sync',
    endpoint='/Sync',
    method='POST',
    data=json.dumps({"table-name": "advertiser_list"}),
    headers={"Content-Type": "application/json",
             "Tt-I2ap-Id": "i2ap-service@tt-cust-adswerve.iam.gserviceaccount.com",
             "Tt-I2ap-Sec": "E8OLhEWWihzdpIz5"},
    http_conn_id='i2ap_processor',
    xcom_push=True,
    dag=dag)

# retrieve the job id associated with the async call in t1
t2 = PythonOperator(
    task_id='weekly_dbm_advertiser_sync_jobid',
    python_callable=setSyncEndPoint,
    provide_context=True,
    dag=dag
)

t3 = HttpSensor(
    task_id='weekly_dbm_advertiser_sync_status',
    http_conn_id='i2ap_processor',
    endpoint=Variable.get('weekly_dbm_advertiser_sync-statusEndpoint'),
    headers={"Content-Type": "application/json",
             "Tt-I2ap-Id": "i2ap-service@tt-cust-adswerve.iam.gserviceaccount.com",
             "Tt-I2ap-Sec": "E8OLhEWWihzdpIz5"},
    response_check=responseCheck,
    poke_interval=60,
    dag=dag)

# Make the asynchronous call to the i2ap data job
t4 = SimpleHttpOperator(
    task_id='weekly_dbm_partner_pull',
    endpoint='/Partner',
    method='POST',
    data=json.dumps({"start-date": startDate,
                     "end-date": endDate,
                     "restrict": "True",
                     "history": "False",
                     "version": Variable.get('weekly_dbm_partner_pull-version')}),
    headers={"Content-Type": "application/json",
             "Tt-I2ap-Id": "i2ap-service@tt-cust-adswerve.iam.gserviceaccount.com",
             "Tt-I2ap-Sec": "E8OLhEWWihzdpIz5"},
    http_conn_id='i2ap_processor',
    xcom_push=True,
    dag=dag)

# retrieve the job id associated with the async call in t1
t5 = PythonOperator(
    task_id='weekly_dbm_partner_pull_jobid',
    python_callable=setPartnerEndPoint,
    provide_context=True,
    dag=dag
)

# loop on the status pull until the status is ERROR or COMPLETE
t6 = HttpSensor(
    task_id='weekly_dbm_partner_pull_status',
    http_conn_id='i2ap_processor',
    endpoint=Variable.get('weekly_dbm_partner_pull-statusEndpoint'),
    headers={"Content-Type": "application/json",
             "Tt-I2ap-Id": "i2ap-service@tt-cust-adswerve.iam.gserviceaccount.com",
             "Tt-I2ap-Sec": "E8OLhEWWihzdpIz5"},
    response_check=responseCheck,
    poke_interval=60,
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)
t6.set_upstream(t5)