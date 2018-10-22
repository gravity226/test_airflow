from datetime import timedelta, datetime
import json
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

default_args = {
    'owner': 'airflow',
    # 'depends_on_past': True,
    'start_date': datetime(2018, 10, 21),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

schedule_interval = "00 21 * * *"

dag = DAG('bigquery_github_trends_v1', default_args=default_args, schedule_interval=schedule_interval)

query = '''
#legacySql
SELECT table_id
FROM [githubarchive:day.__TABLES__]
WHERE table_id = "{{ yesterday_ds_nodash }}"
'''

query = '''
SELECT * FROM TommyJ.product_history_done
  WHERE date = DATE("2018-01-04")
'''

query = '''
SELECT
  CONCAT(
    'https://stackoverflow.com/questions/',
    CAST(id as STRING)) as url,
  view_count
FROM `bigquery-public-data.stackoverflow.posts_questions`
WHERE tags like '%google-bigquery%'
ORDER BY view_count DESC
LIMIT 10
'''

t1 = BigQueryCheckOperator(
    task_id='bq_check_githubarchive_day',
    # use_legacy_sql=False,
    sql=query,
    project_id='tt-cust-analytics',
    dag=dag)

# t2 = BigQueryCheckOperator(
#     task_id='bq_check_hackernews_full',
#     sql='''
#     #legacySql
#     SELECT
#     STRFTIME_UTC_USEC(timestamp, "%Y%m%d") as date
#     FROM
#       [bigquery-public-data:hacker_news.full]
#     WHERE
#     type = 'story'
#     AND STRFTIME_UTC_USEC(timestamp, "%Y%m%d") = "{{ yesterday_ds_nodash }}"
#     LIMIT 1
#     ''',
#     project_id='single-portal-216120',
#     dag=dag)

'''
bq mk --time_partitioning_type=DAY single-portal-216120:github_trends.github_daily_metrics
bq mk --time_partitioning_type=DAY single-portal-216120:github_trends.github_agg
bq mk --time_partitioning_type=DAY single-portal-216120:github_trends.hackernews_agg
bq mk --time_partitioning_type=DAY single-portal-216120:github_trends.hackernews_github_agg
'''

# t3 = BigQueryOperator(
#   task_id='bq_write_to_github_daily_metrics',
#   use_legacy_sql=False,
#   # write_disposition='WRITE_TRUNCATE',
#   # allow_large_results=True,
#   bql='''
#   SELECT * FROM TommyJ.product_history_done
#     WHERE date = DATE("2018-01-04")
#   ''',    
#   # destination_dataset_table='airflow-cloud-public-datasets.github_trends.github_daily_metrics${{ yesterday_ds_nodash }}',
#   project_id='tt-cust-analytics',
#   dag=dag)

t1
