from datetime import datetime
from datetime import timedelta
from functools import partial

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags.lib import email
from dags.lib import presto

DAG_OWNER = ''
dag_start_dt = datetime(2023, 10, 7) # this should be replaced


default_args = {
    'owner': DAG_OWNER,
    'start_date': dag_start_dt,
    'on_failure_callback': partial(email.send_mail_slack, [listof emails, here one email id with be the value inside DAG_OWNER field]),
    'retries': 2,
    'retry_delay': timedelta(seconds=300),
}


dag = DAG(
    'dag_name',
    tags=['presto'],
    default_args=default_args,
    schedule_interval='here cron_schedule will come',
    catchup=False,
)
