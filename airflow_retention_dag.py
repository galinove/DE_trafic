from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow import configuration
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

DAG_NAME = 'fwteam26_GPcleanUp_DAG'
GP_CONN_ID = 'galinov'

SQL_STAT = "delete from fw_kafka_buffer where age(now(),to_timestamp(date, 'yyyy-mm-dd'))>interval '4 month'"
args = {
        'owner': 'galinov',
        'start_date': datetime(2023,9,27),
        'retries': 21,
        'retry_delay': timedelta(seconds=60)
        }

with DAG(DAG_NAME, description='truncale old trafic data drom GP',
    schedule_interval='0 0 * * *',
    catchup=False,
    max_active_runs=1,
    default_args=args,
    params={'labels':{'env': 'prod', 'priority': 'high'}}) as dag:
  sql_operator=PostgresOperator(task_id='sql_task',
      sql=SQL_STAT,
      postgres_conn_id=GP_CONN_ID,
      autocommit=True)

  sql_operator








