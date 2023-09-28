
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow import configuration
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

DAG_NAME = 'fwteam26_GPrefreshMVs_DAG'
GP_CONN_ID = 'galinov'

SQL_refresh1 = "refresh materialized view mv_school_trafic_data"

SQL_refresh2 = "refresh materialized view mv_school_trafic_data_byRegion"

SQL_refresh3 = "refresh materialized view mv_school_trafic_data_byOrganization"

SQL_refresh4 = "refresh materialized view mv_school_trafic_uniqObjects"

args = {
        'owner': 'galinov',
        'start_date': datetime(2023,9,27),
        'retries': 2,
        'retry_delay': timedelta(seconds=60)
        }

with DAG(DAG_NAME, description='refreshes all mv at GP',
    schedule_interval='0 * * * *',
    catchup=False,
    max_active_runs=1,
    default_args=args,
    params={'labels':{'env': 'prod', 'priority': 'high'}}) as dag:
  sql_operator1=PostgresOperator(task_id='sql_task1',
      sql=SQL_refresh1,
      postgres_conn_id=GP_CONN_ID,
      autocommit=True)
  sql_operator2=PostgresOperator(task_id='sql_task2',
      sql=SQL_refresh2,
      postgres_conn_id=GP_CONN_ID,
      autocommit=True)
  sql_operator3=PostgresOperator(task_id='sql_task3',
      sql=SQL_refresh3,
      postgres_conn_id=GP_CONN_ID,
      autocommit=True)
  sql_operator4=PostgresOperator(task_id='sql_task4',
      sql=SQL_refresh4,
      postgres_conn_id=GP_CONN_ID,
      autocommit=True)

  sql_operator1 >> sql_operator2 >> sql_operator3 >> sql_operator4


