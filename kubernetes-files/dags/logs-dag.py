from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta




default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 10),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


with DAG("time_of_dag_execution", default_args=default_args, schedule_interval="0 6 * * *", catchup = False) as dag:

    t1 = DummyOperator(task_id="start_task")
    t2 = DummyOperator(task_id="end_task")

    # t2 = PostgresOperator(task_id="create_new_table_in_postgre", postgres_conn_id='postgres_conn', \
    #     sql="sql/create_new_table.sql", params = {"table_name": "time_of_dag_exec"})

    t1 >> t2