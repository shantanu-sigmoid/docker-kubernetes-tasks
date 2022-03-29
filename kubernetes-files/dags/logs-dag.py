from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

import psycopg2


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 10),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

query = """
CREATE TABLE IF NOT EXISTS dag_run_copy (
    id INT PRIMARY KEY,
    dag_id VARCHAR NOT NULL,
    execution_date VARCHAR NOT NULL
);
INSERT INTO dag_run_copy(id, dag_id, execution_date)
SELECT id, dag_id, execution_date from dag_run
ON CONFLICT (id)
DO NOTHING;
"""

def execute_query_with_psycopg():
    # postgresql://airflow:airflow@postgres-service-db:5432/airflow
    conn = psycopg2.connect(host="postgres-service-db", database = "airflow", user = "airflow", password = "airflow", port = "5432")
    cur = conn.cursor()
    cur.execute(query)


with DAG("time_of_dag_execution", default_args=default_args, schedule_interval="0 6 * * *", catchup = False) as dag:

    t1 = DummyOperator(task_id="start_task")

    # t2 = PostgresOperator(task_id="create_new_table_in_postgre", postgres_conn_id='postgres_conn', \
    #     sql="sql/create_new_table.sql", params = {"table_name": "time_of_dag_exec"})
    t3 = PythonOperator(task_id="create_table", python_callable = execute_query_with_psycopg)
    t1 >> t3