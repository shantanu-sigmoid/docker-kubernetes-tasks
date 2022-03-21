CREATE TABLE IF NOT EXISTS {{params.table_name}} (
    id INT PRIMARY KEY,
    dag_id VARCHAR NOT NULL,
    execution_date VARCHAR NOT NULL
);
INSERT INTO {{params.table_name}}(id, dag_id, execution_date)
SELECT id, dag_id, execution_date from dag_run
ON CONFLICT (id)
DO NOTHING;

