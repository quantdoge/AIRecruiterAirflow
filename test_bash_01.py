from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 24),
    'catchup': False
}

with DAG(
    dag_id='test_bash_operator',
    default_args=default_args,
    schedule_interval='* * * * *',  # Runs every minute
    tags=['bash', 'airflow']
) as dag:
    
    run_script = BashOperator(
        task_id='execute_test_sh',
        bash_command='/home/airflow/airflow_python_activate.sh',
    )

    run_script
