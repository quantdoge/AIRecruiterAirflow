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
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && python /home/airflow/airflow-project/AIRecruiter/test_sh.py',
    )

    run_script
