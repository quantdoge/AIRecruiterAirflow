from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 31),
    'catchup': False
}

with DAG(
    dag_id='test_jd_extract',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    tags=['AIRecruiter', 'ETL', '2_uat_continuous'],
) as dag:

    task1 = BashOperator(
        task_id='2_uat_continuous',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 2_uat_continuous.py',  # Example: fails
    )

    task1
