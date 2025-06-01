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
    dag_id='test_jd_ingest',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    tags=['AIRecruiter', 'ETL', '3_uat_webingest_first'],
) as dag:

    task1 = BashOperator(
        task_id='3_uat_webingest_first',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 3_uat_webingest_first.py',  # Example: fails
    )

    task1
