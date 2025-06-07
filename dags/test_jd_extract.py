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
    schedule_interval='0 */1 * * *',  # Every 1 hour 
    tags=['AIRecruiter', 'ETL', '2_uat_continuous_all'],
) as dag:

    task1 = BashOperator(
        task_id='2_uat_continuous_all',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 2_uat_continuous_all.py',  # Example: fails
    )

    task1
