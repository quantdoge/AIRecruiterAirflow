from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 24),
    'catchup': False
}

with DAG(
    dag_id='test_jd_etl',
    default_args=default_args,
    schedule_interval='0 */6 * * *',  # Every 6 hours
    tags=['AIRecruiter', 'ETL', '2_uat_continuous','3_uat_webingest_first','4_uat_index_summ', '5_uat_upsert_md'],
) as dag:

    task1 = BashOperator(
        task_id='2_uat_continuous',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 2_uat_continuous.py',  # Example: fails
    )

    task2 = BashOperator(
        task_id='3_uat_webingest_first',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 3_uat_webingest_first.py',  # Example: succeeds
        trigger_rule=TriggerRule.ALL_DONE,     # Run regardless of task1's result
    )

    task3 = BashOperator(
        task_id='4_uat_index_summ',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 4_uat_index_summ.py',
        trigger_rule=TriggerRule.ALL_DONE,     # Run regardless of task2's result
    )

    task4 = BashOperator(
        task_id='5_uat_upsert_md',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 5_uat_upsert_md.py',
        trigger_rule=TriggerRule.ALL_DONE,     # Run regardless of task2's result
    )

    task1 >> task2 >> task3 >> task4
