from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 11),
    'catchup': False,
    'email': ["ce9d919@protonmail.com"],
    'email_on_failure': True
}

def success_email_function(context):
    dag_run = context.get("dag_run")
    subject = f"DAG {dag_run.dag_id} Succeeded"
    msg = f"The DAG {dag_run.dag_id} has completed successfully."
    send_email(to=["ce9d919@protonmail.com"], subject=subject, html_content=msg)

with DAG(
    dag_id='test_jd_etl',
    default_args=default_args,
    schedule_interval='0 */6 * * *',  # Every 6 hour
    on_success_callback=success_email_function,
    tags=['AIRecruiter', 'ETL', '3_uat_webingest_first','4_uat_index_summ', '5_uat_upsert_md', '6_pg_to_md'],
) as dag:

    task2 = BashOperator(
        task_id='3_uat_webingest_first',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 3_uat_webingest_first.py',  # Example: succeeds
        execution_timeout=timedelta(seconds=7200),  # Times out after 2 hours
        trigger_rule=TriggerRule.ALL_DONE,     # Run regardless of task1's result
    )

    task3 = BashOperator(
        task_id='4_uat_index_summ',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 4_uat_index_summ.py',
        execution_timeout=timedelta(seconds=10800),  # Times out after 3 hours
        trigger_rule=TriggerRule.ALL_DONE,     # Run regardless of task2's result
    )

    task4 = BashOperator(
        task_id='5_uat_upsert_md',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 5_uat_upsert_md.py',
        execution_timeout=timedelta(seconds=3600),  # Times out after 1 hour
        trigger_rule=TriggerRule.ALL_DONE,     # Run regardless of task3's result
    )

    task5 = BashOperator(
        task_id='6_pg_to_md',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 6_pg_to_md.py',  # Example: succeeds
        execution_timeout=timedelta(seconds=1200),  # Times out after 20 minutes
        trigger_rule=TriggerRule.ALL_DONE,     # Run regardless of task4's result
    )


    task2>> task3>> task4 >> task5
