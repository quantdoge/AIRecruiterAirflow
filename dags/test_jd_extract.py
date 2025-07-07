from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 6),
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
    dag_id='test_jd_extract',
    default_args=default_args,
    schedule_interval='0 */1 * * *',  # Every 1 hour
    tags=['AIRecruiter', 'ETL', '2_uat_continuous_all'],
    on_success_callback=success_email_function,
) as dag:

    task1 = BashOperator(
        task_id='2_uat_continuous_all',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 2_uat_continuous_all.py',  # Example: fails
    )

    task1
