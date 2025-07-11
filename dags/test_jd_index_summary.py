from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 31),
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
    dag_id='test_jd_index_summary',
    default_args=default_args,
    schedule_interval='0 */6 * * *',  # Every 6 hour
    tags=['AIRecruiter', 'ETL', '4_uat_index_summ'],
) as dag:

    task3 = BashOperator(
        task_id='4_uat_index_summ',
        bash_command='source /home/airflow/airflow-project/airflow-env/bin/activate && cd /home/airflow/airflow-project/AIRecruiter/ETL/Debug && python 4_uat_index_summ.py',
        #execution_timeout=timedelta(seconds=3600),  # Times out after 20 minutes
        #trigger_rule=TriggerRule.ALL_DONE,     # Run regardless of task2's result
    )

    task3
