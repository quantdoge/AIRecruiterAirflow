from airflow import DAG
from airflow.operators.email import EmailOperator
from datetime import datetime

with DAG(
    dag_id="test_email_smtp",
    start_date=datetime(2025, 7, 5),
    schedule_interval=None,  # Run manually
    catchup=False,
) as dag:

    send_test_email = EmailOperator(
        task_id="send_test_email",
        to="ce9d919@protonmail.com",  # Replace with your email
        subject="Airflow SMTP Test",
        html_content="<h3>This is a test email from Airflow SMTP configuration.</h3>",
    )
