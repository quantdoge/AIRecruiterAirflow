from airflow.utils.email import send_email

send_email(
    to="limjs4spotify@gmail.com",
    subject="Airflow SMTP Direct Test",
    html_content="Testing Airflow SMTP from Python shell."
)
