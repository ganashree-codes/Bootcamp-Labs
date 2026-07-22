from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd

PROCESSED_FILE = "/opt/airflow/data/processed/cleaned_csv.csv"

default_args = {
    "owner": "analytics_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

def check_sales(**context):

    df = pd.read_csv(PROCESSED_FILE)

    total_salary = df["highest_salary"].iloc[0]

    print(f"Total Salary = {total_salary}")

    if total_salary < 600000000:
        context["ti"].xcom_push(
            key="alert_message",
            value=f"ALERT: Total Salary is less  than the thresholdf : {total_salary}"
        )

with DAG(
    dag_id="salary_alert_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    sales_check_task = PythonOperator(
        task_id="check_sales",
        python_callable=check_sales
    )

    send_email = EmailOperator(
        task_id="send_email_alert",
        to="ganaakash16@gmail.com",
        from_email="gana.drk@gmail.com",
        subject="Salary report Alert",
        html_content="""
        <h3>Salary Alert Triggered</h3>
        <p>Please check total salary report.</p>
        """,
        retries=3,
    )

    sales_check_task >> send_email
