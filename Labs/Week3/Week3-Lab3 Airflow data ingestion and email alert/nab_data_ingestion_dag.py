
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd

#loading the data set into a dataframe
RAW_FILE = "/opt/airflow/data/raw/nba.csv"
PROCESSED_FILE = "/opt/airflow/data/processed/cleaned_csv.csv"

default_args = {
    "owner": "sports_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}
def validate_sales_data():
    df = pd.read_csv(RAW_FILE)
    if df.isnull().values.any():
        #raise ValueError("Null values detected!")
        print("Warning: Null values Found. Filling with 0")
    if (df["Salary"] < 0).any():
        print("Warning : Negative prices found.Dropping those rows")
    df['Salary']=pd.to_numeric(df['Salary'],errors='coerce')
    df=df.fillna(0)
    df=df[df["Salary"]>=0]
    df=df.drop_duplicates(keep="first")
    df['College']=df['College'].fillna('Unknown')
    
    print("Validation successful!")
    df.to_csv(PROCESSED_FILE, index=False)
def transform_sales_data():
    df=pd.read_csv(PROCESSED_FILE)
    df=df.copy()
   
   #Filtering data based on a requirement
    filtered_df = df[
        (df["Team"] == "Boston Celtics")
        & (df["Age"] > 20)
        & (df["Salary"] > 100000.0)
    ].copy()

    total_celtics_salary = filtered_df["Salary"].sum()
    filtered_df["highest_salary"] = total_celtics_salary
    print(
        f"The total salary for Boston Celtics is: {total_celtics_salary}")

    filtered_df.to_csv(PROCESSED_FILE, index=False)
    print("Transformation completed!")

with DAG(
    dag_id="nba_data_ingestion_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:


    wait_for_file = FileSensor(
        task_id="wait_for_nba_file",
        filepath=RAW_FILE,
        poke_interval=30,
        timeout=300,
        mode="poke"
    )

    validate_task = PythonOperator(
        task_id="validate_sales_data",
        python_callable=validate_sales_data
    )

    transform_task = PythonOperator(
        task_id="transform_sales_data",
        python_callable=transform_sales_data
    )

    wait_for_file >> validate_task >> transform_task
