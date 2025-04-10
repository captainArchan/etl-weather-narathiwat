from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd

# กำหนดค่าเบื้องต้น
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# สร้าง DAG
with DAG(
    dag_id='test_weather_etl',
    default_args=default_args,
    description='A simple ETL pipeline for testing',
    schedule_interval='@hourly',  # รันทุกชั่วโมง
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task: Extract Data
    def fetch_weather_data():
        print("Fetching data from API...")
        try:
            # URL และ Headers ตัวอย่าง
            url = "https://jsonplaceholder.typicode.com/posts/1"  # ใช้ API ทดสอบ
            response = requests.get(url)
            response.raise_for_status()
            print("Data fetched successfully!")
            print(response.json())
        except Exception as e:
            print(f"Error in fetching data: {e}")
            raise

    extract_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )

    # Task: Transform Data
    def transform_data():
        print("Transforming data...")
        data = {"time": ["2023-01-01 00:00:00"], "temperature": [25.5], "humidity": [85]}
        df = pd.DataFrame(data)
        print(f"Transformed DataFrame:\n{df}")

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    # Task: Load Data
    def load_data():
        print("Loading data... (Simulated)")
        # จำลองการโหลดโดยการพิมพ์ข้อมูล
        print("Data loaded successfully!")

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # ลำดับการทำงาน
    extract_task >> transform_task >> load_task
