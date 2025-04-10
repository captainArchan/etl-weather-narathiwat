from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import snowflake.connector
import os
from dotenv import load_dotenv
load_dotenv()
conn = snowflake.connector.connect(
    user = os.getenv("SNOWFLAKE_USERNAME"),
    password = os.getenv("SNOWFLAKE_PASSWORD"),
    account  = os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
    database = os.getenv("SNOWFLAKE_DATABASE"),
    schema = os.getenv("SNOWFLAKE_SCHEMA")
)
# ตั้งค่าคงที่
TOKEN = os.getenv("API_WEATHER")
# DATABASE_URL = os.getenv("DATABASE_URL")

HEADERS = {
    'accept': 'application/json',
    'authorization': 'Bearer {token}'.format(token=TOKEN)
}

# ฟังก์ชัน Extract
def fetch_weather_data(**kwargs):
    domain = 2
    province = 'นราธิวาส'
    # tambon='บางนาค',
    starttime = datetime.now().strftime("%Y-%m-%dT%H:00:00")
    field = 'tc,rh,rain,cond'
    data = requests.get(
        "https://data.tmd.go.th/nwpapi/v1/forecast/area/place?domain={domain}&province={province}&starttime={starttime}&fields={field}"
        .format(domain=domain, province=province, starttime=starttime, field=field)                                                                                           
        ,headers=HEADERS
        ).json()


    # ส่งต่อข้อมูลผ่าน XCom
    kwargs['ti'].xcom_push(key='weather_data', value={'data': data})

# ฟังก์ชัน Transform
def validate_and_transform_data(**kwargs):
    getData = kwargs['ti'].xcom_pull(key='weather_data', task_ids='fetch_weather_data')
    data = getData['data']

    time = []
    temperature = []
    rh = []
    rain = []
    weather = []
    lat = []
    lon = []

    for weatherInHour in data['WeatherForecasts']:
        dataWeather = weatherInHour['forecasts'][0]['data']
        dataLocation = weatherInHour['location']
        time.append(weatherInHour['forecasts'][0]['time'])
        temperature.append(dataWeather['tc'])
        rh.append(dataWeather['rh'])
        weather.append(dataWeather['cond'])
        rain.append(dataWeather['rain'])
        lat.append(dataLocation['lat'])
        lon.append(dataLocation['lon'])

    summaryWeather = {
        'time': time,
        'temperature': temperature,
        'humidity': rh,
        'rain': rain,
        'weather': weather,
        'latitude': lat,
        'longitude': lon
    }
    weatherDF = pd.DataFrame(summaryWeather, columns=['time', 'temperature', 'humidity', 'rain', 'weather', 'latitude', 'longitude'])

    kwargs['ti'].xcom_push(key='validated_data', value=weatherDF)

# ฟังก์ชัน Load
def load_data_to_db(**kwargs):
    weatherDF = kwargs['ti'].xcom_pull(key='validated_data', task_ids='validate_and_transform_data')
    selected_columns = weatherDF[['temperature', 'humidity', 'latitude', 'longitude', 'weather', 'time']]
    data_to_insert = selected_columns.values.tolist()
    insert_query = """
        INSERT INTO narathiwat_weather (temperature, humidity, latitude, longitude, weather, datetime)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
    try:
        with conn.cursor() as connection:
            connection.executemany(insert_query, data_to_insert)
            print("Data loaded to the database successfully")
    except Exception as e:
            print(f'ERROR: {e}')


# สร้าง DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'narathiwat_weather_etl',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='validate_and_transform_data',
        python_callable=validate_and_transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_data_to_db,
        provide_context=True
    )

    fetch_task >> transform_task >> load_task
