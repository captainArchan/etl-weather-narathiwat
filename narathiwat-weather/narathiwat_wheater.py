import requests 
from datetime import datetime, timedelta
import pandas as pd
from datetime import datetime
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

TOKEN = os.getenv("API_WEATHER")
# DATABASE_URL = os.getenv("DATABASE_URL")
headers = {
    'accept': 'application/json',
    'authorization': 'Bearer {token}'.format(token=TOKEN)
}

# def check_if_valid_data(df: pd.DataFrame) -> bool:
#     if df.empty:
#         print("No weather data today")
#         return False
#     if pd.Series(df['time']).is_unique:   
#         pass
#     else:
#         raise Exception("Duplicated primary key")
#     if df.isnull().values.any():
#         raise Exception("Null valued found")
#     return True

def main():
    domain = 2
    province = 'นราธิวาส'
    # tambon='บางนาค',
    starttime = datetime.now().strftime("%Y-%m-%dT%H:00:00")
    field = 'tc,rh,rain,cond'
    data = requests.get(
        "https://data.tmd.go.th/nwpapi/v1/forecast/area/place?domain={domain}&province={province}&starttime={starttime}&fields={field}"
        .format(domain=domain, province=province, starttime=starttime, field=field)                                                                                           
        ,headers=headers
        ).json()

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
    selected_columns = weatherDF[['temperature', 'humidity', 'latitude', 'longitude', 'weather', 'time']]
    data_to_insert = selected_columns.values.tolist()
    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO narathiwat_weather (temperature, humidity, latitude, longitude, weather, datetime)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data_to_insert)
        cursor.close()
        conn.close()
        print('SUCCESS')
    except Exception as e:
        print(f'ERROR: {e}')
    # time = []
    # temperature = []
    # humidity = []
    # weather = []
    # lat = []
    # lon = []
    # date=[]
    # # ข้อมูลที่ส่งมามีสองตัวซึ่งคิดว่าตัวเดียวก็พอแล้ว
    # location = {
    #         'lat': data['WeatherForecasts'][0]['location']['lat'],
    #         'lon': data['WeatherForecasts'][0]['location']['lon']
    #  }
    # for dataWeather in data['WeatherForecasts'][0]['forecasts']:
    #     isoToString = datetime.fromisoformat(dataWeather['time'])
    #     date.append(isoToString.date())
    #     time.append(isoToString.time())
    #     temperature.append(dataWeather['data']['tc'])
    #     humidity.append(dataWeather['data']['rh'])
    #     weather.append(dataWeather['data']['cond'])
    #     lat.append(location['lat'])
    #     lon.append(location['lon'])

    # summaryWeather = {
    #     'date': date,
    #     'time': time,
    #     'temperature': temperature,
    #     'humidity': humidity,
    #     'weather': weather,
    #     'latitude': lat,
    #     'longitude': lon
    # }
    # weatherDF = pd.DataFrame(summaryWeather, columns=['date', 'time', 'temperature', 'humidity', 'weather', 'latitude', 'longitude'])
    # print(weatherDF)
    # try:
    #     engine = create_engine(DATABASE_URL)
    #     with engine.connect() as connection:
    #         print("Connected to the database!")
    #         sql_query = text("""
    #                 CREATE TABLE IF NOT EXISTS WEATHERS (
    #                     date DATE NOT NULL,
    #                     time TIME NOT NULL,
    #                     temperature FLOAT,
    #                     humidity FLOAT,
    #                     weather INTEGER,
    #                     latitude FLOAT,
    #                     longitude FLOAT,
    #                     PRIMARY KEY (date, time)
    #                 );
    #         """)
    #         connection.execute(sql_query)
    #         weatherDF.to_sql('weathers', engine, if_exists='append', index=False, method='multi')
    # except Exception as e:
    #     print(f"Error: {e}")

if __name__=="__main__":
    main()


        
        