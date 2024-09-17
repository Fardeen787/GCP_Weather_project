from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime,timedelta
import requests
from google.cloud import bigquery

# Function to fetch weather data from the Weather API
def fetch_weather(**kwargs):
    api_key = 'd1170e94c3144fd3899102919241609'  # Replace with your Weather API key
    location = 'London'
    url = f"http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={location}&days=7"
    
    response = requests.get(url)
    data = response.json()

    # Log the raw data for debugging
    print(f"Fetched weather data: {data}")

    # Push the entire data to XCom for transformation
    kwargs['ti'].xcom_push(key='weather_data', value=data)

# Function to transform the weather data (extract current weather and forecast details)
def transform_weather(**kwargs):
    # Get weather data from XCom
    weather_data = kwargs['ti'].xcom_pull(key='weather_data')

    if not weather_data:
        raise ValueError("No weather data found!")

    transformed_data = {}

    # Process current weather
    current_weather = weather_data['current']
    transformed_data['current'] = {
        'last_updated': current_weather['last_updated'],
        'temp_c': current_weather['temp_c'],
        'temp_f': current_weather['temp_f'],
        'is_day': current_weather['is_day'],
        'weather_condition': current_weather['condition']['text'],
        'wind_mph': current_weather['wind_mph'],
        'humidity': current_weather['humidity'],
        'uv': current_weather['uv']
    }

    # Process forecast data (for simplicity, we only handle the first day of forecast)
    forecast_weather = weather_data['forecast']['forecastday'][0]['day']
    transformed_data['forecast'] = {
        'date': weather_data['forecast']['forecastday'][0]['date'],
        'maxtemp_c': forecast_weather['maxtemp_c'],
        'mintemp_c': forecast_weather['mintemp_c'],
        'avgtemp_c': forecast_weather['avgtemp_c'],
        'maxwind_mph': forecast_weather['maxwind_mph'],
        'daily_will_it_rain': forecast_weather['daily_will_it_rain'],
        'daily_chance_of_rain': forecast_weather['daily_chance_of_rain']
    }

    # Push transformed data to XCom
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

# Function to load transformed data into Google BigQuery
def load_weather(**kwargs):
    # Get transformed data from XCom
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data')

    if not transformed_data:
        raise ValueError("No transformed data available!")

    # Initialize BigQuery client
    client = bigquery.Client()

    # Define the dataset and table you want to insert the data into
    table_id = 'golden-plateau-434303-r2.weather_dataset.weather_table'  # Update with your project ID and dataset

    # Prepare rows to insert into BigQuery
    rows_to_insert = [
        {
            'last_updated': transformed_data['current']['last_updated'],
            'temp_c': transformed_data['current']['temp_c'],
            'temp_f': transformed_data['current']['temp_f'],
            'is_day': transformed_data['current']['is_day'],
            'weather_condition': transformed_data['current']['weather_condition'],
            'wind_mph': transformed_data['current']['wind_mph'],
            'humidity': transformed_data['current']['humidity'],
            'uv': transformed_data['current']['uv']
        },
        {
            'date': transformed_data['forecast']['date'],
            'maxtemp_c': transformed_data['forecast']['maxtemp_c'],
            'mintemp_c': transformed_data['forecast']['mintemp_c'],
            'avgtemp_c': transformed_data['forecast']['avgtemp_c'],
            'maxwind_mph': transformed_data['forecast']['maxwind_mph'],
            'daily_will_it_rain': transformed_data['forecast']['daily_will_it_rain'],
            'daily_chance_of_rain': transformed_data['forecast']['daily_chance_of_rain']
        }
    ]

    # Insert rows into the table
    errors = client.insert_rows_json(table_id, rows_to_insert)
    
    if errors == []:
        print("New rows added successfully.")
    else:
        print(f"Encountered errors while inserting rows: {errors}")

# Define default_args for the DAG
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG for the ETL process
dag = DAG(
    'weather_etl_to_bigquery',
    default_args=default_args,
    description='ETL weather data into BigQuery',
    schedule_interval='@daily',
)

# Define the tasks
fetch_task = PythonOperator(
    task_id='fetch_weather',
    python_callable=fetch_weather,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_weather',
    python_callable=transform_weather,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_weather',
    python_callable=load_weather,
    provide_context=True,
    dag=dag
)

# Set task dependencies
fetch_task >> transform_task >> load_task
