"""from airflow import DAG
from airflow.providers.http import SimpleHttpOperator
from airflow.providers.postgres.hooks import postgres
from datetime import datetime
from airflow import task"""
import datetime
import json

import requests
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id="ml_pipeline",
    start_date=pendulum.datetime(2025, 10, 20, tz="UTC"),
    schedule=datetime.timedelta(days=1),  # Runs daily
    catchup=False,
) as dag:

    @task
    def create_table():
        #initialize postgresql Hook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
        
        #SQL query to create table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data(
            id SERIAL PRIMARY KEY,
            Title VARCHAR(225)
            Explamation TEXT,
            URL TEXT,
            Date DATE,
            Media_Type VARCHAR(50)
        );
        """
        #Excecute the SQL query
        postgres_hook.run(create_table_query)
        
        #Extract API DATA
        #https://api.nasa.gov/planetary/apod?api_key=7BbRvxo8uuzas9U3ho1RwHQQCkZIZtJojRIr293p
        extract_apod = SimpleHttpOperator(
            task_id = 'extract_apod',
            http_conn_id = 'nasa_api',
            endpoint = 'planetary/apod',
            method = 'GET',
            data = {"api_key":"{{conn.nasa.api.extra_dejson.api_key}}"},
            response_filter = lambda response: response.json(),
        )
        
        # Transform data
        @task
        def _transform_apod_data(response):
            apod_data = {
                'title': response.get('title', ''),
                'explanation': response.get('explanation', ''),
                'url': response.get('url', ''),
                'date': response.get('date', ''),
                'media_type': response.get('media_type', '')
            }
            return apod_data
        # Load data in postgresql
        @task
        def _load_data_to_postgres(apo_data):
            postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
            
            #Define SQL query
            insert_query = """
            INSERT INTO apo_data (Title, Explanation, URL, Date, Media_type)
            VALUES (%s, %s, %s, %s, %s)
            """
            
            #Execute SQL query
            postgres_hook.run(insert_query, parameters=(
                apo_data['title'],
                apo_data['explanation'],
                apo_data['url'],
                apo_data['date'],
                apo_data['media_type']
            ))
            
            # verify DB viewer
            
            #Define dependencies
            create_table() >> extract_apod
            api_response = extract_apod.data
            transform_apod_data=_transform_apod_data(api_response)
            _load_data_to_postgres(transform_apod_data)
            
        

