import datetime
import pendulum

from airflow import DAG
from airflow import BashOperator
from airflow import PythonOperator

def _preprocess_data():
    """A Python function to simulate data processing."""
    print("Processing data: cleaning, transforming, and validating.")
    # In a real scenario, this would involve complex data manipulation
    # using libraries like pandas, numpy, etc.
    
def _train_model():
    print("Train: split and testing.")
    
    
def _evaluate_model():
    print("Evaluate: check model drifting with hypertuning.")
    

with DAG(
    dag_id="ml_pipeline",
    start_date=pendulum.datetime(2025, 10, 20, tz="UTC"),
    schedule=datetime.timedelta(days=1),  # Runs daily
    catchup=False,
    tags=["example", "data_pipeline"],
) as dag:
    # Task 1: Ingest raw data
    ingest_data = BashOperator(
        task_id="ingest_raw_data",
        bash_command="echo 'Downloading data from source...'",
    )

    # Task 2: Process the ingested data
    preprocess_data = PythonOperator(
        task_id="process_ingested_data",
        python_callable=_preprocess_data,
    )
    
    train_model = PythonOperator(
        task_id="train_model",
        python_callable=_train_model,
        
    )
    
    evaluate = PythonOperator(
        task_id="evaluate_model",
        python_callable=_evaluate_model,
    )

    # Task 3: Load processed data into a data warehouse
    load_data = BashOperator(
        task_id="load_processed_data",
        bash_command="echo 'Loading processed data into data warehouse...'",
    )

    # Define the task dependencies
    ingest_data >> preprocess_data >> train_model>> evaluate >> load_data