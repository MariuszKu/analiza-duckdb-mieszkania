import airflow
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
import datetime
import os
import sys
from api_nbp import save_usd_df
from flat_price import import_flat_price
from clean import *
from falts_report import report

# Creating an Environmental Variable for the service key configuration
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/key.json'

default_args = {
    "start_date": datetime.datetime.today(),
    "schedule_interval": "0 0 * * *",  # Run every day at midnight
}

with DAG(dag_id="NBP-flats", default_args=default_args, catchup=False) as dag:

    @task
    def import_currency():
       save_usd_df()

    @task
    def import_flat_data():
        import_flat_price()

    @task
    def clean_data_flats():
        clean_flats()

    @task
    def clean_data_salary():
        clean_salary()

    @task
    def clean_data_m1():
        clean_m1()

    @task
    def clean_data_currency():
        clean_currency()

    @task_group
    def clean():
        clean_data_flats()
        clean_data_salary()
        clean_data_m1()
        clean_data_currency()

    @task
    def create_report():
        report()

    # Dependencies
    import_currency() >> import_flat_data() >> clean() >> create_report()
        
    
