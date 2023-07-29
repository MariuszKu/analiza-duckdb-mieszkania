import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import datetime
import os
import sys
from utils import create_array_first_last_day_of_year
from api_nbp import import_usd_prices
from flat_price import import_flat_price
from clean import *

# Creating an Environmental Variable for the service key configuration
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/configs/ServiceKey_GoogleCloud.json'

default_args = {
    "start_date": datetime.datetime.today(),
    "schedule_interval": "0 0 * * *",  # Run every day at midnight
}

with DAG(dag_id="NBP-flats", default_args=default_args, catchup=False) as dag:

    @task
    def import_currency():
        start_year = 2006
        end_year = 2023
        first_last_days_of_years = create_array_first_last_day_of_year(
            start_year, end_year
        )

        with open("data/usd.csv", "w") as file:
            for date in first_last_days_of_years:
                gold_prices_data = import_usd_prices(date)

                if gold_prices_data:
                    for date, price in gold_prices_data:
                        print(f"{date},{price:.2f}")
                        file.write(f"{date},{price:.2f}\n")

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

    # Dependencies
    [import_currency, import_flat_data] >> [
        clean_data_flats,
        clean_data_salary,
        clean_data_m1,
        clean_data_currency,
    ]
