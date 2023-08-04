# Airflow configuration and installation
git clone https://github.com/MariuszKu/analiza-duckdb-mieszkania.git
cd analiza-duckdb-mieszkania
chmod +x deploy.sh
./deploy.sh

# If you want to work with GCP you need to upload your secrete key and change the working path in env.py file

LINK = "gs://mk-dev-gcs/data/" # for GCP
LINK = 'data/' #for local
LINK = "/opt/airflow/dags/data/" # for airflow local


# create table in BigQuery

create or replace EXTERNAL table mk.flats_report
OPTIONS(
  format = "PARQUET",
  uris = ['gs://mk-dev-gcs/data/data/*.parquet']
);

![1 XOfSnDfvgKyfNadFgwtfiw](https://github.com/MariuszKu/analiza-duckdb-mieszkania/assets/55062728/0d598642-d374-49da-82d6-a6083c47ba05)
