# create table

create or replace EXTERNAL table mk.flats_report
OPTIONS(
  format = "PARQUET",
  uris = ['gs://mk-dev-gcs/data/data/*.parquet']
);