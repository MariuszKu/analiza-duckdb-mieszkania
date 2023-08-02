# create table

create or replace EXTERNAL table mk.flats_report
OPTIONS(
  format = "PARQUET",
  uris = ['gs://mk-dev-gcs/data/data/*.parquet']
);

![1 XOfSnDfvgKyfNadFgwtfiw](https://github.com/MariuszKu/analiza-duckdb-mieszkania/assets/55062728/0d598642-d374-49da-82d6-a6083c47ba05)
