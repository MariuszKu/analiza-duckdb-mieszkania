import pandas as pd 
import duckdb
import gcsfs as fs
from utils import convert_to_last_day_of_quarter, generate_days_in_years

def report():
     calendar = pd.DataFrame(generate_days_in_years(2006,2023), columns=["date","last_date"])
     flats_price = pd.read_parquet("gs://mk-dev-gcs/data/flats_price.parquet")
     currency = pd.read_parquet("gs://mk-dev-gcs/data/currency.parquet")
     salary = pd.read_parquet("gs://mk-dev-gcs/data/salary.parquet")

     df_data = duckdb.sql("""
               select 
                    a.date, 
                    a.value  flat_price, 
                    b.price gold, 
                    b.price*31 ounce, 
                    c.price usd,
                    a.value / c.price flat_price_usd,
                    a.value  / (b.price * 31) flat_price_gold,
                    d.salary,
                    a.value / cast(d.salary as Double) salaries_m2                     
               from 
               flats_price a 
               left join currency b on a.date = b.date and b.currency = 'gold'
               left join currency c on a.date = c.date and c.currency = 'usd'
               left join salary d on a.date = d.date
               where
               city = 'Warszawa'
               order by a.date
               """).to_df()

     #df_data.to_csv("data.csv", encoding='utf-8', index=False)
     df_data.to_parquet("gs://mk-dev-gcs/data/data/falts_report.parquet")


if __name__ == "__main__":
     report()