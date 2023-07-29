import pandas as pd 
import duckdb
from utils import convert_to_last_day_of_quarter, generate_days_in_years

def clean_flats():
    flats = pd.read_excel("data/flat_prices.xlsx", header=6, sheet_name="Rynek pierwotny")
    cities = ['Białystok','Bydgoszcz','Gdańsk','Gdynia','Katowice','Kielce','Kraków','Lublin','Łódź','Olsztyn','Opole',
    'Poznań','Rzeszów','Szczecin','Warszawa','Wrocław','Zielona Góra']
    flats_unpivot = pd.melt(flats, id_vars='Kwartał', value_vars=cities)
    flats_unpivot['date'] = flats_unpivot.apply(lambda row: convert_to_last_day_of_quarter(row['Kwartał']),axis=1)
    flats_unpivot['date'] = pd.to_datetime(flats_unpivot['date'])
   
    flats_unpivot.to_parquet("data/flats_price.parquet")

def clean_salary():
    salary = pd.read_csv("data/salary.csv")
    salary['salary'] = salary['salary'].str.replace(" ","")
    salary['date'] = salary.apply(lambda row: convert_to_last_day_of_quarter(row['qt'], False),axis=1)
    salary['date'] = pd.to_datetime(salary['date'])
    salary.to_parquet("data/salary.parquet")

def clean_m1():
    m1 = pd.read_csv("data/m1.csv")
    m1['value'] = m1['value'].str.replace(" ","")
    m1['date'] = m1.apply(lambda row: convert_to_last_day_of_quarter(row['date'], False),axis=1)
    m1['date'] = pd.to_datetime(m1['date'])
    m1.to_parquet("data/m1.parquet")

def clean_currency():
    
    gold = pd.read_csv("data/gold.csv", names=["date","price"], header=None)
    gold2013 = pd.read_csv("data/gold_2013.csv", names=["date","price"], header=None)
    gold['date'] = pd.to_datetime(gold['date'])
    gold2013['date'] = pd.to_datetime(gold2013['date'])
    gold['currency'] = 'gold'
    gold2013['currency'] = 'gold'
    calendar = pd.DataFrame(generate_days_in_years(2006,2023), columns=["date","last_date"])
    calendar['date'] = pd.to_datetime(calendar['date'])
    usd = pd.read_csv("data/usd.csv", names=["date","price"], header=None)
    usd['date'] = pd.to_datetime(usd['date'])
    usd['currency'] = 'usd'
    currency = pd.concat([gold, gold2013, usd], ignore_index=True, sort=False)
    # fill gups
    usd = duckdb.sql("""
                     
                     select
                         date,
                         price,
                         currency,
                     from
                     (
                     select
                     row_number() over (partition by currency, a.date order by b.date desc) lp,
                     a.date,
                     b.date org_date,
                     b.price,    
                     currency,
                     from
                     calendar a left join currency b on b.date between a.date - INTERVAL 3 DAY and a.date 
                     --where a.date between '2006-01-19' and '2006-01-30'
                     )
                     WHERE
                     lp = 1
                     order by date
                     """).to_parquet("data/currency.parquet")

