import pandas as pd 
import duckdb
import gcsfs
from utils import convert_to_last_day_of_quarter, generate_days_in_years
import env


def clean_flats():
    flats = pd.read_excel(f"{env.LINK}flat_prices.xlsx", header=6, sheet_name="Rynek pierwotny")
    cities = ['Białystok','Bydgoszcz','Gdańsk','Gdynia','Katowice','Kielce','Kraków','Lublin','Łódź','Olsztyn','Opole',
    'Poznań','Rzeszów','Szczecin','Warszawa','Wrocław','Zielona Góra']
    flats_unpivot = pd.melt(flats, id_vars='Kwartał', value_vars=cities)
    flats_unpivot['date'] = flats_unpivot.apply(lambda row: convert_to_last_day_of_quarter(row['Kwartał']),axis=1)
    flats_unpivot['date'] = pd.to_datetime(flats_unpivot['date'])
    flats_unpivot['city'] = flats_unpivot['variable']
   
    flats_unpivot.to_parquet(f"{env.LINK}flats_price.parquet")

def clean_salary():
    if "gs://" in env.LINK:
        fs = gcsfs.GCSFileSystem()
        fs.put("data/salary.csv",f"{env.LINK}salary.csv")
    salary = pd.read_csv(f"{env.LINK}salary.csv")
    salary['salary'] = salary['salary'].str.replace(" ","")
    salary['date'] = salary.apply(lambda row: convert_to_last_day_of_quarter(row['qt'], False),axis=1)
    salary['date'] = pd.to_datetime(salary['date'])
    salary.to_parquet(f"{env.LINK}salary.parquet")

def clean_m1():
    pass
#    m1 = pd.read_csv(f"{env.LINK}m1.csv")
#    m1['value'] = m1['value'].str.replace(" ","")
#    m1['date'] = m1.apply(lambda row: convert_to_last_day_of_quarter(row['date'], False),axis=1)
#    m1['date'] = pd.to_datetime(m1['date'])
#    m1.to_parquet("data/m1.parquet")

def clean_currency():
    
    gold = pd.read_csv(f"{env.LINK}gold.csv", names=["date","price"], header=None)
    gold2013 = pd.read_csv(f"{env.LINK}gold_2013.csv", names=["date","price"], header=None)
    gold['date'] = pd.to_datetime(gold['date'])
    gold2013['date'] = pd.to_datetime(gold2013['date'])
    gold['currency'] = 'gold'
    gold2013['currency'] = 'gold'
    calendar = pd.DataFrame(generate_days_in_years(2006,2023), columns=["date","last_date"])
    calendar['date'] = pd.to_datetime(calendar['date'])
    usd = pd.read_csv(f"{env.LINK}usd.csv", names=["date","price"], header=None)
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
                     
                     )
                     WHERE
                     lp = 1
                     order by date
                     """).to_df()

    usd.to_parquet(f"{env.LINK}currency.parquet")

if __name__ == "__main__":
    clean_flats()
    clean_currency()
    clean_m1()
    clean_currency()
    clean_salary()