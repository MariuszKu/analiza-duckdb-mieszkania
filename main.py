import pandas as pd 
import duckdb
from utils import convert_to_last_day_of_quarter, generate_days_in_years


flats = pd.read_excel("flat_prices.xlsx", header=6, sheet_name="Rynek pierwotny")
flats['date'] = flats.apply(lambda row: convert_to_last_day_of_quarter(row['Kwartał']),axis=1)
salary = pd.read_csv("salary.csv")

salary['salary'] = salary['salary'].str.replace(" ","")
salary['date'] = salary.apply(lambda row: convert_to_last_day_of_quarter(row['qt'], False),axis=1)

gold = pd.read_csv("gold.csv", names=["date","price"], header=None)
gold2013 = pd.read_csv("gold_2013.csv", names=["date","price"], header=None)
usd = pd.read_csv("usd.csv", names=["date","price"], header=None)
gold = pd.concat([gold, gold2013], ignore_index=True, sort=False)

calendar = pd.DataFrame(generate_days_in_years(2006,2023), columns=["date","last_date"])

df_data = duckdb.sql("""
           with cte_gold as (
                select * from (
                select row_number() over(partition by b.last_date order by a.date desc) lp, 
                b.last_date, 
                a.date, 
                a.price 
                from 
                gold a inner join calendar b on a.date=b.date
                ) res 
                where lp = 1
           ), 
           cte_usd as (
                select * from (
                select row_number() over(partition by b.last_date order by a.date desc) lp, 
                b.last_date, 
                a.date, 
                a.price 
                from 
                usd a inner join calendar b on a.date=b.date
                ) res 
                where lp = 1
           )


           select 
           a.date, 
           a.Kwartał, 
           Warszawa flat_price, 
           b.price gold, 
           b.price*31 ounce, 
           c.price usd,
           Warszawa / c.price flat_price_usd,
           Warszawa / (b.price * 31) flat_price_gold,
           d.salary,
           Warszawa / cast(d.salary as Double) salaries_m2                     

           from 
           flats a 
           left join cte_gold b on a.date = b.last_date
           left join cte_usd c on a.date = c.last_date
           left join salary d on a.date = d.date
           order by a.date
           """).df()

df_data.to_csv("data.csv", encoding='utf-8', index=False)