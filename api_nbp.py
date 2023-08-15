from datetime import datetime, timedelta
import requests
import json
import gcsfs
import pandas as pd
from utils import create_array_first_last_day_of_year
import env


def import_gold_prices(date: tuple) -> list[(str, str)]:
    """
    This function returns an array of date and gold price.

    Args:
        date: tuple: date range.

    Returns:
        list[str, str]: list of prices.
    """
    url = f"https://api.nbp.pl/api/cenyzlota/{date[0]}/{date[1]}"
    response = requests.get(url)
    response.raise_for_status()
    data = json.loads(response.text)
    gold_prices = []

    for cena_zlota in data:
        date = cena_zlota["data"]
        price = cena_zlota["cena"]
        gold_prices.append((date, price))

    return gold_prices

def import_usd_prices(date: tuple[str, str]) -> list[(str, str)]:
    """
    This function returns an array of date and usd price.

    Args:
        date: tuple: date range.

    Returns:
        list[str, str]: list of prices.
    """
    url = f"https://api.nbp.pl/api/exchangerates/rates/a/usd/{date[0]}/{date[1]}/"
    response = requests.get(url)
    response.raise_for_status()
    data = json.loads(response.text)
    print(data["rates"])
    prices = []

    for cena_usd in data["rates"]:
        date = cena_usd["effectiveDate"]
        price = cena_usd["mid"]
        prices.append((date, price))
        
    return prices

def save_gold_df() -> None:
    start_year = 2013 
    end_year = 2023
    fs = gcsfs.GCSFileSystem()
    first_last_days_of_years = create_array_first_last_day_of_year(start_year, end_year)
    arr = []

    for date in first_last_days_of_years:
            
        arr_gold = import_gold_prices(date)
        arr.extend(arr_gold)

    if 'gs://' in env.LINK:
        fs.put("data/gold.csv",f"{env.LINK}gold.csv")
    pd.DataFrame(arr).to_csv(f'{env.LINK}gold.csv', index=False, header=None)   

   
def save_usd_df() -> None:
    """
    Saves USD dataframe

    Args:
    Returns:
    """
    start_year = 2006
    end_year = 2023
    first_last_days_of_years = create_array_first_last_day_of_year(start_year, end_year)
    arr = []
    fs = gcsfs.GCSFileSystem()

    for date in first_last_days_of_years:
        gold_prices_data = import_usd_prices(date)
        arr.extend(gold_prices_data)
        pd.DataFrame(arr).to_csv(f'{env.LINK}usd.csv', index=False, header=None) 


if __name__ == "__main__":
    save_gold_df()
    save_usd_df()

