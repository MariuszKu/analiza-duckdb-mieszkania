from datetime import datetime, timedelta
import requests
import json
from utils import create_array_first_last_day_of_year



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
        print(cena_zlota)

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
        print(cena_usd)

    return prices

    

if __name__ == "__main__":
    start_year = 2006
    end_year = 2023
    first_last_days_of_years = create_array_first_last_day_of_year(start_year, end_year)

    #with open("gold_2013.csv", "w") as file:
    #    for date in first_last_days_of_years:
    #        gold_prices_data = import_gold_prices(date)

    #        if gold_prices_data:
    #            for date, price in gold_prices_data:
    #                print(f"{date},{price:.2f}")
    #                file.write(f"{date},{price:.2f}\n")

    with open("usd.csv", "w") as file:
        for date in first_last_days_of_years:
            gold_prices_data = import_usd_prices(date)

            if gold_prices_data:
                for date, price in gold_prices_data:
                    print(f"{date},{price:.2f}")
                    file.write(f"{date},{price:.2f}\n")
