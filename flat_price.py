import requests


def import_flat_price():
    url = "https://static.nbp.pl/dane/rynek-nieruchomosci/ceny_mieszkan.xlsx"
    response = requests.get(url)
    response.raise_for_status()
    with open("data/flat_prices.xlsx", "wb") as file:
        file.write(response.content)



