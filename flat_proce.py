import requests

url = "https://static.nbp.pl/dane/rynek-nieruchomosci/ceny_mieszkan.xlsx"
response = requests.get(url)
response.raise_for_status()
with open("flat_prices.xlsx", "wb") as file:
    file.write(response.content)



