import gcsfs
import requests


def import_flat_price():
    fs = gcsfs.GCSFileSystem()
    url = "https://static.nbp.pl/dane/rynek-nieruchomosci/ceny_mieszkan.xlsx"
    response = requests.get(url)
    response.raise_for_status()
    with fs.open("gs://mk-dev-gcs/data/flat_prices.xlsx", "wb") as file:
        file.write(response.content)


if __name__ == "__main__":
    import_flat_price()