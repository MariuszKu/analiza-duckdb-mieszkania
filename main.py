import datetime
import os
import sys
from api_nbp import save_usd_df
from flat_price import import_flat_price
import clean
from falts_report import report


save_usd_df()
import_flat_price()
clean.clean_flats()
clean.clean_salary()
clean.clean_m1()
clean.clean_currency()
report()