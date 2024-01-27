# updatedata.py

from initalize import *

update_year = 2023

update_start = time.time()

updater = Update(year=update_year)

updater.update_schedule()
updater.update_roster()
updater.update_pbp()

