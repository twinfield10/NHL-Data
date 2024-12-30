# updatedata.py

from initalize import *

update_year = 2024

update_start = time.time()

updater = Update(year=update_year)

updater.update_schedule()
roster_data = updater.update_roster()
updater.update_pbp(roster_obj = roster_data)

