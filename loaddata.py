# Import Class #
from initalize import *

# Set Up Loop For Loading Inital Data #
load_start = time.time()
for k in [year for year in list(range(2011, 2024)) if year != 2012]:

    loader = LoadData(year=k)

    # Load Schedule #
    print(" ")
    print("="*30, "Begin Loading Schedule Data", "="*30)
    print(" ")
    loader.load_schedule()

    # Load Rosters #
    print(" ")
    print("="*31, "Begin Loading Roster Data", "="*31)
    print(" ")
    roster_slim = loader.load_roster()

    # Load Play By Play #
    print(" ")
    print("="*28, "Begin Loading Play By Play Data", "="*28)
    print(" ")
    loader.load_pbp(roster_slim)

# Print Load Metric #
load_end = time.time()
load_elap = round((load_end - load_start)/3600, 2)
print(f"Load Completed in {load_elap} Hours")
