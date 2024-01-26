# Import Class #
from initalize import *

# Set Up Loop For Loading Inital Data #
load_start = time.time()
for k in [year for year in list(range(2020, 2024)) if year != 2012]:

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

# Collapse Rosters To Create Large Roster File
def compile_rosters(type):
    print(f"Now Compiling {type} Rosters From All NHL Seasons")
    df_list = []
    for i in [year for year in list(range(2009, 2024)) if year != 2012]:
        season_roster_path = f"Rosters/parquet/{type.lower()}/NHL_Roster_{type}_{i}{i+1}.parquet"
        df_list.append(pl.read_parquet(season_roster_path))

    # Combine
    result = df_list[0]
    for df in df_list[1:]:
        result = result.extend(df)
    
    # Save
    save_path = f"Rosters/parquet/all/NHL_Roster_AllSeasons_{type}.parquet"
    result.write_parquet(save_path)
    print(f"All NHL Rosters Loaded | Path: {save_path}")

for j in ['Slim', 'Full']:
    compile_rosters(j)

