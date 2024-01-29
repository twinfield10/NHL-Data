# Import Class #
from initalize import *

# Load Years
start = 2009 # 2009 is earliest
end = 2024 # 2024 is latest

# Set Up Loop For Loading Inital Data (Rosters and Schedule #
# *Want to load schedule and rosters so I can compile rosters before using in shift data*

load_start = time.time()
for k in range(start, end):

    loader = LoadData(year=k)

    # Load Schedule #
    print(" ")
    print("="*30, f"Begin Loading {k}-{k+1} Schedule And Roster Data", "="*30)
    print(" ")

    # Load Schedule #
    loader.load_schedule()

    # Load Rosters #
    roster_slim = loader.load_roster()
    print(" ")

# Collapse Rosters To Create Large Roster File
def compile_rosters(type):
    print(f"Now Compiling {type} Rosters From All NHL Seasons")
    df_list = []
    for i in range(start, end):
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

print(" ")
print("="*34, "Begin Compiling Roster Files", "="*34)
for j in ['Slim', 'Full']:
    compile_rosters(j)

# Load Slim Rosters
slim_rosters = pl.read_parquet("Rosters/parquet/all/NHL_Roster_AllSeasons_slim.parquet")

# Load Play By Play #
if start < 2010:
    start = 2010
for k in range(start, end):
    
    loader = LoadData(year=k)

    print(" ")
    print("="*28, f"Begin Loading {k}-{k+1} Play By Play Data", "="*28)
    print(" ")

    loader.load_pbp(slim_rosters)

# Print Load Metric #
load_end = time.time()
load_elap = round((load_end - load_start)/3600, 2)
print(" ")
print("="*15, f"Schedule, Rosters, and PBP Data From {start} to {end} Loaded in {load_elap} Hours", "="*15)
