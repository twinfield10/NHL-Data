# createmodel.py
from requirements import *

### CONSTANTS ###
### Event Type Classification ###
xG_Events = ['GOAL', 'SHOT', 'MISSED_SHOT', 'BLOCKED_SHOT', 'FACEOFF', 'TAKEAWAY', 'GIVEAWAY', 'HIT']
fenwick_events = ['SHOT', 'GOAL', 'MISSED_SHOT']
corsi_events = ['SHOT', 'GOAL', 'MISSED_SHOT', 'BLOCKED_SHOT']

# Strength States
EV_STR_Codes = ['5v5', '4v4', '3v3']
PP_STR_Codes = ["5v4", "4v5", "5v3", "3v5", "4v3", "3v4"]
UE_STR_Codes = ["5v4", "4v5", "5v3", "3v5", "4v3", "3v4", "5vE", "Ev5", "4vE", "Ev4", "3vE", "Ev3"]
SH_STR_Codes = ['5v6', '4v5', '3v4', '4v6']

# Load Rosters
roster_file = '/Users/tommywinfield/GitRepos/NHL-Data/Rosters/parquet/NHL_Slim_Roster_2010_2024.parquet'
ROSTER_DF_RAW = pl.read_parquet(roster_file)
ROSTER_DF = (
    ROSTER_DF_RAW
    .rename({"id": "event_player_1_id"})
    .with_columns([pl.col("event_player_1_id").cast(pl.Utf8)])
    .select(['event_player_1_id', 'hand_R', 'hand_L', 'pos_F', 'pos_D', 'pos_G'])
    .unique()
)

# Create Goalie DF
GOALIES = (
    ROSTER_DF_RAW
    .filter(pl.col('pos_G') == 1)
    .with_columns([pl.col("id").cast(pl.Utf8)])
    .select('id',
        (pl.col("firstName").str.to_uppercase() + '.' + pl.col("lastName").str.to_uppercase()).alias('event_goalie_name'),
        'hand_R', 'hand_L'
    )
)
GOALIES.columns = ['event_goalie_id', 'event_goalie_name', 'G_hand_R', 'G_hand_L']

### FUNCTIONS ###
