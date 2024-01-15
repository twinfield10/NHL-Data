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

# 1) Clean and Index Play by Play Data
def clean_pbp_data(data):
    """ This function will use inputs from play-by-play data to build usable features in a model.
    Notes:
        1) x_fixed and y_fixed are independent of period and remain constant (Home attacking zone is x_fixed > 0 and Away attacking zone is y_fixed < 0)
        2) Neutral Zone calculation assumes that x coord is > abs(25). >= abs(25) is considered OZ and DZ
    """
    ### EVENT CALCULATIONS ###

    # 1) Create Columns Relative To Event Team
    data = data.with_columns([
        #pl.when(data['event_team_type'] == 'away').then(-data['x_fixed']).otherwise(data['x_fixed']).alias('x_abs'),
        #pl.when(data['event_team_type'] == 'away').then(-data['y_fixed']).otherwise(data['y_fixed']).alias('y_abs')
        pl.when(pl.col('event_team_id') == pl.col('away_id')).then(pl.col('away_abbreviation'))
          .when(pl.col('event_team_id') == pl.col('home_id')).then(pl.col('home_abbreviation'))
          .otherwise(pl.lit(None)).alias('event_team')
      ])
    # 2) Create Zones Using Coordinates
    data = data.with_columns(
      [
        pl.when(data['x_abs'] >= 25).then(pl.lit('OZ'))
        .when((data['x_abs'] > -25) & (data['x_abs'] < 25)).then(pl.lit('NZ'))
        .when(data['x_abs'] <= -25).then(pl.lit('DZ'))
        .otherwise(None)
        .alias('event_zone')
      ]
    )

    data = data.with_columns(
      [
        pl.when((data['event_zone'] == 'OZ') & (data['event_team_type'] == 'home')).then(pl.lit('OZ'))
        .when((data['event_zone'] == 'OZ') & (data['event_team_type'] == 'away')).then(pl.lit('DZ'))
        .when((data['event_zone'] == 'DZ') & (data['event_team_type'] == 'home')).then(pl.lit('DZ'))
        .when((data['event_zone'] == 'DZ') & (data['event_team_type'] == 'away')).then(pl.lit('OZ'))
        .when((data['event_zone'] == 'NZ')).then(pl.lit('NZ'))
        .otherwise(None)
        .alias('home_zone')
      ]
    )
    # 3) Create Event Distance Calculation
    data = data.with_columns(
          pl.when(data['x_abs'] >= 0).then(pl.Series.sqrt((89 - pl.Series.abs(data['x_abs']))**2 + data['y_abs']**2))
          .when(data['x_abs'] <  0).then(pl.Series.sqrt((pl.Series.abs(data['x_abs']) + 89)**2 + data['y_abs']**2))
          .alias('shot_distance')
    )
    # 4) Create Event Angle Calculation
    data = (
        data
        .with_columns(
        pl.when(data['x_abs'] >= 0)
          .then(pl.Series.arctan(data['y_abs'] / (89 - pl.Series.abs(data['x_abs'])))
                .map_elements(lambda x: abs(x * (180 / pi))))
          .when(data['x_abs'] < 0)
          .then(pl.Series.arctan(data['y_abs'] / (pl.Series.abs(data['x_abs']) + 89))
                .map_elements(lambda x: abs(x * (180 / pi))))
          .alias('shot_angle')
        )
        .with_columns(
            pl.when(pl.col('x_abs') > 89).then((180 - pl.col('event_angle'))).otherwise(pl.col('event_angle')).alias('event_angle')
        )
    )
    # 5) Adjust Penalty Shot Game State
    data = data.with_columns(
        [
            pl.when(
                (data['secondary_type'] == 'Penalty Shot') &
                (data['event_team_type'] == 'home')
            ).then(pl.lit('Ev1'))
            .when(
                (data['secondary_type'] == 'Penalty Shot') &
                (data['event_team_type'] == 'away')
            ).then(pl.lit('1vE'))
            .otherwise(data['strength_state']).alias('strength_state'),

            pl.when(
                (data['secondary_type'] == 'Penalty Shot') &
                (data['event_team_type'] == 'home')
            ).then(pl.lit(1))
            .when(
                (data['secondary_type'] == 'Penalty Shot') &
                (data['event_team_type'] == 'away')
            ).then(pl.lit(0))
            .otherwise(data['home_skaters']).alias('home_skaters'),

            pl.when(
                (data['secondary_type'] == 'Penalty Shot') &
                (data['event_team_type'] == 'home')
            ).then(pl.lit(0))
            .when(
                (data['secondary_type'] == 'Penalty Shot') &
                (data['event_team_type'] == 'away')
            ).then(pl.lit(1))
            .otherwise(data['away_skaters']).alias('away_skaters')
        ]
    )

    ### END EVENT CALCULATION ###

    ### BEGIN INDEXING OPERATIONS ###

    # 1) Add Zone Start For Corsi Events (i.e., shots)
    fc_idx = (
        data
        .filter((data['event_type'].is_in(['FACEOFF'] + corsi_events)) &
        (((data['period'] < 5) & (data['season_type'] == 'R')) | (data['season_type'] == 'P'))
        )
        .sort('game_id', 'event_idx')
        .with_columns(pl.when(pl.col('event_type') == "FACEOFF").then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_fac'))
        .with_columns(pl.col('is_fac').cum_sum().alias('face_index'))
        .select(['game_id', 'event_idx', 'face_index', 'home_zone'])
        .sort('game_id', 'event_idx', 'face_index')
        .with_columns(pl.col('home_zone').first().over(['game_id', 'face_index']).alias('first_home_zone'))
        .with_columns(
            pl.when(pl.first('first_home_zone') == 'OZ').then(1)
            .when(pl.first('first_home_zone') == 'NZ').then(2)
            .when(pl.first('first_home_zone') == 'DZ').then(3)
            .otherwise(pl.lit(None))
            .alias('home_zonestart')
        )
        .select(['game_id', 'event_idx', 'home_zonestart'])
    )
    data = data.join(fc_idx, on=["game_id", "event_idx"], how="left")


    # 2) Create Indexes For Shift and Penalties 

    data = (
        data
        .sort(["season","game_id", "event_idx"])
        .with_columns([
            pl.when(pl.col("event_type") == "FACEOFF").then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_fac'),
            pl.when(pl.col("event_type") == "PENALTY").then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_pen'),
            pl.when(pl.col("event_type") == "CHANGE").then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_shi'),
          ])
        .with_columns([
            pl.col('is_fac').cum_sum().alias('face_index'),
            pl.col('is_pen').cum_sum().alias('pen_index'),
            pl.col('is_shi').cum_sum().alias('shift_index')
        ])
        .sort('season', 'game_id', 'event_idx')
        .with_columns([
            (pl.col('game_seconds').first().over(["season","game_id", "shift_index", 'face_index', "pen_index"])).alias("shift_start_seconds"),
            (pl.col('game_seconds').last().over(["season","game_id", "shift_index", 'face_index', "pen_index"])).alias("shift_end_seconds")
            ])
        .drop(['is_fac', 'is_shi', 'is_pen'])
        )


    gb_cols_1 = ["game_id", "period", "season",
                 "home_1_on_id", "home_2_on_id", "home_3_on_id", "home_4_on_id", "home_5_on_id", "home_6_on_id",
                 "away_1_on_id", "away_2_on_id", "away_3_on_id", "away_4_on_id", "away_5_on_id", "away_6_on_id",
                 "home_goalie", "away_goalie",
                 "face_index", "shift_index", "pen_index"]

    idx_df = (
        data
        .filter(
            (pl.col('event_type').is_in(xG_Events)) &
            (((pl.col('period') < 5) & (pl.col('season_type') == 'R')) | (pl.col('season_type') == 'P'))
        )
        .sort('season', 'game_id', 'event_idx')
        .with_columns([
            (((pl.col("event_idx").first().over(gb_cols_1).cast(pl.Float64)) * (pl.col('game_id').cast(pl.Float64))).round()).alias("shift_ID"),
            (pl.col('game_seconds') - pl.col('shift_start_seconds')).alias('shift_event_secs')
                   ])
        .with_columns([
            (pl.col("shift_event_secs").max().over(gb_cols_1)).alias('shift_length')
        ])
    )
    # Join Indexes To Data (Join All Common Columns)
    idx_cols = idx_df.columns
    data_cols = data.columns
    common_cols = list(set(idx_cols) & set(data_cols))

    data =  data.join(
             idx_df,
             on=common_cols,
             how="left"
         )

    return data
# 2) Split into Strength Type Data Frames
def model_prep(data, prep_type):
    """ This function will prep each dataframe to be inputted into a classification model to predict expected goals """

    if(prep_type == 'EV'):
        model_prep = (
            data
            .join(ROSTER_DF.with_columns(pl.col('event_player_1_id').cast(pl.Utf8)), on=["event_player_1_id"], how = 'left')
            .join(GOALIES, on=["event_goalie_name"], how = 'left')
            .with_columns([
                # Target Variable
                (pl.when(pl.col('event_type') == "GOAL").then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_goal'),
                # Game State
                (pl.when(pl.col('strength_state') == "5v5").then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_5v5'),
                (pl.when(pl.col('strength_state') == "4v4").then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_4v4'),
                (pl.when(pl.col('strength_state') == "3v3").then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_3v3'),
                # Score State
                (pl.when(pl.col('score_state') <= -4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_4'),
                (pl.when(pl.col('score_state') == -3).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_3'),
                (pl.when(pl.col('score_state') == -2).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_2'),
                (pl.when(pl.col('score_state') == -1).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_1'),
                (pl.when(pl.col('score_state') == 0).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_even'),
                (pl.when(pl.col('score_state') == 1).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_1'),
                (pl.when(pl.col('score_state') == 2).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_2'),
                (pl.when(pl.col('score_state') == 3).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_3'),
                (pl.when(pl.col('score_state') >= 4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_4'),
                # Prior Shot Outcome
                (pl.when((pl.col('event_type_last') == "SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_shot_same'),
                (pl.when((pl.col('event_type_last') == "MISSED_SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_miss_same'),
                (pl.when((pl.col('event_type_last') == "BLOCKED_SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_block_same'),
                (pl.when((pl.col('event_type_last') == "SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_shot_opp'),
                (pl.when((pl.col('event_type_last') == "MISSED_SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_miss_opp'),
                (pl.when((pl.col('event_type_last') == "BLOCKED_SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_block_opp'),
                # Prior Event - Non Shot
                (pl.when((pl.col('event_type_last') == "GIVEAWAY") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_give_opp'),
                (pl.when((pl.col('event_type_last') == "GIVEAWAY") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_give_same'),
                (pl.when((pl.col('event_type_last') == "TAKEAWAY") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_take_opp'),
                (pl.when((pl.col('event_type_last') == "TAKEAWAY") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_take_same'),
                (pl.when((pl.col('event_type_last') == "HIT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_hit_opp'),
                (pl.when((pl.col('event_type_last') == "HIT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_hit_same'),
                (pl.when((pl.col('event_type_last') == "FACEOFF") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_face_win'),
                (pl.when((pl.col('event_type_last') == "FACEOFF") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_face_lose'),
                # Handiness
                (pl.when(
                    ((pl.col('hand_R') == 1) & (pl.col('y_abs') > 0) & ((pl.col('shot_angle') > 10))) |
                    ((pl.col('hand_L') == 1) & (pl.col('y_abs') < 0) & ((pl.col('shot_angle') > 10)))
                    ).then(pl.lit(1)).otherwise(pl.lit(0)).alias('off_wing')),
                # Create Rebound Flag
                (pl.when(
                (pl.col('event_type_last').is_in(["SHOT", "MISSED_SHOT"])) &
                (pl.col('same_team_last') == 1) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rebound'),
                # Set Play (Faceoff Win + Shot In 5 Seconds)
                (pl.when(
                (pl.col('event_type_last').is_in(["FACEOFF"])) &
                (pl.col('same_team_last') == 1) &
                (pl.col('x_abs_last') > 25) &
                (pl.col('seconds_since_last') <= 4)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_set_play'),
                # Is Rush Play (Transition)
                (pl.when(
                (pl.col('x_abs_last') < 25) &
                (pl.col('seconds_since_last') <= 4)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rush_play')
            ])
            .with_columns([
                pl.when(pl.col('G_hand_R').is_null()).then(pl.lit(0)).otherwise(pl.col('G_hand_R')).alias('G_R_Hand'),
                pl.when(pl.col('G_hand_L').is_null()).then(pl.lit(0)).otherwise(pl.col('G_hand_L')).alias('G_L_Hand')
            ])
        )

        # Get Creates Column Names + Slim
        new_cols = model_prep.columns[-32:]
        model_prep = (
            model_prep
            .select([ 'season', 'game_id', 'event_idx', 'secondary_type',
                'is_goal',
                'period', 'game_seconds','is_home', 'is_overtime', 'is_playoff',
                'x_abs', 'y_abs', 'event_distance', 'event_angle',
                'event_angle_change', 'event_angle_change_speed',
                'seconds_since_last', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
                'pos_F', 'pos_D', 'pos_G', 'hand_R', 'hand_L',
                'shift_length'] + new_cols
            )
        )

    elif(prep_type == 'PP'):
        model_prep = (
            data
            .join(ROSTER_DF.with_columns(pl.col('event_player_1_id').cast(pl.Utf8)), on=["event_player_1_id"], how = 'left')
            .join(GOALIES, on=["event_goalie_name"], how = 'left')
            .with_columns([
                # Target Variable
                (pl.when(pl.col('event_type') == "GOAL").then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_goal'),
                # Game State
                (pl.when(((pl.col('true_strength_state') == "5v4") & (pl.col('event_team_type') == 'home')) | 
                         ((pl.col('true_strength_state') == "4v5") & (pl.col('event_team_type') == 'away'))).then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_5v4'),
                (pl.when(((pl.col('true_strength_state') == "5v3") & (pl.col('event_team_type') == 'home')) | 
                         ((pl.col('true_strength_state') == "3v5") & (pl.col('event_team_type') == 'away'))).then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_5v3'),
                (pl.when(((pl.col('true_strength_state') == "4v3") & (pl.col('event_team_type') == 'home')) | 
                         ((pl.col('true_strength_state') == "3v4") & (pl.col('event_team_type') == 'away'))).then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_4v3'),
                (pl.when(((pl.col('true_strength_state') == "6v5") & (pl.col('event_team_type') == 'home')) | 
                         ((pl.col('true_strength_state') == "5v6") & (pl.col('event_team_type') == 'away'))).then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_6v5'),
                (pl.when(((pl.col('true_strength_state') == "6v4") & (pl.col('event_team_type') == 'home')) | 
                         ((pl.col('true_strength_state') == "4v6") & (pl.col('event_team_type') == 'away'))).then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_6v4'),
                # Score State
                (pl.when(pl.col('score_state') <= -4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_4'),
                (pl.when(pl.col('score_state') == -3).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_3'),
                (pl.when(pl.col('score_state') == -2).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_2'),
                (pl.when(pl.col('score_state') == -1).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_1'),
                (pl.when(pl.col('score_state') == 0).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_even'),
                (pl.when(pl.col('score_state') == 1).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_1'),
                (pl.when(pl.col('score_state') == 2).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_2'),
                (pl.when(pl.col('score_state') == 3).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_3'),
                (pl.when(pl.col('score_state') >= 4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_4'),
                # Prior Shot Outcome
                (pl.when((pl.col('event_type_last') == "SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_shot_same'),
                (pl.when((pl.col('event_type_last') == "MISSED_SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_miss_same'),
                (pl.when((pl.col('event_type_last') == "BLOCKED_SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_block_same'),
                (pl.when((pl.col('event_type_last') == "SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_shot_opp'),
                (pl.when((pl.col('event_type_last') == "MISSED_SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_miss_opp'),
                (pl.when((pl.col('event_type_last') == "BLOCKED_SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_block_opp'),
                # Prior Event - Non Shot
                (pl.when((pl.col('event_type_last') == "GIVEAWAY") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_give_opp'),
                (pl.when((pl.col('event_type_last') == "GIVEAWAY") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_give_same'),
                (pl.when((pl.col('event_type_last') == "TAKEAWAY") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_take_opp'),
                (pl.when((pl.col('event_type_last') == "TAKEAWAY") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_take_same'),
                (pl.when((pl.col('event_type_last') == "HIT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_hit_opp'),
                (pl.when((pl.col('event_type_last') == "HIT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_hit_same'),
                (pl.when((pl.col('event_type_last') == "FACEOFF") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_face_win'),
                (pl.when((pl.col('event_type_last') == "FACEOFF") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_face_lose'),
                # Handiness
                (pl.when(
                    ((pl.col('hand_R') == 1) & (pl.col('y_abs') > 0) & ((pl.col('shot_angle') > 10))) |
                    ((pl.col('hand_L') == 1) & (pl.col('y_abs') < 0) & ((pl.col('shot_angle') > 10)))
                    ).then(pl.lit(1)).otherwise(pl.lit(0)).alias('off_wing')),
                # Create Rebound Flag
                (pl.when(
                (pl.col('event_type_last').is_in(["SHOT", "MISSED_SHOT"])) &
                (pl.col('same_team_last') == 1) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rebound'),
                # Set Play (Faceoff Win + Shot In 5 Seconds)
                (pl.when(
                (pl.col('event_type_last').is_in(["FACEOFF"])) &
                (pl.col('same_team_last') == 1) &
                (pl.col('x_abs_last') > 25) &
                (pl.col('seconds_since_last') <= 4)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_set_play'),
                # Is Rush Play (Transition)
                (pl.when(
                (pl.col('x_abs_last') < 0) &
                (pl.col('seconds_since_last') <= 5)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rush_play')
            ])
            .with_columns([
                pl.when(pl.col('G_hand_R').is_null()).then(pl.lit(0)).otherwise(pl.col('G_hand_R')).alias('G_R_Hand'),
                pl.when(pl.col('G_hand_L').is_null()).then(pl.lit(0)).otherwise(pl.col('G_hand_L')).alias('G_L_Hand')
            ])
        )

        # Get Creates Column Names + Slim
        new_cols = model_prep.columns[-34:]
        model_prep = (
            model_prep
            .select([ 'season', 'game_id', 'event_idx', 'secondary_type',
                'is_goal',
                'period', 'game_seconds','is_home', 'is_overtime', 'is_playoff',
                'x_abs', 'y_abs', 'event_angle', 'event_distance',
                'event_angle_change', 'event_angle_change_speed',
                'seconds_since_last', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
                'pen_seconds_since', 'prior_event_EV', 'is_two_ma',
                'pos_F', 'pos_D', 'pos_G', 'hand_R', 'hand_L',
                'shift_length'] + new_cols
            )
        )

    elif(prep_type == 'SH'):
        model_prep = (
            data
            .join(ROSTER_DF.with_columns(pl.col('event_player_1_id').cast(pl.Utf8)), on=["event_player_1_id"], how = 'left')
            .join(GOALIES, on=["event_goalie_name"], how = 'left')
            .with_columns([
                # Target Variable
                (pl.when(pl.col('event_type') == "GOAL").then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_goal'),
                # Game State
                (pl.when(((pl.col('true_strength_state') == "5v4") & (pl.col('event_team_type') == 'away')) | 
                         ((pl.col('true_strength_state') == "4v5") & (pl.col('event_team_type') == 'home'))).then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_4v5'),
                (pl.when(((pl.col('true_strength_state') == "5v3") & (pl.col('event_team_type') == 'away')) | 
                         ((pl.col('true_strength_state') == "3v5") & (pl.col('event_team_type') == 'home'))).then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_3v5'),
                (pl.when(((pl.col('true_strength_state') == "4v3") & (pl.col('event_team_type') == 'away')) | 
                         ((pl.col('true_strength_state') == "3v4") & (pl.col('event_team_type') == 'home'))).then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_3v4'),
                # Score State
                (pl.when(pl.col('score_state') <= -4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_4'),
                (pl.when(pl.col('score_state') == -3).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_3'),
                (pl.when(pl.col('score_state') == -2).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_2'),
                (pl.when(pl.col('score_state') == -1).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_1'),
                (pl.when(pl.col('score_state') == 0).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_even'),
                (pl.when(pl.col('score_state') == 1).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_1'),
                (pl.when(pl.col('score_state') == 2).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_2'),
                (pl.when(pl.col('score_state') == 3).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_3'),
                (pl.when(pl.col('score_state') >= 4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_4'),
                # Prior Shot Outcome
                (pl.when((pl.col('event_type_last') == "SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_shot_same'),
                (pl.when((pl.col('event_type_last') == "MISSED_SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_miss_same'),
                (pl.when((pl.col('event_type_last') == "BLOCKED_SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_block_same'),
                (pl.when((pl.col('event_type_last') == "SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_shot_opp'),
                (pl.when((pl.col('event_type_last') == "MISSED_SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_miss_opp'),
                (pl.when((pl.col('event_type_last') == "BLOCKED_SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_block_opp'),
                # Prior Event - Non Shot
                (pl.when((pl.col('event_type_last') == "GIVEAWAY") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_give_opp'),
                (pl.when((pl.col('event_type_last') == "GIVEAWAY") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_give_same'),
                (pl.when((pl.col('event_type_last') == "TAKEAWAY") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_take_opp'),
                (pl.when((pl.col('event_type_last') == "TAKEAWAY") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_take_same'),
                (pl.when((pl.col('event_type_last') == "HIT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_hit_opp'),
                (pl.when((pl.col('event_type_last') == "HIT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_hit_same'),
                (pl.when((pl.col('event_type_last') == "FACEOFF") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_face_win'),
                (pl.when((pl.col('event_type_last') == "FACEOFF") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_face_lose'),
                # Handiness
                (pl.when(
                    ((pl.col('hand_R') == 1) & (pl.col('y_abs') > 0) & ((pl.col('shot_angle') > 10))) |
                    ((pl.col('hand_L') == 1) & (pl.col('y_abs') < 0) & ((pl.col('shot_angle') > 10)))
                    ).then(pl.lit(1)).otherwise(pl.lit(0)).alias('off_wing')),
                # Create Rebound Flag
                (pl.when(
                (pl.col('event_type_last').is_in(["SHOT", "MISSED_SHOT"])) &
                (pl.col('same_team_last') == 1) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rebound'),
                # Set Play (Faceoff Win + Shot In 5 Seconds)
                (pl.when(
                (pl.col('event_type_last').is_in(["FACEOFF"])) &
                (pl.col('same_team_last') == 1) &
                (pl.col('x_abs_last') > 25) &
                (pl.col('seconds_since_last') <= 4)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_set_play'),
                # Is Rush Play (Transition)
                (pl.when(
                (pl.col('x_abs_last') < 0) &
                (pl.col('seconds_since_last') <= 5)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rush_play')
            ])
            .with_columns([
                pl.when(pl.col('G_hand_R').is_null()).then(pl.lit(0)).otherwise(pl.col('G_hand_R')).alias('G_R_Hand'),
                pl.when(pl.col('G_hand_L').is_null()).then(pl.lit(0)).otherwise(pl.col('G_hand_L')).alias('G_L_Hand')
            ])
        )

        # Get Creates Column Names + Slim
        new_cols = model_prep.columns[-32:]
        model_prep = (
            model_prep
            .select([ 'season', 'game_id', 'event_idx', 'secondary_type',
                'is_goal',
                'period', 'game_seconds','is_home', 'is_overtime', 'is_playoff',
                'x_abs', 'y_abs', 'event_angle', 'event_distance',
                'event_angle_change', 'event_angle_change_speed',
                'seconds_since_last', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
                'pen_seconds_since', 'prior_event_EV',
                'pos_F', 'pos_D', 'pos_G', 'hand_R', 'hand_L',
                'shift_length'] + new_cols
            )
        )

    elif(prep_type == 'EN'):
        model_prep = (
        data
        .join(ROSTER_DF.with_columns(pl.col('event_player_1_id').cast(pl.Utf8)), on=["event_player_1_id"], how = 'left')
        .with_columns([
            # Target Variable
            (pl.when(pl.col('event_type') == "GOAL").then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_goal'),
            # Game State
            (pl.when(((pl.col('true_strength_state') == "Ev5") & (pl.col('event_team_type') == 'away')) | 
                     ((pl.col('true_strength_state') == "5vE") & (pl.col('event_team_type') == 'home'))).then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_Ev5'),
            (pl.when(((pl.col('true_strength_state') == "Ev4") & (pl.col('event_team_type') == 'away')) | 
                     ((pl.col('true_strength_state') == "4vE") & (pl.col('event_team_type') == 'home'))).then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_Ev4'),
            (pl.when(((pl.col('true_strength_state') == "Ev3") & (pl.col('event_team_type') == 'away')) | 
                     ((pl.col('true_strength_state') == "3vE") & (pl.col('event_team_type') == 'home'))).then(pl.lit(1)).otherwise(pl.lit(0))).alias('state_Ev3'),
            # Score State
            (pl.when(pl.col('score_state') <= -4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_4'),
            (pl.when(pl.col('score_state') == -3).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_3'),
            (pl.when(pl.col('score_state') == -2).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_2'),
            (pl.when(pl.col('score_state') == -1).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_down_1'),
            (pl.when(pl.col('score_state') == 0).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_even'),
            (pl.when(pl.col('score_state') == 1).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_1'),
            (pl.when(pl.col('score_state') == 2).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_2'),
            (pl.when(pl.col('score_state') == 3).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_3'),
            (pl.when(pl.col('score_state') >= 4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('score_up_4'),
            # Prior Shot Outcome
            (pl.when((pl.col('event_type_last') == "SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_shot_same'),
            (pl.when((pl.col('event_type_last') == "MISSED_SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_miss_same'),
            (pl.when((pl.col('event_type_last') == "BLOCKED_SHOT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_block_same'),
            (pl.when((pl.col('event_type_last') == "SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_shot_opp'),
            (pl.when((pl.col('event_type_last') == "MISSED_SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_miss_opp'),
            (pl.when((pl.col('event_type_last') == "BLOCKED_SHOT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_block_opp'),
            # Prior Event - Non Shot
            (pl.when((pl.col('event_type_last') == "GIVEAWAY") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_give_opp'),
            (pl.when((pl.col('event_type_last') == "GIVEAWAY") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_give_same'),
            (pl.when((pl.col('event_type_last') == "TAKEAWAY") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_take_opp'),
            (pl.when((pl.col('event_type_last') == "TAKEAWAY") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_take_same'),
            (pl.when((pl.col('event_type_last') == "HIT") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_hit_opp'),
            (pl.when((pl.col('event_type_last') == "HIT") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_hit_same'),
            (pl.when((pl.col('event_type_last') == "FACEOFF") & (pl.col('same_team_last') == 1)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_face_win'),
            (pl.when((pl.col('event_type_last') == "FACEOFF") & (pl.col('same_team_last') == 0)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_face_lose'),
            # Handiness
            (pl.when(
                    ((pl.col('hand_R') == 1) & (pl.col('y_abs') > 0) & ((pl.col('shot_angle') > 10))) |
                    ((pl.col('hand_L') == 1) & (pl.col('y_abs') < 0) & ((pl.col('shot_angle') > 10)))
                    ).then(pl.lit(1)).otherwise(pl.lit(0)).alias('off_wing')),
            # Create Rebound Flag
            (pl.when(
            (pl.col('event_type_last').is_in(["SHOT", "MISSED_SHOT"])) &
            (pl.col('same_team_last') == 1) &
            (pl.col('seconds_since_last') <= 3)
            ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rebound'),
            # Set Play (Faceoff Win + Shot In 5 Seconds)
            (pl.when(
            (pl.col('event_type_last').is_in(["FACEOFF"])) &
            (pl.col('same_team_last') == 1) &
            (pl.col('x_abs_last') > 25) &
            (pl.col('seconds_since_last') <= 4)
            ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_set_play'),
            # Rush (Transition Play)
            (pl.when(
            (pl.col('x_abs_last') < 0) &
            (pl.col('seconds_since_last') <= 5)
            ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rush_play')
            ])
        )

        # Get Creates Column Names + Slim
        new_cols = model_prep.columns[-30:]
        model_prep = (
            model_prep
            .select(['season', 'game_id', 'event_idx', 'secondary_type',
                'is_goal',
                'period', 'game_seconds','is_home', 'is_overtime', 'is_playoff',
                'x_abs', 'y_abs', 'event_distance', 'event_angle',
                'event_angle_change', 'event_angle_change_speed',
                'seconds_since_last', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
                'prior_event_EV',
                'pos_F', 'pos_D', 'pos_G', 'hand_R', 'hand_L',
                'shift_length'] + new_cols
            )
        )
    return model_prep
