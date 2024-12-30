from requirements import *

#### CONSTANTS + FUNCTIONS ####
st_yr = 2010
end_yr = 2025

# Event Type Constant ###
xG_Events = ['GOAL', 'SHOT', 'MISSED_SHOT', 'BLOCKED_SHOT', 'FACEOFF', 'TAKEAWAY', 'GIVEAWAY', 'HIT']
fenwick_events = ['SHOT', 'GOAL', 'MISSED_SHOT']
corsi_events = ['SHOT', 'GOAL', 'MISSED_SHOT', 'BLOCKED_SHOT']

# Strength States - Constant #
EV_STR_Codes = ['5v5', '4v4', '3v3']
PP_STR_Codes = ["5v4", "4v5", "5v3", "3v5", "4v3", "3v4"]
UE_STR_Codes = ["5v4", "4v5", "5v3", "3v5", "4v3", "3v4", "5vE", "Ev5", "4vE", "Ev4", "3vE", "Ev3"]
SH_STR_Codes = ['5v6', '4v5', '3v4', '4v6']


# Load Rosters
roster_file = 'https://raw.githubusercontent.com/twinfield10/NHL-Data/main/Rosters/parquet/all/NHL_Roster_AllSeasons_Slim.parquet'
ROSTER_DF_RAW = pl.read_parquet(roster_file)
ROSTER_DF = (
    ROSTER_DF_RAW
    .rename({"player_id": "event_player_1_id"})
    .with_columns([
        pl.col("event_player_1_id").cast(pl.Utf8),
        pl.when((pl.col('pos_F') == 0) & (pl.col('pos_G') == 0)).then(pl.lit(1)).otherwise(pl.lit(0)).alias('pos_D')
        ])
    .select(['event_player_1_id', 'hand_R', 'hand_L', 'pos_F', 'pos_D', 'pos_G'])
    .unique()
)

# Create Goalie DF
GOALIES = (
    ROSTER_DF_RAW
    .filter(pl.col('pos_G') == 1)
    .with_columns([pl.col("player_id").cast(pl.Utf8)])
    .select('player_id','hand_R', 'hand_L')
    .unique()
)
GOALIES.columns = ['event_goalie_id', 'G_hand_R', 'G_hand_L']

## FUNCTIONS ##

# 1) Clean PBP Data
def clean_pbp_data(data):
    """ This function will use inputs from play-by-play data to build usable features in a model.
    Notes:
        1) x_fixed and y_fixed are independent of period and remain constant (Home attacking zone is x_fixed > 0 and Away attacking zone is y_fixed < 0)
        2) Neutral Zone calculation assumes that x coord is > abs(25). >= abs(25) is considered OZ and DZ
    """
    ### EVENT CALCULATIONS ###

    # 1) Create Columns Relative To Event Team
    data = data.with_columns([
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
        .alias('home_event_zone')
      ]
    )
    # 3) Create Event Distance Calculation
    data = (
        data
        .with_columns([
          pl.when(data['x_abs'] >= 0).then(pl.Series.sqrt((89.25 - pl.Series.abs(data['x_abs']))**2 + data['y_abs']**2))
          .when(data['x_abs'] <  0).then(pl.Series.sqrt((pl.Series.abs(data['x_abs']) + 89.25)**2 + data['y_abs']**2))
          .alias('event_distance')
        ])
        .with_columns([
            pl.when((pl.col('event_distance').abs() == 0.0)).then(pl.lit(0.25))
            .otherwise(pl.col('event_distance').round(3)).alias('event_distance')
        ])
    )
    # 4) Create Event Angle Calculation
    data = (
        data
        .with_columns(
        pl.when(data['x_abs'] >= 0)
          .then(pl.Series.arctan(data['y_abs'] / (89.25 - pl.Series.abs(data['x_abs'])))
                .map_elements(lambda x: abs(x * (180 / pi))))
          .when(data['x_abs'] < 0)
          .then(pl.Series.arctan(data['y_abs'] / (pl.Series.abs(data['x_abs']) + 89.25))
                .map_elements(lambda x: abs(x * (180 / pi))))
          .alias('event_angle')
        )
        .with_columns(
            pl.when(pl.col('x_abs') > 89.25).then((180 - pl.col('event_angle'))).otherwise(pl.col('event_angle')).alias('event_angle')
        )
        .with_columns(
            pl.col('event_angle').round(3)
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
    return data
# 2) Index Clean PBP Data
def index_input_data(data):
    """ This Function will create indexes and ID's for certain types of plays/events."""

    # Create Indexes For Shifts, Faceoffs and Penalties
    home_player_id_col_struct = ["home_1_on_id", "home_2_on_id", "home_3_on_id", "home_4_on_id", "home_5_on_id", "home_6_on_id", "home_goalie"]
    away_player_id_col_struct = ["away_1_on_id", "away_2_on_id", "away_3_on_id", "away_4_on_id", "away_5_on_id", "away_6_on_id", "away_goalie"]
    all_player_id_col_struct = home_player_id_col_struct + away_player_id_col_struct

    data = (
        data
        .sort(["season","game_id", "period", "event_idx"])
        .with_columns([
            pl.when(pl.col("event_type") == "FACEOFF").then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_fac'),
            pl.when(pl.col("event_type") == "PENALTY").then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_pen'),
            pl.when((pl.struct(all_player_id_col_struct) != pl.struct(all_player_id_col_struct).shift()) | (pl.col('event_type').shift() == 'PERIOD_START')).then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_shi'),
            pl.when((pl.struct(home_player_id_col_struct) != pl.struct(home_player_id_col_struct).shift()) | (pl.col('event_type').shift() == 'PERIOD_START')).then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_H_shi'),
            pl.when((pl.struct(away_player_id_col_struct) != pl.struct(away_player_id_col_struct).shift()) | (pl.col('event_type').shift() == 'PERIOD_START')).then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_A_shi'),
          ])
        .with_columns([
            pl.col('is_fac').cum_sum().alias('face_index'),
            pl.col('is_pen').cum_sum().alias('pen_index'),
            pl.col('is_shi').cum_sum().alias('all_shift_index'),
            pl.col('is_H_shi').cum_sum().alias('home_shift_index'),
            pl.col('is_A_shi').cum_sum().alias('away_shift_index'),
        ])
        .sort('season', 'game_id', 'event_idx')
        .with_columns([
            (pl.col('game_seconds').first().over(["season","game_id", "period", "all_shift_index"])).alias("all_shift_start_seconds"),
            (pl.col('game_seconds').last().over(["season","game_id", "period", "all_shift_index"])).alias("all_shift_end_seconds"),
            (pl.col('game_seconds').first().over(["season","game_id", "period", "home_shift_index"])).alias("home_shift_start_seconds"),
            (pl.col('game_seconds').last().over(["season","game_id", "period", "home_shift_index"])).alias("home_shift_end_seconds"),
            (pl.col('game_seconds').first().over(["season","game_id", "period", "away_shift_index"])).alias("away_shift_start_seconds"),
            (pl.col('game_seconds').last().over(["season","game_id", "period", "away_shift_index"])).alias("away_shift_end_seconds")
            ])
        .drop(['is_fac', 'is_shi', 'is_pen', 'is_H_shi', 'is_A_shi'])
        )


    gb_cols_1 = ["game_id", "period", "face_index", "pen_index"]

    idx_df = (
        data
        .filter(
            (((pl.col('period') < 5) & (pl.col('season_type') == 'R')) | (pl.col('season_type') == 'P'))
        )
        .sort('season', 'game_id', 'period', 'event_idx')
        .with_columns([
            pl.when(pl.struct(gb_cols_1 + ['all_shift_index']) != pl.struct(gb_cols_1 + ['all_shift_index']).shift()).then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_all_new_shift'),
            pl.when(pl.struct(["game_id", "period",'home_shift_index']) != pl.struct(["game_id", "period",'home_shift_index']).shift()).then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_home_new_shift'),
            pl.when(pl.struct(["game_id", "period",'away_shift_index']) != pl.struct(["game_id", "period",'away_shift_index']).shift()).then(pl.lit(1)).otherwise(pl.lit(0)).alias('is_away_new_shift'),
                   ])
        .with_columns([
            pl.col('is_all_new_shift').cum_sum().alias('all_shift_ID'),
            pl.col('is_home_new_shift').cum_sum().alias('home_shift_ID'),
            pl.col('is_away_new_shift').cum_sum().alias('away_shift_ID')
        ])
        .with_columns([
            (pl.col('game_seconds') - pl.col('all_shift_start_seconds')).alias('all_shift_length'),
            (pl.col('game_seconds') - pl.col('home_shift_start_seconds')).alias('home_shift_length'),
            (pl.col('game_seconds') - pl.col('away_shift_start_seconds')).alias('away_shift_length')
        ])
        .with_columns([
            pl.when(pl.col('event_team_type') == 'home').then(pl.col('home_shift_length'))
              .when(pl.col('event_team_type') == 'away').then(pl.col('away_shift_length'))
              .otherwise(None).alias('event_team_toi'),
            pl.when(pl.col('event_team_type') == 'away').then(pl.col('home_shift_length'))
              .when(pl.col('event_team_type') == 'home').then(pl.col('away_shift_length'))
              .otherwise(None).alias('def_team_toi'),
            pl.when(pl.col('event_team_type') == 'home').then(pl.col('home_shift_length') - pl.col('away_shift_length'))
              .when(pl.col('event_team_type') == 'away').then(pl.col('away_shift_length') - pl.col('home_shift_length'))
              .otherwise(None).alias('event_team_shift_time_diff')
        ])
        .select("game_id", "period", "event_idx", "face_index", "all_shift_index", "home_shift_index", "away_shift_index", "pen_index",
                "all_shift_ID", "home_shift_ID", "away_shift_ID",
                "all_shift_length", "home_shift_length", "away_shift_length")
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
# 3) Split Data By Strength
def split_by_strength(data):
    """This function will split and clean indexed play-by-play data into 4 categories (EV, PP, SH, and EN)"""

    EV_DF = (
        data
        .filter(
            (pl.col('event_type').is_in(xG_Events)) &
            (((pl.col('period') < 5) & (pl.col('season_type') == 'R')) | (pl.col('season_type') == 'P')) &
            (~pl.col('x_abs').is_null()) &
            (~pl.col('y_abs').is_null())
        )
        .sort('season', 'game_id', 'period', 'event_idx')
        .with_columns([
            ((pl.col('game_seconds')) - (pl.col('game_seconds').shift(1).over(['season', 'game_id', 'period']))).alias('seconds_since_last'),
            ((pl.col('game_seconds')) - (pl.col('game_seconds').first().over(['season', 'game_id', 'period', 'home_shift_ID']))).alias('home_skaters_toi'),
            ((pl.col('game_seconds')) - (pl.col('game_seconds').first().over(['season', 'game_id', 'period', 'away_shift_ID']))).alias('away_skaters_toi'),
            ((pl.col('event_type').shift(1).over(['season', 'game_id', 'period']))).alias('event_type_last'),
            ((pl.col('event_team_abbr').shift(1).over(['season', 'game_id', 'period']))).alias('event_team_last'),
            ((pl.col('strength_state').shift(1).over(['season', 'game_id', 'period']))).alias('event_strength_last'),
            ((pl.col('x_abs').shift(1).over(['season', 'game_id', 'period']))).alias('x_abs_last'),
            ((pl.col('y_abs').shift(1).over(['season', 'game_id', 'period']))).alias('y_abs_last'),
            ((pl.col('home_score').shift(1).over(['season', 'game_id', 'period']))).alias('home_score'),
            ((pl.col('away_score').shift(1).over(['season', 'game_id', 'period']))).alias('away_score'),
            (pl.when((pl.col('event_team_type') == 'home')).then((pl.col('home_goalie')).str.to_uppercase()).otherwise((pl.col('away_goalie').str.to_uppercase())).alias('event_goalie_id'))
        ])
        .with_columns([
            (pl.when(pl.col('event_team_type') == 'home').then(pl.col('home_skaters_toi'))
               .when(pl.col('event_team_type') == 'away').then(pl.col('away_skaters_toi'))
               .otherwise(pl.lit(None))
            ).alias('event_team_toi'),
            (pl.when(pl.col('event_team_type') == 'away').then(pl.col('home_skaters_toi'))
               .when(pl.col('event_team_type') == 'home').then(pl.col('away_skaters_toi'))
               .otherwise(pl.lit(None))
            ).alias('def_team_toi')
        ])
        .with_columns([
            (pl.col('def_team_toi') - pl.col('event_team_toi')).alias('event_team_shift_time_diff')
        ])
        .sort('season', 'game_id', 'event_idx')
        .filter(
            (pl.col('event_type').is_in(fenwick_events)) &
            (pl.col('strength_state').is_in(EV_STR_Codes)) &
            (~pl.col('x_abs_last').is_null()) &
            (~pl.col('y_abs_last').is_null())
        )
        .with_columns([
            (pl.when(pl.col('event_team_last') == pl.col('event_team_abbr')).then(pl.col('x_abs_last')).otherwise(pl.col('x_abs_last') * -1).alias('x_abs_last')),
            (pl.when(pl.col('event_team_last') == pl.col('event_team_abbr')).then(pl.col('y_abs_last')).otherwise(pl.col('y_abs_last') * -1).alias('y_abs_last')),
            (pl.when(pl.col('home_score').is_null()).then(pl.lit(0)).otherwise(pl.col('home_score'))).alias('home_score'),
            (pl.when(pl.col('away_score').is_null()).then(pl.lit(0)).otherwise(pl.col('away_score'))).alias('away_score')
        ])
        .with_columns([
            (pl.when(pl.col('event_team_abbr') == pl.col('event_team_last')).then(pl.lit(1)).otherwise(pl.lit(0))).alias('same_team_last'),
            (pl.when(pl.col('event_team_type') == 'home').then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_home'),
            (pl.when(pl.col('period') >= 4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_overtime'),
            (pl.when(pl.col('season_type') == 'P').then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_playoff'),
            (pl.when(pl.col('event_team_type') == 'home').then(pl.col('home_score') - pl.col('away_score')).otherwise(pl.col('away_score') - pl.col('home_score'))).alias('score_state'),
            #(pl.when((pl.col('seconds_since_last') == 0) & (pl.col('event_type_last') == 'FACEOFF')).then(pl.col("shift_length")).otherwise(pl.col('seconds_since_last'))).alias('seconds_since_last'),
            ((((pl.col('x_abs') - pl.col('x_abs_last')) ** 2) + ((pl.col('y_abs') - pl.col('y_abs_last')) ** 2)).sqrt()).alias('distance_from_last')
        ])
        .with_columns(
            pl.when(pl.col('seconds_since_last') == 0).then(pl.lit(0.5)).otherwise(pl.col('seconds_since_last')).alias('seconds_since_last'),
            pl.when(pl.col('x_abs_last') >= 0)
            .then((pl.col('y_abs_last') / (89.25 - (pl.col('x_abs_last').abs()))).arctan()
                    .map_elements(lambda x: abs(x * (180 / pi))))
            .when(pl.col('x_abs_last') < 0)
            .then((pl.col('y_abs_last') / ((pl.col('x_abs_last').abs()) + 89.25)).arctan()
                    .map_elements(lambda x: abs(x * (180 / pi))))
            .alias('event_angle_last')
        )
        .with_columns(
            pl.when(pl.col('x_abs_last') > 89.25).then((180 - pl.col('event_angle_last'))).otherwise(pl.col('event_angle_last')).alias('event_angle_last')
        )
        .with_columns([
            (pl.col('distance_from_last') / pl.col('seconds_since_last')).alias('puck_speed_since_last'),
            ((pl.col('event_angle') - pl.col('event_angle_last')).abs()).alias('event_angle_change')
            ])
        .with_columns((pl.col('event_angle_change') / pl.col('seconds_since_last')).alias('event_angle_change_speed'))
        .with_columns([
            pl.when(pl.col('puck_speed_since_last').is_infinite()).then(pl.col('distance_from_last') / pl.lit(0.5)).otherwise(pl.col('puck_speed_since_last')).alias('puck_speed_since_last'),
            pl.when(pl.col('event_angle_last').is_infinite()).then(None).otherwise(pl.col('event_angle_last')).alias('event_angle_last')
            ])
        .select(['season', 'game_id', 'game_date', 'event_idx', 'period', 'game_seconds', 'is_overtime', 'is_playoff',
                'strength_state', 'score_state', 'is_home', 
                'event_player_1_id', 'home_goalie', 'away_goalie', 'event_player_2_id', 'event_goalie_id',
                'home_score', 'away_score', 'home_abbreviation', 'away_abbreviation', 'home_skaters', 'away_skaters',
                'event_type', 'event_team', 'event_team_abbr', 'secondary_type',
                'x_abs', 'y_abs', 'event_angle_last', 'event_angle', 'event_distance',
                'event_angle_change', 'event_angle_change_speed',
                'event_team_last', 'same_team_last', 'event_strength_last', 'event_type_last',
                'seconds_since_last', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
                'event_team_shift_time_diff', 'event_team_toi', 'def_team_toi'])
    )

    ## Build PP DataFrame ##
    PP_DF = (
        data
        .filter(
            (pl.col('event_type').is_in(xG_Events)) &
            (((pl.col('period') < 5) & (pl.col('season_type') == 'R')) | (pl.col('season_type') == 'P')) &
            (~pl.col('x_abs').is_null()) &
            (~pl.col('y_abs').is_null())
        )
        .sort('season', 'game_id', 'period', 'event_idx')
        .with_columns([
            ((pl.col('game_seconds')) - (pl.col('game_seconds').shift(1).over(['season', 'game_id', 'period']))).alias('seconds_since_last'),
            ((pl.col('game_seconds')) - (pl.col('game_seconds').first().over(['season', 'game_id', 'period', 'home_shift_ID']))).alias('home_skaters_toi'),
            ((pl.col('game_seconds')) - (pl.col('game_seconds').first().over(['season', 'game_id', 'period', 'away_shift_ID']))).alias('away_skaters_toi'),
            ((pl.col('event_type').shift(1).over(['season', 'game_id', 'period']))).alias('event_type_last'),
            ((pl.col('event_team_abbr').shift(1).over(['season', 'game_id', 'period']))).alias('event_team_last'),
            ((pl.col('strength_state').shift(1).over(['season', 'game_id', 'period']))).alias('event_strength_last'),
            ((pl.col('x_abs').shift(1).over(['season', 'game_id', 'period']))).alias('x_abs_last'),
            ((pl.col('y_abs').shift(1).over(['season', 'game_id', 'period']))).alias('y_abs_last'),
            ((pl.col('event_angle').shift(1).over(['season', 'game_id', 'period']))).alias('event_angle_last'),
            ((pl.col('home_score').shift(1).over(['season', 'game_id', 'period']))).alias('home_score'),
            ((pl.col('away_score').shift(1).over(['season', 'game_id', 'period']))).alias('away_score'),
            (pl.when(pl.col('strength_state').is_in(PP_STR_Codes)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_pen'),
            (pl.when(((pl.col('home_skaters') - pl.col('away_skaters')) >= 2) | ((pl.col('away_skaters') - pl.col('home_skaters')) >= 2)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_two_ma'),
            (pl.when((pl.col('event_team_type') == 'home')).then((pl.col('home_goalie')).str.to_uppercase()).otherwise((pl.col('away_goalie').str.to_uppercase())).alias('event_goalie_id'))
        ])
        .with_columns(((pl.col('is_pen')) * ((pl.col('game_seconds')) - (pl.col('game_seconds').first().over(['season', 'game_id', 'pen_index'])))).alias('pen_seconds_since'))
        .with_columns([
            (pl.when((pl.col('pen_seconds_since') > 0) & (pl.col('pen_seconds_since') >= 300)).then(pl.lit(120)).otherwise(pl.col('pen_seconds_since'))).alias('pen_seconds_since')
            ])
        .with_columns([
            (pl.when(pl.col('event_team_type') == 'home').then(pl.col('home_skaters_toi'))
               .when(pl.col('event_team_type') == 'away').then(pl.col('away_skaters_toi'))
               .otherwise(pl.lit(None))
            ).alias('event_team_toi'),
            (pl.when(pl.col('event_team_type') == 'away').then(pl.col('home_skaters_toi'))
               .when(pl.col('event_team_type') == 'home').then(pl.col('away_skaters_toi'))
               .otherwise(pl.lit(None))
            ).alias('def_team_toi')
        ])
        .with_columns([
            (pl.col('def_team_toi') - pl.col('event_team_toi')).alias('event_team_shift_time_diff')
        ])
        .sort('season', 'game_id', 'event_idx')
        .filter(
            (pl.col('event_type').is_in(fenwick_events)) &
            (((pl.col('event_team_type') == 'home') & (pl.col('true_strength_state').is_in(["6v5", "6v4", "5v4", "5v3", "4v3"]))) |
            ((pl.col('event_team_type') == 'away') & (pl.col('true_strength_state').is_in(["5v6", "4v6", "4v5", "3v5", "3v4"])))) &
            (~pl.col('x_abs_last').is_null()) &
            (~pl.col('y_abs_last').is_null())
        )
        .with_columns([
            (pl.when(pl.col('event_team_last') == pl.col('event_team_abbr')).then(pl.col('x_abs_last')).otherwise(pl.col('x_abs_last') * -1).alias('x_abs_last')),
            (pl.when(pl.col('event_team_last') == pl.col('event_team_abbr')).then(pl.col('y_abs_last')).otherwise(pl.col('y_abs_last') * -1).alias('y_abs_last')),
            (pl.when(pl.col('home_score').is_null()).then(pl.lit(0)).otherwise(pl.col('home_score'))).alias('home_score'),
            (pl.when(pl.col('away_score').is_null()).then(pl.lit(0)).otherwise(pl.col('away_score'))).alias('away_score')
        ])
        .with_columns([
            (pl.when(pl.col('event_team_abbr') == pl.col('event_team_last')).then(pl.lit(1)).otherwise(pl.lit(0))).alias('same_team_last'),
            (pl.when(pl.col('event_team_type') == 'home').then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_home'),
            (pl.when(pl.col('period') >= 4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_overtime'),
            (pl.when(pl.col('season_type') == 'P').then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_playoff'),
            (pl.when(pl.col('event_team_type') == 'home').then(pl.col('home_score') - pl.col('away_score')).otherwise(pl.col('away_score') - pl.col('home_score'))).alias('score_state'),
            #(pl.when((pl.col('seconds_since_last') == 0) & (pl.col('event_type_last') == 'FACEOFF')).then(pl.col("shift_length")).otherwise(pl.col('seconds_since_last'))).alias('seconds_since_last'),
            ((((pl.col('x_abs') - pl.col('x_abs_last')) ** 2) + ((pl.col('y_abs') - pl.col('y_abs_last')) ** 2)).sqrt()).alias('distance_from_last'),
            (pl.when(pl.col('event_strength_last').is_in(EV_STR_Codes)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_event_EV')
        ])
        .with_columns(
            pl.when(pl.col('seconds_since_last') == 0).then(pl.lit(0.5)).otherwise(pl.col('seconds_since_last')).alias('seconds_since_last'),
            pl.when(pl.col('x_abs_last') >= 0)
            .then((pl.col('y_abs_last') / (89.25 - (pl.col('x_abs_last').abs()))).arctan()
                    .map_elements(lambda x: abs(x * (180 / pi))))
            .when(pl.col('x_abs_last') < 0)
            .then((pl.col('y_abs_last') / ((pl.col('x_abs_last').abs()) + 89.25)).arctan()
                    .map_elements(lambda x: abs(x * (180 / pi))))
            .alias('event_angle_last')
        )
        .with_columns(
            pl.when(pl.col('x_abs_last') > 89.25).then((180 - pl.col('event_angle_last'))).otherwise(pl.col('event_angle_last')).alias('event_angle_last')
        )
        .with_columns([
            (pl.col('distance_from_last') / pl.col('seconds_since_last')).alias('puck_speed_since_last'),
            ((pl.col('event_angle') - pl.col('event_angle_last')).abs()).alias('event_angle_change')
            ])
        .with_columns((pl.col('event_angle_change') / pl.col('seconds_since_last')).alias('event_angle_change_speed'))
        .with_columns([
            pl.when(pl.col('puck_speed_since_last').is_infinite()).then(pl.col('distance_from_last') / pl.lit(0.5)).otherwise(pl.col('puck_speed_since_last')).alias('puck_speed_since_last'),
            pl.when(pl.col('event_angle_last').is_infinite()).then(None).otherwise(pl.col('event_angle_last')).alias('event_angle_last')
            ])
        .select([
            'season', 'game_id', 'game_date', 'event_idx', 'period', 'game_seconds', 'is_overtime', 'is_playoff',
            'strength_state', 'true_strength_state', 'score_state', 'is_home', 'is_two_ma',
            'event_player_1_id', 'home_goalie', 'away_goalie', 'event_player_2_id', 'event_goalie_id',
            'home_score', 'away_score', 'home_abbreviation', 'away_abbreviation', 'home_skaters', 'away_skaters',
            'event_type', 'event_team', 'event_team_abbr', 'event_team_type', 'secondary_type',
            'x_abs', 'y_abs', 'event_angle_last', 'event_angle', 'event_distance',
            'event_angle_change', 'event_angle_change_speed',
            'event_team_last', 'same_team_last', 'event_strength_last', 'prior_event_EV', 'event_type_last',
            'seconds_since_last', 'pen_seconds_since', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
            'event_team_shift_time_diff', 'event_team_toi', 'def_team_toi'
        ])
    )

    ## Build SH DataFrame ##
    SH_DF = (
        data
        .filter(
            (pl.col('event_type').is_in(xG_Events)) &
            (((pl.col('period') < 5) & (pl.col('season_type') == 'R')) | (pl.col('season_type') == 'P')) &
            (~pl.col('x_abs').is_null()) &
            (~pl.col('y_abs').is_null())
        )
        .sort('season', 'game_id', 'period', 'event_idx')
        .with_columns([
            ((pl.col('game_seconds')) - (pl.col('game_seconds').shift(1).over(['season', 'game_id', 'period']))).alias('seconds_since_last'),
            ((pl.col('game_seconds')) - (pl.col('game_seconds').first().over(['season', 'game_id', 'period', 'home_shift_ID']))).alias('home_skaters_toi'),
            ((pl.col('game_seconds')) - (pl.col('game_seconds').first().over(['season', 'game_id', 'period', 'away_shift_ID']))).alias('away_skaters_toi'),
            ((pl.col('event_type').shift(1).over(['season', 'game_id', 'period']))).alias('event_type_last'),
            ((pl.col('event_team_abbr').shift(1).over(['season', 'game_id', 'period']))).alias('event_team_last'),
            ((pl.col('strength_state').shift(1).over(['season', 'game_id', 'period']))).alias('event_strength_last'),
            ((pl.col('x_abs').shift(1).over(['season', 'game_id', 'period']))).alias('x_abs_last'),
            ((pl.col('y_abs').shift(1).over(['season', 'game_id', 'period']))).alias('y_abs_last'),
            ((pl.col('event_angle').shift(1).over(['season', 'game_id', 'period']))).alias('event_angle_last'),
            ((pl.col('home_score').shift(1).over(['season', 'game_id', 'period']))).alias('home_score'),
            ((pl.col('away_score').shift(1).over(['season', 'game_id', 'period']))).alias('away_score'),
            (pl.concat_str([pl.col('home_skaters'), pl.lit('v'), pl.col('away_skaters')])).alias('true_strength_state'),
            (pl.when(pl.col('strength_state').is_in(PP_STR_Codes)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_pen'),
            (pl.when(((pl.col('home_skaters') - pl.col('away_skaters')) >= 2) | ((pl.col('away_skaters') - pl.col('home_skaters')) >= 2)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_two_ma'),
            (pl.when((pl.col('event_team_type') == 'home')).then((pl.col('home_goalie')).str.to_uppercase()).otherwise((pl.col('away_goalie').str.to_uppercase())).alias('event_goalie_id'))
        ])
        .with_columns([((pl.col('is_pen')) * ((pl.col('game_seconds')) - (pl.col('game_seconds').first().over(['season', 'game_id', 'pen_index'])))).alias('pen_seconds_since')
            ])
        .with_columns((pl.when((pl.col('pen_seconds_since') > 0) & (pl.col('pen_seconds_since') >= 300)).then(pl.lit(120)).otherwise(pl.col('pen_seconds_since'))).alias('pen_seconds_since'))
        .with_columns([
            (pl.when(pl.col('event_team_type') == 'home').then(pl.col('home_skaters_toi'))
               .when(pl.col('event_team_type') == 'away').then(pl.col('away_skaters_toi'))
               .otherwise(pl.lit(None))
            ).alias('event_team_toi'),
            (pl.when(pl.col('event_team_type') == 'away').then(pl.col('home_skaters_toi'))
               .when(pl.col('event_team_type') == 'home').then(pl.col('away_skaters_toi'))
               .otherwise(pl.lit(None))
            ).alias('def_team_toi')
        ])
        .with_columns([
            (pl.col('def_team_toi') - pl.col('event_team_toi')).alias('event_team_shift_time_diff')
        ])
        .sort('season', 'game_id', 'event_idx')
        .filter(
            (pl.col('event_type').is_in(fenwick_events)) &
            (((pl.col('event_team_type') == 'away') & (pl.col('true_strength_state').is_in(["5v4", "5v3", "4v3"]))) |
            ((pl.col('event_team_type') == 'home') & (pl.col('true_strength_state').is_in(["4v5", "3v5", "3v4"])))) &
            (~pl.col('x_abs_last').is_null()) &
            (~pl.col('y_abs_last').is_null())
        )
        .with_columns([
            (pl.when(pl.col('event_team_last') == pl.col('event_team_abbr')).then(pl.col('x_abs_last')).otherwise(pl.col('x_abs_last') * -1).alias('x_abs_last')),
            (pl.when(pl.col('event_team_last') == pl.col('event_team_abbr')).then(pl.col('y_abs_last')).otherwise(pl.col('y_abs_last') * -1).alias('y_abs_last')),
            (pl.when(pl.col('home_score').is_null()).then(pl.lit(0)).otherwise(pl.col('home_score'))).alias('home_score'),
            (pl.when(pl.col('away_score').is_null()).then(pl.lit(0)).otherwise(pl.col('away_score'))).alias('away_score')
        ])
        .with_columns([
            (pl.when(pl.col('event_team_abbr') == pl.col('event_team_last')).then(pl.lit(1)).otherwise(pl.lit(0))).alias('same_team_last'),
            (pl.when(pl.col('event_team_type') == 'home').then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_home'),
            (pl.when(pl.col('period') >= 4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_overtime'),
            (pl.when(pl.col('season_type') == 'P').then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_playoff'),
            (pl.when(pl.col('event_team_type') == 'home').then(pl.col('home_score') - pl.col('away_score')).otherwise(pl.col('away_score') - pl.col('home_score'))).alias('score_state'),
            #(pl.when((pl.col('seconds_since_last') == 0) & (pl.col('event_type_last') == 'FACEOFF')).then(pl.col("shift_length")).otherwise(pl.col('seconds_since_last'))).alias('seconds_since_last'),
            ((((pl.col('x_abs') - pl.col('x_abs_last')) ** 2) + ((pl.col('y_abs') - pl.col('y_abs_last')) ** 2)).sqrt()).alias('distance_from_last'),
            (pl.when(pl.col('event_strength_last').is_in(EV_STR_Codes)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_event_EV')
        ])
        .with_columns(
            pl.when(pl.col('seconds_since_last') == 0).then(pl.lit(0.5)).otherwise(pl.col('seconds_since_last')).alias('seconds_since_last'),
            pl.when(pl.col('x_abs_last') >= 0)
            .then((pl.col('y_abs_last') / (89.25 - (pl.col('x_abs_last').abs()))).arctan()
                    .map_elements(lambda x: abs(x * (180 / pi))))
            .when(pl.col('x_abs_last') < 0)
            .then((pl.col('y_abs_last') / ((pl.col('x_abs_last').abs()) + 89.25)).arctan()
                    .map_elements(lambda x: abs(x * (180 / pi))))
            .alias('event_angle_last')
        )
        .with_columns(
            pl.when(pl.col('x_abs_last') > 89.25).then((180 - pl.col('event_angle_last'))).otherwise(pl.col('event_angle_last')).alias('event_angle_last')
        )
        .with_columns([
            (pl.col('distance_from_last') / pl.col('seconds_since_last')).alias('puck_speed_since_last'),
            ((pl.col('event_angle') - pl.col('event_angle_last')).abs()).alias('event_angle_change')
            ])
        .with_columns((pl.col('event_angle_change') / pl.col('seconds_since_last')).alias('event_angle_change_speed'))
        .with_columns([
            pl.when(pl.col('puck_speed_since_last').is_infinite()).then(pl.col('distance_from_last') / pl.lit(0.5)).otherwise(pl.col('puck_speed_since_last')).alias('puck_speed_since_last'),
            pl.when(pl.col('event_angle_last').is_infinite()).then(None).otherwise(pl.col('event_angle_last')).alias('event_angle_last')
            ])
        .select([
            'season', 'game_id', 'game_date', 'event_idx', 'period', 'game_seconds', 'is_overtime', 'is_playoff',
            'strength_state', 'true_strength_state', 'score_state', 'is_home', 'is_two_ma',
            'event_player_1_id', 'home_goalie', 'away_goalie', 'event_player_2_id', 'event_goalie_id',
            'home_score', 'away_score', 'home_abbreviation', 'away_abbreviation', 'home_skaters', 'away_skaters',
            'event_type', 'event_team', 'event_team_abbr', 'event_team_type', 'secondary_type',
            'x_abs', 'y_abs', 'event_angle_last', 'event_angle', 'event_distance',
            'event_angle_change', 'event_angle_change_speed',
            'event_team_last', 'same_team_last', 'event_strength_last', 'prior_event_EV', 'event_type_last',
            'seconds_since_last', 'pen_seconds_since', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
            'event_team_shift_time_diff', 'event_team_toi', 'def_team_toi'
        ])
    )

    ## Build Empty Net DataFrame ##
    EN_DF = (
        data
        .filter(
            (pl.col('event_type').is_in(xG_Events)) &
            (((pl.col('period') < 5) & (pl.col('season_type') == 'R')) | (pl.col('season_type') == 'P')) &
            (~pl.col('x_abs').is_null()) &
            (~pl.col('y_abs').is_null())
        )
        .sort('season', 'game_id', 'period', 'event_idx')
        .with_columns([
            ((pl.col('game_seconds')) - (pl.col('game_seconds').shift(1).over(['season', 'game_id', 'period']))).alias('seconds_since_last'),
            ((pl.col('game_seconds')) - (pl.col('game_seconds').first().over(['season', 'game_id', 'period', 'home_shift_index']))).alias('home_skaters_toi'),
            ((pl.col('game_seconds')) - (pl.col('game_seconds').first().over(['season', 'game_id', 'period', 'away_shift_index']))).alias('away_skaters_toi'),
            ((pl.col('event_type').shift(1).over(['season', 'game_id', 'period']))).alias('event_type_last'),
            ((pl.col('event_team_abbr').shift(1).over(['season', 'game_id', 'period']))).alias('event_team_last'),
            ((pl.col('strength_state').shift(1).over(['season', 'game_id', 'period']))).alias('event_strength_last'),
            ((pl.col('x_abs').shift(1).over(['season', 'game_id', 'period']))).alias('x_abs_last'),
            ((pl.col('y_abs').shift(1).over(['season', 'game_id', 'period']))).alias('y_abs_last'),
            ((pl.col('home_score').shift(1).over(['season', 'game_id', 'period']))).alias('home_score'),
            ((pl.col('away_score').shift(1).over(['season', 'game_id', 'period']))).alias('away_score'),
            ((pl.col('event_angle').shift(1).over(['season', 'game_id', 'period']))).alias('event_angle_last'),
            (pl.when(pl.col('strength_state').is_in(PP_STR_Codes)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_pen'),
            (pl.when(((pl.col('home_skaters') - pl.col('away_skaters')) >= 2) | ((pl.col('away_skaters') - pl.col('home_skaters')) >= 2)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_two_ma')
        ])
        .with_columns([
            (pl.when(pl.col('event_team_type') == 'home').then(pl.col('home_skaters_toi'))
               .when(pl.col('event_team_type') == 'away').then(pl.col('away_skaters_toi'))
               .otherwise(pl.lit(None))
            ).alias('event_team_toi'),
            (pl.when(pl.col('event_team_type') == 'away').then(pl.col('home_skaters_toi'))
               .when(pl.col('event_team_type') == 'home').then(pl.col('away_skaters_toi'))
               .otherwise(pl.lit(None))
            ).alias('def_team_toi')
        ])
        .with_columns([
            (pl.col('def_team_toi') - pl.col('event_team_toi')).alias('event_team_shift_time_diff')
        ])
        .sort('season', 'game_id', 'event_idx')
        .filter(
            (pl.col('event_type').is_in(fenwick_events)) &
            (((pl.col('event_team_type') == 'away') & (pl.col('true_strength_state').is_in(["Ev5", "Ev4", "Ev3"]))) |
            ((pl.col('event_team_type') == 'home') & (pl.col('true_strength_state').is_in(["5vE", "4vE", "3vE"])))) &
            (~pl.col('x_abs_last').is_null()) &
            (~pl.col('y_abs_last').is_null())
        )
        .with_columns([
            (pl.when(pl.col('event_team_last') == pl.col('event_team_abbr')).then(pl.col('x_abs_last')).otherwise(pl.col('x_abs_last') * -1).alias('x_abs_last')),
            (pl.when(pl.col('event_team_last') == pl.col('event_team_abbr')).then(pl.col('y_abs_last')).otherwise(pl.col('y_abs_last') * -1).alias('y_abs_last')),
            (pl.when(pl.col('home_score').is_null()).then(pl.lit(0)).otherwise(pl.col('home_score'))).alias('home_score'),
            (pl.when(pl.col('away_score').is_null()).then(pl.lit(0)).otherwise(pl.col('away_score'))).alias('away_score')
        ])
        .with_columns([
            (pl.when(pl.col('event_team_abbr') == pl.col('event_team_last')).then(pl.lit(1)).otherwise(pl.lit(0))).alias('same_team_last'),
            (pl.when(pl.col('event_team_type') == 'home').then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_home'),
            (pl.when(pl.col('period') >= 4).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_overtime'),
            (pl.when(pl.col('season_type') == 'P').then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_playoff'),
            (pl.when(pl.col('event_team_type') == 'home').then(pl.col('home_score') - pl.col('away_score')).otherwise(pl.col('away_score') - pl.col('home_score'))).alias('score_state'),
            #(pl.when((pl.col('seconds_since_last') == 0) & (pl.col('event_type_last') == 'FACEOFF')).then(pl.col("shift_length")).otherwise(pl.col('seconds_since_last'))).alias('seconds_since_last'),
            ((((pl.col('x_abs') - pl.col('x_abs_last')) ** 2) + ((pl.col('y_abs') - pl.col('y_abs_last')) ** 2)).sqrt()).alias('distance_from_last'),
            (pl.when(pl.col('event_strength_last').is_in(EV_STR_Codes)).then(pl.lit(1)).otherwise(pl.lit(0))).alias('prior_event_EV'),
            (pl.when(pl.col('strength_state').is_in(EV_STR_Codes + ['6v6'])).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_EV')
        ])
        .with_columns(
            pl.when(pl.col('seconds_since_last') == 0).then(pl.lit(0.5)).otherwise(pl.col('seconds_since_last')).alias('seconds_since_last'),
            pl.when(pl.col('x_abs_last') >= 0)
            .then((pl.col('y_abs_last') / (89.25 - (pl.col('x_abs_last').abs()))).arctan()
                    .map_elements(lambda x: abs(x * (180 / pi))))
            .when(pl.col('x_abs_last') < 0)
            .then((pl.col('y_abs_last') / ((pl.col('x_abs_last').abs()) + 89.25)).arctan()
                    .map_elements(lambda x: abs(x * (180 / pi))))
            .alias('event_angle_last')
        )
        .with_columns(
            pl.when(pl.col('x_abs_last') > 89.25).then((180 - pl.col('event_angle_last'))).otherwise(pl.col('event_angle_last')).alias('event_angle_last')
        )
        .with_columns([
            (pl.col('distance_from_last') / pl.col('seconds_since_last')).alias('puck_speed_since_last'),
            ((pl.col('event_angle') - pl.col('event_angle_last')).abs()).alias('event_angle_change')
            ])
        .with_columns((pl.col('event_angle_change') / pl.col('seconds_since_last')).alias('event_angle_change_speed'))
        .with_columns([
            pl.when(pl.col('puck_speed_since_last').is_infinite()).then(pl.col('distance_from_last') / pl.lit(0.5)).otherwise(pl.col('puck_speed_since_last')).alias('puck_speed_since_last'),
            pl.when(pl.col('event_angle_last').is_infinite()).then(None).otherwise(pl.col('event_angle_last')).alias('event_angle_last')
            ])
        .select([
            'season', 'game_id', 'game_date', 'event_idx', 'period', 'game_seconds', 'is_overtime', 'is_playoff',
            'strength_state', 'true_strength_state', 'score_state', 'is_home', 'is_two_ma', 'is_pen', 'is_EV',
            'event_player_1_id', 'home_goalie', 'away_goalie', 'event_player_2_id',
            'home_score', 'away_score', 'home_abbreviation', 'away_abbreviation', 'home_skaters', 'away_skaters',
            'event_type', 'event_team', 'event_team_abbr', 'event_team_type', 'secondary_type', 
            'x_abs', 'y_abs', 'event_angle', 'event_distance',
            'event_angle_change', 'event_angle_change_speed',
            'event_team_last', 'same_team_last', 'event_strength_last', 'prior_event_EV', 'event_type_last',
            'seconds_since_last', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
            'event_team_shift_time_diff', 'event_team_toi', 'def_team_toi'
        ])
    )
    return EV_DF, PP_DF, SH_DF, EN_DF
# 4) One-Hot Encode Certain Columns
def model_prep(data, prep_type):
    """ This function will prep each dataframe to be inputted into a classification model to predict expected goals """

    if(prep_type == 'EV'):
        model_prep = (
            data
            .join(ROSTER_DF.with_columns(pl.col('event_player_1_id').cast(pl.Utf8)), on=["event_player_1_id"], how = 'left')
            .join(GOALIES, on=["event_goalie_id"], how = 'left')
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
                    ((pl.col('hand_R') == 1) & (pl.col('y_abs') > 0) & ((pl.col('event_angle') > 10))) |
                    ((pl.col('hand_L') == 1) & (pl.col('y_abs') < 0) & ((pl.col('event_angle') > 10)))
                    ).then(pl.lit(1)).otherwise(pl.lit(0)).alias('off_wing'))
            ])
            .with_columns([
                # Create Rebound Flag
                (pl.when(
                (pl.col('prior_shot_same') == 1) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rebound'),
                (pl.when(
                ((pl.col('prior_miss_same') == 1) | (pl.col('prior_block_same') == 1)) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_post_miss_shot'),
                # Set Play (Faceoff Win + Shot In 5 Seconds)
                (pl.when(
                (pl.col('prior_face_win') == 1) &
                (pl.col('x_abs_last') > 25) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_set_play'),
                # Rush (Transition Play after Turnover)
                (pl.when(
                (pl.col('x_abs_last') < 0) &
                (pl.col('seconds_since_last') <= 5) &
                ((pl.col('prior_give_opp') == 1) | (pl.col('prior_shot_opp') == 1) | (pl.col('prior_miss_opp') == 1) | (pl.col('prior_block_opp') == 1) | (pl.col('prior_take_same') == 1))
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rush_play'),
                (pl.when(
                (pl.col('x_abs_last') < 25) &
                (pl.col('seconds_since_last') <= 3) &
                ((pl.col('prior_give_opp') == 1) | (pl.col('prior_shot_opp') == 1) | (pl.col('prior_miss_opp') == 1) | (pl.col('prior_block_opp') == 1) | (pl.col('prior_take_same') == 1))
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_fast_rush_play')
            ])
        )

        # Get Creates Column Names + Slim
        new_cols = model_prep.columns[-32:]
        model_prep = (
            model_prep
            .select([ 'season', 'game_id', 'event_idx', 'secondary_type', 'event_player_1_id', 'event_goalie_id',
                'is_goal',
                'period', 'game_seconds','is_home', 'is_overtime', 'is_playoff',
                'x_abs', 'y_abs', 'event_distance', 'event_angle',
                'event_angle_change', 'event_angle_change_speed',
                'seconds_since_last', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
                'pos_F', 'pos_D', 'pos_G', 'hand_R', 'hand_L', 'G_hand_R', 'G_hand_L',
                'event_team_shift_time_diff', 'event_team_toi', 'def_team_toi'] + new_cols
            )
        )

    elif(prep_type == 'PP'):
        model_prep = (
            data
            .join(ROSTER_DF.with_columns(pl.col('event_player_1_id').cast(pl.Utf8)), on=["event_player_1_id"], how = 'left')
            .join(GOALIES, on=["event_goalie_id"], how = 'left')
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
                    ((pl.col('hand_R') == 1) & (pl.col('y_abs') > 0) & ((pl.col('event_angle') > 10))) |
                    ((pl.col('hand_L') == 1) & (pl.col('y_abs') < 0) & ((pl.col('event_angle') > 10)))
                    ).then(pl.lit(1)).otherwise(pl.lit(0)).alias('off_wing'))
            ])
            .with_columns([
                # Create Rebound Flag
                (pl.when(
                (pl.col('prior_shot_same') == 1) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rebound'),
                (pl.when(
                ((pl.col('prior_miss_same') == 1) | (pl.col('prior_block_same') == 1)) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_post_miss_shot'),
                # Set Play (Faceoff Win + Shot In 5 Seconds)
                (pl.when(
                (pl.col('prior_face_win') == 1) &
                (pl.col('x_abs_last') > 25) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_set_play'),
                # Rush (Transition Play after Turnover)
                (pl.when(
                (pl.col('x_abs_last') < 0) &
                (pl.col('seconds_since_last') <= 5) &
                ((pl.col('prior_give_opp') == 1) | (pl.col('prior_shot_opp') == 1) | (pl.col('prior_miss_opp') == 1) | (pl.col('prior_block_opp') == 1) | (pl.col('prior_take_same') == 1))
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rush_play'),
                (pl.when(
                (pl.col('x_abs_last') < 25) &
                (pl.col('seconds_since_last') <= 3) &
                ((pl.col('prior_give_opp') == 1) | (pl.col('prior_shot_opp') == 1) | (pl.col('prior_miss_opp') == 1) | (pl.col('prior_block_opp') == 1) | (pl.col('prior_take_same') == 1))
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_fast_rush_play')
            ])
        )

        # Get Creates Column Names + Slim
        new_cols = model_prep.columns[-34:]
        model_prep = (
            model_prep
            .select([ 'season', 'game_id', 'event_idx', 'secondary_type', 'event_player_1_id', 'event_goalie_id',
                'is_goal',
                'period', 'game_seconds','is_home', 'is_overtime', 'is_playoff',
                'x_abs', 'y_abs', 'event_angle', 'event_distance',
                'event_angle_change', 'event_angle_change_speed',
                'seconds_since_last', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
                'pen_seconds_since', 'prior_event_EV', 'is_two_ma',
                'pos_F', 'pos_D', 'pos_G', 'hand_R', 'hand_L', 'G_hand_R', 'G_hand_L',
                'event_team_shift_time_diff', 'event_team_toi', 'def_team_toi'] + new_cols
            )
        )

    elif(prep_type == 'SH'):
        model_prep = (
            data
            .join(ROSTER_DF.with_columns(pl.col('event_player_1_id').cast(pl.Utf8)), on=["event_player_1_id"], how = 'left')
            .join(GOALIES, on=["event_goalie_id"], how = 'left')
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
                    ((pl.col('hand_R') == 1) & (pl.col('y_abs') > 0) & ((pl.col('event_angle') > 10))) |
                    ((pl.col('hand_L') == 1) & (pl.col('y_abs') < 0) & ((pl.col('event_angle') > 10)))
                    ).then(pl.lit(1)).otherwise(pl.lit(0)).alias('off_wing'))
            ])
            .with_columns([
                # Create Rebound Flag
                (pl.when(
                (pl.col('prior_shot_same') == 1) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rebound'),
                (pl.when(
                ((pl.col('prior_miss_same') == 1) | (pl.col('prior_block_same') == 1)) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_post_miss_shot'),
                # Set Play (Faceoff Win + Shot In 5 Seconds)
                (pl.when(
                (pl.col('prior_face_win') == 1) &
                (pl.col('x_abs_last') > 25) &
                (pl.col('seconds_since_last') <= 3)
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_set_play'),
                # Rush (Transition Play after Turnover)
                (pl.when(
                (pl.col('x_abs_last') < 0) &
                (pl.col('seconds_since_last') <= 5) &
                ((pl.col('prior_give_opp') == 1) | (pl.col('prior_shot_opp') == 1) | (pl.col('prior_miss_opp') == 1) | (pl.col('prior_block_opp') == 1) | (pl.col('prior_take_same') == 1))
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rush_play'),
                (pl.when(
                (pl.col('x_abs_last') < 25) &
                (pl.col('seconds_since_last') <= 3) &
                ((pl.col('prior_give_opp') == 1) | (pl.col('prior_shot_opp') == 1) | (pl.col('prior_miss_opp') == 1) | (pl.col('prior_block_opp') == 1) | (pl.col('prior_take_same') == 1))
                ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_fast_rush_play')
            ])
        )

        # Get Creates Column Names + Slim
        new_cols = model_prep.columns[-32:]
        model_prep = (
            model_prep
            .select([ 'season', 'game_id', 'event_idx', 'secondary_type', 'event_player_1_id', 'event_goalie_id',
                'is_goal',
                'period', 'game_seconds','is_home', 'is_overtime', 'is_playoff',
                'x_abs', 'y_abs', 'event_angle', 'event_distance',
                'event_angle_change', 'event_angle_change_speed',
                'seconds_since_last', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
                'pen_seconds_since', 'prior_event_EV',
                'pos_F', 'pos_D', 'pos_G', 'hand_R', 'hand_L', 'G_hand_R', 'G_hand_L',
                'event_team_shift_time_diff', 'event_team_toi', 'def_team_toi'] + new_cols
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
                    ((pl.col('hand_R') == 1) & (pl.col('y_abs') > 0) & ((pl.col('event_angle') > 10))) |
                    ((pl.col('hand_L') == 1) & (pl.col('y_abs') < 0) & ((pl.col('event_angle') > 10)))
                    ).then(pl.lit(1)).otherwise(pl.lit(0)).alias('off_wing'))
        ])
        .with_columns([
            # Create Rebound Flag
            (pl.when(
            (pl.col('prior_shot_same') == 1) &
            (pl.col('seconds_since_last') <= 3)
            ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rebound'),
            (pl.when(
            ((pl.col('prior_miss_same') == 1) | (pl.col('prior_block_same') == 1)) &
            (pl.col('seconds_since_last') <= 3)
            ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_post_miss_shot'),
            # Set Play (Faceoff Win + Shot In 5 Seconds)
            (pl.when(
            (pl.col('prior_face_win') == 1) &
            (pl.col('x_abs_last') > 25) &
            (pl.col('seconds_since_last') <= 3)
            ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_set_play'),
            # Rush (Transition Play after Turnover)
            (pl.when(
            (pl.col('x_abs_last') < 0) &
            (pl.col('seconds_since_last') <= 5) &
            ((pl.col('prior_give_opp') == 1) | (pl.col('prior_shot_opp') == 1) | (pl.col('prior_miss_opp') == 1) | (pl.col('prior_block_opp') == 1) | (pl.col('prior_take_same') == 1))
            ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_rush_play'),
            (pl.when(
            (pl.col('x_abs_last') < 25) &
            (pl.col('seconds_since_last') <= 3) &
            ((pl.col('prior_give_opp') == 1) | (pl.col('prior_shot_opp') == 1) | (pl.col('prior_miss_opp') == 1) | (pl.col('prior_block_opp') == 1) | (pl.col('prior_take_same') == 1))
            ).then(pl.lit(1)).otherwise(pl.lit(0))).alias('is_fast_rush_play')
            ])
        )

        # Get Creates Column Names + Slim
        new_cols = model_prep.columns[-32:]
        model_prep = (
            model_prep
            .select(['season', 'game_id', 'event_idx', 'secondary_type', 'event_player_1_id',
                'is_goal',
                'period', 'game_seconds','is_home', 'is_overtime', 'is_playoff',
                'x_abs', 'y_abs', 'event_distance', 'event_angle',
                'event_angle_change', 'event_angle_change_speed',
                'seconds_since_last', 'distance_from_last', 'x_abs_last', 'y_abs_last', 'puck_speed_since_last',
                'prior_event_EV',
                'pos_F', 'pos_D', 'pos_G', 'hand_R', 'hand_L',
                'event_team_shift_time_diff', 'event_team_toi', 'def_team_toi'] + new_cols
            )
        )
    return model_prep
# 5) Imputate Shot Type For Weird Shots
def imp_sec_type(data):
    """ This Function will looks to impute missing values in secondary type by using a classification model to guess the shot type"""

    data = data.with_columns(pl.when(pl.col('secondary_type').is_in(["Poked", "Batted", "Between Legs"])).then(pl.lit(None)).otherwise(pl.col('secondary_type')).alias("secondary_type"))

    train = data.filter(~pl.col('secondary_type').is_null())
    test = data.filter(pl.col('secondary_type').is_null())

    exclude_cols = ['game_id', 'event_idx']
    features = [t_col for t_col in train.columns if t_col not in exclude_cols and t_col != "secondary_type"]

    train_features = train.select(features)
    impute_features = test.select(features)


    # Label encode the target variable
    le = LabelEncoder()
    train_target = le.fit_transform(
        train.select('secondary_type')['secondary_type']
    )
    
    # Train a classifier
    classifier = XGBClassifier()
    classifier.fit(train_features, train_target)

    # Predict missing values
    predicted_values = classifier.predict(impute_features)

    # Decode predicted values
    imputed_labels = le.inverse_transform(predicted_values)

    # Use map_elements method to replace missing values
    test = test.with_columns(pl.lit(imputed_labels).alias("event_detail"))
    train = train.with_columns(pl.col('secondary_type').alias("event_detail"))
    final_df = train.vstack(test)
    final_df = (
        final_df
        .sort('season', 'game_id', 'event_idx')
        .with_columns(
        (pl.when(pl.col('event_detail') == "Wrist").then(pl.lit(1)).otherwise(pl.lit(0))).alias('wrist_shot'),
        (pl.when(pl.col('event_detail') == "Deflected").then(pl.lit(1)).otherwise(pl.lit(0))).alias('deflected_shot'),
        (pl.when(pl.col('event_detail') == "Tip-In").then(pl.lit(1)).otherwise(pl.lit(0))).alias('tip_shot'),
        (pl.when(pl.col('event_detail') == "Slap").then(pl.lit(1)).otherwise(pl.lit(0))).alias('slap_shot'),
        (pl.when(pl.col('event_detail') == "Backhand").then(pl.lit(1)).otherwise(pl.lit(0))).alias('backhand_shot'),
        (pl.when(pl.col('event_detail') == "Snap").then(pl.lit(1)).otherwise(pl.lit(0))).alias('snap_shot'),
        (pl.when(pl.col('event_detail') == "Wrap-Around").then(pl.lit(1)).otherwise(pl.lit(0))).alias('wrap_shot')
        )
    )

    # Calculate Differences In New Shot Types
    null_df = final_df.filter(pl.col('secondary_type').is_null())
    rws = null_df.height
    val_cts = null_df['event_detail'].value_counts()
    val_cts = val_cts.with_columns(((pl.col('count')*100 / rws).round(2)).alias('Percent'))
    val_cts = val_cts.with_columns(((val_cts['count'].map_elements(lambda x: f"{x:,.0f}")) + ' ' + (val_cts['Percent'].map_elements(lambda x: f"({x:.2f}%)"))).alias('Label')).sort("count", descending=True).drop('count', 'Percent')
    print("Rows Imputated Using XGB MultiClassifier of Null Shot Types (Blocked And Missed Shots): "+ str(rws))
    print(val_cts)
    
    return final_df.drop('secondary_type')

def load_model_data(start=st_yr, end=end_yr):
    ## BEGIN LOAD ##
    ev_dfs = []
    pp_dfs = []
    sh_dfs = []
    en_dfs = []

    print("================== Begin Loading + Cleaning Individual Seasons ==================")
    print(" ")

    for i in range(start,end):

        # Begin Load
        print(f"Now Loading Play by Play Data From {i}-{i+1} NHL Season")

        # Basic Clean/Manipulation
        df = clean_pbp_data(pl.read_parquet(f'https://raw.githubusercontent.com/twinfield10/NHL-Data/main/PBP/parquet/API_RAW_PBP_Data_{i}{i+1}.parquet'))

        # Create Indexes
        df = index_input_data(df)

        if i == 2023:
            CURRENT_DF = df
        else:
            pass

        # Split by Strength
        ev, pp, sh, en = split_by_strength(df)

        # Prep For Model (OHE + Other Features)
        ev = model_prep(ev, "EV")
        pp = model_prep(pp, "PP")
        sh = model_prep(sh, "SH")
        en = model_prep(en, "EN")

        # Append the modified dataframe to the list
        ev_dfs.append(ev)
        pp_dfs.append(pp)
        sh_dfs.append(sh)
        en_dfs.append(en)

        # Create the PBP DataFrames
    print(" ")
    print("================== Begin Appending DataFrames Together ==================")
    print(" ")

    # EV
    EV_PBP = ev_dfs[0]
    for df in ev_dfs[1:]:
        EV_PBP = EV_PBP.extend(df)
    print(str(EV_PBP.height) + " Total Shots in Even Strength DF")

    # PP
    PP_PBP = pp_dfs[0]
    for df in pp_dfs[1:]:
        PP_PBP = PP_PBP.extend(df)
    print(str(PP_PBP.height) + " Total Shots in Power Play DF")

    # SH
    SH_PBP = sh_dfs[0]
    for df in sh_dfs[1:]:
        SH_PBP = SH_PBP.extend(df)
    print(str(SH_PBP.height) + " Total Shots in Short Handed (Offense) DF")

    # EN
    EN_PBP = en_dfs[0]
    for df in en_dfs[1:]:
        EN_PBP = EN_PBP.extend(df)
    print(str(EN_PBP.height) + " Total Shots in Empty Net DF")

    # Imputate + Clean Null Values of Shot Type
    print(" ")
    print("================== Begin Imputing Null Shot Type Values ==================")
    print(" ")

    EV_PBP = imp_sec_type(EV_PBP).drop('event_detail', 'event_team_toi', 'def_team_toi').with_columns(pl.lit('EV').alias('strength_type'))
    PP_PBP = imp_sec_type(PP_PBP).drop('event_detail', 'event_team_toi', 'def_team_toi').with_columns(pl.lit('PP').alias('strength_type'))
    SH_PBP = imp_sec_type(SH_PBP).drop('event_detail', 'event_team_toi', 'def_team_toi').with_columns(pl.lit('SH').alias('strength_type'))
    EN_PBP = imp_sec_type(EN_PBP).drop('event_detail', 'event_team_toi', 'def_team_toi').with_columns(pl.lit('EN').alias('strength_type'))

    print("================== End Loading Data From the " + str(EN_PBP['season'].min()) + " Season to the " + str(EN_PBP['season'].max())  + " Season ==================")

    return EV_PBP, PP_PBP, SH_PBP, EN_PBP

EV_PBP, PP_PBP, SH_PBP, EN_PBP = load_model_data(2010, 2024)