# initalload.py
from requirements import *

# Constants - Team Abbreviation
team_abbr_dict = {
    24: 'ANA',           # Anaheim Ducks         
    53: 'ARI',           # Arizona Coyotes       
    11: 'ATL',           # Atlanta Thrashers     
    6 : 'BOS',           # Boston Bruins         
    7 : 'BUF',           # Buffalo Sabres        
    20: 'CGY',           # Calgary Flames        
    12: 'CAR',           # Carolina Hurricanes   
    16: 'CHI',           # Chicago Blackhawks    
    21: 'COL',           # Colorado Avalanche    
    29: 'CBJ',           # Columbus Blue Jackets 
    25: 'DAL',           # Dallas Stars          
    17: 'DET',           # Detroit Red Wings     
    22: 'EDM',           # Edmonton Oilers       
    13: 'FLA',           # Florida Panthers      
    26: 'LAK',           # Los Angeles Kings     
    30: 'MIN',           # Minnesota Wild        
    8 : 'MTL',           # Montréal Canadiens    
    18: 'NSH',           # Nashville Predators   
    1 : 'NJD',           # New Jersey Devils     
    2 : 'NYI',           # New York Islanders    
    3 : 'NYR',           # New York Rangers      
    9 : 'OTT',           # Ottawa Senators       
    4 : 'PHI',           # Philadelphia Flyers   
    27: 'PHX',           # Phoenix Coyotes       
    5 : 'PIT',           # Pittsburgh Penguins   
    28: 'SJS',           # San Jose Sharks       
    55: 'SEA',           # Seattle Kraken        
    19: 'STL',           # St. Louis Blues       
    14: 'TBL',           # Tampa Bay Lightning   
    10: 'TOR',           # Toronto Maple Leafs   
    23: 'VAN',           # Vancouver Canucks     
    54: 'VGK',           # Vegas Golden Knights  
    15: 'WSH',           # Washington Capitals   
    52: 'WPG'            # Winnipeg Jets
}

away_teams = pl.DataFrame([{'away_team_id': team_id, 'away_abbreviation': team_abbr} for team_id, team_abbr in team_abbr_dict.items()]).with_columns(pl.col('away_team_id').cast(pl.Utf8))
home_teams = pl.DataFrame([{'home_team_id': team_id, 'home_abbreviation': team_abbr} for team_id, team_abbr in team_abbr_dict.items()]).with_columns(pl.col('home_team_id').cast(pl.Utf8))

# Constants - Time Zone Conversions #
est = pytz.timezone('US/Eastern')
utc = pytz.utc
fmt = "%Y-%m-%dT%H:%M:%SZ"
day_fmt = "%Y-%m-%d"
time_fmt = "%H:%M"

# Constants - Link Bases #
fastR_base = 'https://raw.githubusercontent.com/sportsdataverse/fastRhockey-data/main/nhl/schedules/parquet/nhl_schedule_'
pbp_link_pre = 'https://api-web.nhle.com/v1/gamecenter/'
pbp_link_suf = '/play-by-play'
shift_link = 'https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId='

# Constant - Helper Function (Covert GameTime to Seconds)
def min_to_sec(time_str):
    """This function will help to convert time's formatted like MM:SS to a round seconds number"""
    if time_str is None:
        return None
    minutes, seconds = map(int, time_str.split(':'))
    return minutes * 60 + seconds

# Constants - PBP RAW Schema #
raw_schema = {
    'id': 'i32',
    'gameDate': 'str',
    'season': 'i32',
    'sortOrder': 'i32',
    'gameType': 'i32',
    'period': 'i32',
    'periodType': 'str',
    'timeRemaining': 'str',
    'timeInPeriod': 'str',
    'situationCode': 'str',
    'homeTeamDefendingSide': 'str',
    'eventOwnerTeamId': 'str',
    'awayTeam.id': 'str',
    'awayTeam.abbrev': 'str',
    'awayScore': 'f32',
    'homeTeam.id': 'str',
    'homeTeam.abbrev': 'str',
    'homeScore': 'f32',
    'eventId': 'i32',
    'typeCode': 'i32',
    'penaltytTypeCode': 'str',
    'typeDescKey': 'str',
    'descKey': 'str',
    'reason': 'str',
    'secondaryReason': 'str',
    'shotType': 'str',
    'zoneCode': 'str',
    'xCoord': 'f32',
    'yCoord': 'f32',
    'scoringPlayerId': 'str',
    'shootingPlayerId': 'str',
    'goalieInNetId': 'str',
    'blockingPlayerId': 'str',
    'committedByPlayerId': 'str',
    'drawnByPlayerId': 'str',
    'servedByPlayerId': 'str',
    'duration': 'str',
    'hittingPlayerId': 'str',
    'hitteePlayerId': 'str',
    'winningPlayerId': 'str',
    'losingPlayerId': 'str',
    'assist1PlayerId': 'str',
    'assist2PlayerId': 'str',
    'playerId': 'str'    
}

# PBP Helper Functions #
def ping_nhl_api(i):
            """This function will get the raw data from the NHL API.
                It will then save two files:

                1) Roster Data From Each Game
                    a) This will be used to create features like handiness and position
                2) Play By Play Data From Each Game
                    a) Within PBP data, we will need to normalize the json object stored in 'details'
            to ensure we collect every detail from each event"""

            # 1) Create Link For API Endpoint
            pbp_link = pbp_link_pre+str(i)+pbp_link_suf

            # 2) Get Game Data From Response
            pbp_response = requests.get(pbp_link).json()
            game_data = pl.DataFrame({
                    'id': pbp_response.get('id'),
                    'season': pbp_response.get('season'),
                    'gameDate': pbp_response.get('gameDate'),
                    'gameType': pbp_response.get('gameType'),
                    'awayTeam.id': pbp_response.get('awayTeam', {}).get('id'),
                    'awayTeam.abbrev': pbp_response.get('awayTeam', {}).get('abbrev'),
                    'homeTeam.id': pbp_response.get('homeTeam', {}).get('id'),
                    'homeTeam.abbrev': pbp_response.get('homeTeam', {}).get('abbrev'),
                    'key': 1
                })

            # 3) Get Plays Data (Stored As List)
            raw_list = pbp_response.get('plays', [])

            # 4) Normalize Details Dictionary
            ### a) Create Keys To Compare Set
            keys_to_compare = {
                'descKey','reason','secondaryReason','shotType', #Event Description
                'xCoord','yCoord','zoneCode', # Location
                'homeScore','awayScore','homeSOG','awaySOG', 'scoringPlayerTotal','assist1PlayerTotal','assist2PlayerTotal', # Game Details
                'eventOwnerTeamId', # Team ID
                'goalieInNetId','scoringPlayerId','assist1PlayerId','assist2PlayerId','shootingPlayerId','blockingPlayerId', # Player IDs (Shots)
                'winningPlayerId','losingPlayerId','hittingPlayerId','hitteePlayerId','playerId', # Faceoff/Hit/GiveTakeAway Player IDs
                'typeCode', 'committedByPlayerId', 'drawnByPlayerId','servedByPlayerId','duration' # Penalty IDs
            }
            ### b) Loop Through Rows To Get Complete Details Dictionary 
            for entry in raw_list:
                details_dict = entry.get("details", {})

                ##### i) Check For Extra Keys
                extra_keys = set(details_dict.keys()) - keys_to_compare
                if extra_keys:
                    print(f"GameID: {i} | Extra keys in details_dict: {extra_keys}")

                ##### ii) Update Details To Include All Keys
                entry["details"] = {key: details_dict.get(key, None) for key in keys_to_compare}

            # 5) Build New DataFrame With Full Details
            plays_raw =  (
                pl.DataFrame(raw_list)
                .rename({"typeCode":"eventTypeCode"})
                .unnest('periodDescriptor')
                .unnest('details')
                .rename({"typeCode":"penaltyTypeCode","eventTypeCode":"typeCode"})
                .with_columns(pl.lit(1).cast(pl.Int64).alias('key'))
            )

            return_df = game_data.join(plays_raw, on=pl.col("key"), how="inner").drop("key")

            return return_df
def align_and_cast_columns(data, sch):
            # Identify missing and extra columns
            extra_cols_int = set(data.columns) - set(sch.keys())
            data = data.drop(extra_cols_int)

            # Fill missing columns with null values and cast to the correct type
            for col in sch.keys():
            
                col_type = sch.get(col)

                if (col in data.columns) & (col_type == 'str'):
                    data = data.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
                elif (col in data.columns) & (col_type == 'i32'):
                    data = data.with_columns(pl.col(col).cast(pl.Int32).alias(col))
                elif (col in data.columns) & (col_type == 'f32'):
                    data = data.with_columns(pl.col(col).cast(pl.Float32).alias(col))
                elif (col not in data.columns) & (col_type == 'str'):
                    data = data.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
                elif (col not in data.columns) & (col_type == 'f32'):
                    data = data.with_columns(pl.lit(None).cast(pl.Float32).alias(col))

            # Select columns and update schema
            data = data.select(sch.keys())

                # Create Dictionaries For Column Name/Value Rename
            rename_dict = {
                "id": "game_id",
                "gameDate": "game_date",
                "awayTeam.id": "away_id",
                "awayTeam.abbrev": "away_abbreviation",
                "homeTeam.id": "home_id",
                "homeTeam.abbrev": "home_abbreviation",
                "gameType": "season_type",
                "eventId": "event_id",
                "typeDescKey": "event_type",
                "sortOrder": "event_idx",
                "periodType": "period_type",
                "eventOwnerTeamId": "event_team_id",
                "xCoord": "x",
                "yCoord": "y",
                "zoneCode": "event_zone",
                "shotType": "secondary_type",
                "awayScore": "away_score",
                "homeScore": "home_score",
                "goalieInNetId": "event_goalie_id",
                "blockingPlayerId": "blocking_player_id",
                "drawnByPlayerId": "drawnby_player_id",
                "servedByPlayerId": "servedby_player_id",
                "committedByPlayerId": "committedby_player_id",
                "hittingPlayerId": "hitting_player_id",
                "hitteePlayerId": "hittee_player_id",
                "assist1PlayerId": "assist_1_player_id",
                "assist2PlayerId": "assist_2_player_id",
                "shootingPlayerId": "shooting_player_id",
                "reason": "reason",
                "scoringPlayerId": "scoring_player_id",
                "duration": "penalty_minutes",
                "winningPlayerId": "winning_player_id",
                "losingPlayerId": "losing_player_id"
            }

            # Event Type
            event_type_dict = {
                "faceoff": "FACEOFF",
                "shot-on-goal": "SHOT",
                "stoppage": "STOPPAGE",
                "hit": "HIT",
                "blocked-shot": "BLOCKED_SHOT",
                "missed-shot": "MISSED_SHOT",
                "giveaway": "GIVEAWAY",
                "takeaway": "TAKEAWAY",
                "penalty": "PENALTY",
                "goal": "GOAL",
                "period-start": "PERIOD_START",
                "period-end": "PERIOD_END",
                "delayed-penalty": "DELAYED_PENALTY",
                "game-end": "GAME_END",
                "shootout-complete": "SHOOTOUT_COMPLETE",
                "failed-shot-attempt": "FAILED_SHOT",
                None:None
            }

            # Season Type
            season_type_dict = {
                2: "R",
                3: "P",
                None:None
            }

            # Shot Type
            shot_type_dict = {
                "snap": "Snap",
                "between-legs": "Between Legs",
                "wrap-around": "Wrap-Around",
                "tip-in": "Tip-In",
                "cradle": "Wrap-Around",
                "poke": 'Poked',
                "bat": 'Batted',
                "deflected": "Deflected",
                "wrist": "Wrist",
                "slap":	"Slap",
                "backhand": "Backhand",
                None: None
            }

            # Rename Columns + Values AND Add Event/Season Type Helpers
            data = data.rename(rename_dict).filter((pl.col('period_type') != 'SO') & (pl.col('season_type').is_in([2, 3])))

            data = (
                data
                .with_columns([
                    (pl.col('season_type').replace(season_type_dict, default = pl.col('season_type'))).alias('season_type'),
                    (pl.col('event_type').replace(event_type_dict,default = pl.col('event_type'))).alias('event_type'),
                    (pl.col('secondary_type').replace(shot_type_dict,default = pl.col('secondary_type'))).alias('secondary_type'),
                    pl.when(pl.col('event_team_id') == pl.col('home_id')).then(pl.lit('home')).otherwise(pl.lit('away')).alias('event_team_type'),
                    pl.when(pl.col('event_team_id') == pl.col('home_id')).then(pl.col('home_abbreviation')).otherwise(pl.col('away_abbreviation')).alias('event_team_abbr')
                    ])
                #.drop('gameType', 'typeDescKey', 'shotType')
                .filter(~pl.col('situationCode').is_in(["PERIOD_START", "PERIOD_END", "GAME_START", "GAME_END"]))
            )

            # Create Game and Period Seconds Remaining from timeInPeriod, timeRemaining: 'period', 'period_seconds', 'period_seconds_remaining', 'game_seconds', 'game_seconds_remaining'
            data = (
                data
                .with_columns(pl.when(pl.col('timeInPeriod').is_null()).then(pl.lit(None)).otherwise(pl.col('timeInPeriod').map_elements(min_to_sec)).alias('period_seconds'))
                .with_columns([
                    (1200 - pl.col('period_seconds')).alias('period_seconds_remaining'),
                    (pl.col('period_seconds') + ((pl.col('period')-1)*1200)).alias('game_seconds'),
                    ((3600 - pl.col('period_seconds')) + ((pl.col('period') - 3) * 1200)).alias('game_seconds_remaining')
                ])
            )

            # Create event_player_1_id and event_player_2_id columns based on event_type and corresponding columns
            remove_ply_ids = ['winning_player_id', 'hitting_player_id', 'scoring_player_id', 'shooting_player_id', 'committedby_player_id',
                              'playerId', 'losing_player_id', 'hittee_player_id', 'drawnby_player_id', 'assist_1_player_id', 'assist_2_player_id',
                              'blocking_player_id']
            data = (
                data
                .with_columns([
                    (pl.when(pl.col('event_type') == 'FACEOFF').then(pl.col('winning_player_id'))
                       .when(pl.col('event_type') == 'HIT').then(pl.col('hitting_player_id'))
                       .when(pl.col('event_type') == 'GOAL').then(pl.col('scoring_player_id'))
                       .when(pl.col('event_type').is_in(['SHOT', 'MISSED_SHOT', "BLOCKED_SHOT"])).then(pl.col('shooting_player_id'))
                       .when(pl.col('event_type') == 'PENALTY').then(pl.col('committedby_player_id'))
                       .when(pl.col('event_type') == 'GIVEAWAY').then(pl.col('playerId'))
                       .when(pl.col('event_type') == 'TAKEAWAY').then(pl.col('playerId'))
                       .otherwise(pl.lit(None))
                     ).alias("event_player_1_id"),
                     (pl.when(pl.col('event_type') == 'FACEOFF').then(pl.col('losing_player_id'))
                       .when(pl.col('event_type') == 'HIT').then(pl.col('hittee_player_id'))
                       .when(pl.col('event_type').is_in(['GOAL','SHOT', 'MISSED_SHOT', 'BLOCKED_SHOT'])).then(pl.col('event_goalie_id'))
                       .when(pl.col('event_type') == 'PENALTY').then(pl.col('drawnby_player_id'))
                       .otherwise(pl.lit(None))
                     ).alias("event_player_2_id"),
                     (pl.when((pl.col('event_type') == 'GOAL') & (~pl.col('assist_1_player_id').is_null())).then(pl.col('assist_1_player_id'))
                       .when((pl.col('event_type') == 'PENALTY') & (~pl.col('servedby_player_id').is_null())).then(pl.col('servedby_player_id'))
                       .when((pl.col('event_type') == 'BLOCKED_SHOT') & (~pl.col('blocking_player_id').is_null())).then(pl.col('blocking_player_id'))
                       .otherwise(pl.lit(None))
                     ).alias("event_player_3_id"),
                     (pl.when((pl.col('event_type') == 'GOAL') & (~pl.col('assist_2_player_id').is_null())).then(pl.col('assist_2_player_id'))
                       .otherwise(pl.lit(None))
                     ).alias("event_player_4_id"),
                     (pl.when(pl.col('event_type') == 'FACEOFF').then(pl.lit('Winner'))
                       .when(pl.col('event_type') == 'HIT').then(pl.lit('Hitter'))
                       .when(pl.col('event_type') == 'GOAL').then(pl.lit('Scorer'))
                       .when(pl.col('event_type').is_in(['SHOT', 'MISSED_SHOT', "BLOCKED_SHOT"])).then(pl.lit('Shooter'))
                       .when(pl.col('event_type') == 'PENALTY').then(pl.lit('PenaltyOn'))
                       .when(pl.col('event_type') == 'GIVEAWAY').then(pl.lit('PlayerID'))
                       .when(pl.col('event_type') == 'TAKEAWAY').then(pl.lit('PlayerID'))
                       .otherwise(pl.lit(None))
                     ).alias("event_player_1_type"),
                     (pl.when(pl.col('event_type') == 'FACEOFF').then(pl.lit('Loser'))
                       .when(pl.col('event_type') == 'HIT').then(pl.lit('Hittee'))
                       .when((pl.col('event_type') == 'GOAL') & (~pl.col('event_goalie_id').is_null())).then(pl.lit('Goalie'))
                       .when((pl.col('event_type') == 'GOAL') & (pl.col('event_goalie_id').is_null())).then(pl.lit('EmptyNet'))
                       .when(pl.col('event_type').is_in(['SHOT', 'MISSED_SHOT', 'BLOCKED_SHOT'])).then(pl.lit('Goalie'))
                       .when(pl.col('event_type') == 'PENALTY').then(pl.lit('DrewBy'))
                       .when(pl.col('event_type') == 'GIVEAWAY').then(pl.lit('PlayerID'))
                       .when(pl.col('event_type') == 'TAKEAWAY').then(pl.lit('PlayerID'))
                       .otherwise(pl.lit(None))
                     ).alias("event_player_2_type"),
                     (pl.when((pl.col('event_type') == 'GOAL') & (~pl.col('assist_1_player_id').is_null())).then(pl.lit('Assist'))
                       .when((pl.col('event_type') == 'PENALTY') & (~pl.col('servedby_player_id').is_null())).then(pl.lit('ServedBy'))
                       .when((pl.col('event_type') == 'BLOCKED_SHOT') & (~pl.col('blocking_player_id').is_null())).then(pl.lit('Blocker'))
                       .otherwise(pl.lit(None))
                     ).alias("event_player_3_type"),
                     (pl.when((pl.col('event_type') == 'GOAL') & (~pl.col('assist_2_player_id').is_null())).then(pl.lit('Assist'))
                       .otherwise(pl.lit(None))
                     ).alias("event_player_4_type")
                ])
                .drop(remove_ply_ids)
            )
            # Parse Situation Code For Home/Away Skaters/EmptyNet
            data = (
                data
                .sort('season', 'game_id', 'period', 'event_idx')
                .with_columns(
                    pl.when(pl.col('situationCode').is_null()).then(pl.col("situationCode").fill_null(strategy="forward")).otherwise(pl.col('situationCode')).alias('situationCode')
                )
                .filter(~pl.col('situationCode').is_in(['0101', '1010']))
                .with_columns([
                    pl.when(pl.col("situationCode").str.slice(0, 1).cast(pl.Int32) == 0).then(pl.lit(1)).otherwise(pl.lit(0)).alias("away_en"),
                    pl.when(pl.col("situationCode").str.slice(3, 1).cast(pl.Int32) == 0).then(pl.lit(1)).otherwise(pl.lit(0)).alias("home_en"),
                    pl.col("situationCode").str.slice(1, 1).cast(pl.Int32).alias("away_skaters"),
                    pl.col("situationCode").str.slice(2, 1).cast(pl.Int32).alias("home_skaters")
                ])
                .with_columns([
                    (pl.concat_str([pl.col('home_skaters'), pl.lit('v'), pl.col('away_skaters')])).alias('strength_state'),
                    pl.when(pl.col('away_en') == 1).then(pl.lit('E')).otherwise(pl.col('away_skaters')).alias('true_away_skaters'),
                    pl.when(pl.col('home_en') == 1).then(pl.lit('E')).otherwise(pl.col('home_skaters')).alias('true_home_skaters')
                ])
                .with_columns((pl.concat_str([pl.col('true_home_skaters'), pl.lit('v'), pl.col('true_away_skaters')])).alias('true_strength_state'))
                .drop(['true_away_skaters', 'true_home_skaters'])
            )

            # Create x_fixed and y_fixed. These coordinates will be relative to the event team's attacking zone (i.e., x_abs is positive)
            data = (
                data
                .with_columns([
                    pl.when((pl.col('event_zone') == 'O') & (pl.col('x').mean() > 0)).then(pl.lit(1)).otherwise(pl.lit(-1)).alias('flipped_coords')
                ])
                .with_columns([
                    # Where homeTeamDefendingSide Exists
                    (pl.when((~pl.col('homeTeamDefendingSide').is_null()) &
                             ( pl.col('homeTeamDefendingSide') == 'left') &
                             ( pl.col('event_team_type') == 'home'))
                             .then(pl.col('x'))
                       .when((~pl.col('homeTeamDefendingSide').is_null()) &
                             ( pl.col('homeTeamDefendingSide') == 'right') &
                             ( pl.col('event_team_type') == 'home'))
                             .then(pl.col('x')*-1)
                       .when((~pl.col('homeTeamDefendingSide').is_null()) &
                             ( pl.col('homeTeamDefendingSide') == 'left') &
                             ( pl.col('event_team_type') == 'away'))
                             .then(pl.col('x')*-1)
                       .when((~pl.col('homeTeamDefendingSide').is_null()) &
                             ( pl.col('homeTeamDefendingSide') == 'right') &
                             ( pl.col('event_team_type') == 'away'))
                             .then(pl.col('x'))
                      # Where homeTeamDefendingSide does not exist
                      .when((pl.col('homeTeamDefendingSide').is_null()) &
                            (pl.col('event_zone') == 'O'))
                            .then(pl.col('x').abs())
                      .when((pl.col('homeTeamDefendingSide').is_null()) &
                            (pl.col('event_zone') == 'D'))
                            .then((pl.col('x').abs())*-1)
                      .when((pl.col('homeTeamDefendingSide').is_null()) &
                            (pl.col('event_zone') == 'N'))
                            .then((pl.col('x')) * (pl.col('flipped_coords').max().over(['season', 'game_id', 'period'])))
                      .otherwise(pl.lit(None)).alias('x_abs')
                    ),
                    # Where homeTeamDefendingSide does exist
                    (pl.when((~pl.col('homeTeamDefendingSide').is_null()) &
                             ( pl.col('homeTeamDefendingSide') == 'left') &
                             ( pl.col('event_team_type') == 'home'))
                             .then(pl.col('y'))
                       .when((~pl.col('homeTeamDefendingSide').is_null()) &
                             ( pl.col('homeTeamDefendingSide') == 'right') &
                             ( pl.col('event_team_type') == 'home'))
                             .then(pl.col('y')*-1)
                       .when((~pl.col('homeTeamDefendingSide').is_null()) &
                             ( pl.col('homeTeamDefendingSide') == 'left') &
                             ( pl.col('event_team_type') == 'away'))
                             .then(pl.col('y')*-1)
                       .when((~pl.col('homeTeamDefendingSide').is_null()) &
                             ( pl.col('homeTeamDefendingSide') == 'right') &
                             ( pl.col('event_team_type') == 'away'))
                             .then(pl.col('y'))
                      # Where homeTeamDefendingSide does not exist
                      .when((pl.col('homeTeamDefendingSide').is_null()) &
                            (pl.col('event_zone') == 'O'))
                            .then(pl.col('y').abs())
                      .when((pl.col('homeTeamDefendingSide').is_null()) &
                            (pl.col('event_zone') == 'D'))
                            .then((pl.col('y').abs())*-1)
                      .when((pl.col('homeTeamDefendingSide').is_null()) &
                            (pl.col('event_zone') == 'N'))
                            .then((pl.col('y')) * (pl.col('flipped_coords').max().over(['season', 'game_id', 'period'])))
                      .otherwise(pl.lit(None)).alias('y_abs')
                    )
                ])
                .drop("flipped_coords")
            )

            # Create Event Distance Calculation
            data = data.with_columns(
                pl.when(pl.col('x_abs') >= 0).then(pl.Series.sqrt((89 - pl.Series.abs(data['x_abs']))**2 + data['y_abs']**2))
                  .when(pl.col('x_abs') <  0).then(pl.Series.sqrt((pl.Series.abs(data['x_abs']) + 89)**2 + data['y_abs']**2))
                  .alias('event_distance')
            )

            # Create Event Angle Calculation
            data = (
                data
                .with_columns(
                pl.when(data['x_abs'] >= 0)
                  .then(pl.Series.arctan(data['y_abs'] / (89 - pl.Series.abs(data['x_abs'])))
                        .map_elements(lambda x: abs(x * (180 / pi))))
                  .when(data['x_abs'] < 0)
                  .then(pl.Series.arctan(data['y_abs'] / (pl.Series.abs(data['x_abs']) + 89))
                        .map_elements(lambda x: abs(x * (180 / pi))))
                  .alias('event_angle')
                )
                .with_columns(
                    pl.when(pl.col('x_abs') > 89).then((180 - pl.col('event_angle'))).otherwise(pl.col('event_angle')).alias('event_angle')
                )
            )

            return data      
def append_shift_data(data, roster_data):
            """ This function will load shift data allowing the user to see which players are on the ice at a given time in each game"""
            # Load Game ID and Home/Away Ids
            i = data['game_id'][0]
            bad_shift_ids = []

            game_info_slim = (
                data
                .filter(pl.col('game_id') == i)
                .select('game_id', 'home_id', 'away_id', 'period', 'game_seconds', 'period_seconds', 'event_id', 'event_idx', 'event_type')
                .unique()
            )

            shift_link = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId="+str(i)
            shift_response = requests.get(shift_link).json()

            # Assuming "data" is the key containing nested data
            data_list = shift_response.get('data', [])
            keep_keys = ['id', 'endTime', 'firstName', 'gameId', 'lastName', 'period', 'playerId', 'startTime', 'teamAbbrev', 'teamId', 'duration']
            filtered_data = [{key: item[key] for key in keep_keys} for item in data_list]
            shift_raw = pl.DataFrame(filtered_data)
            try:
                shift_raw = (
                    shift_raw
                    .with_columns([
                        pl.col('endTime').str.len_bytes().alias('endTime_min'),
                        pl.col('startTime').str.len_bytes().alias('startTime_min'),
                        pl.when(pl.col('startTime').str.len_bytes() == 4).then(pl.concat_str(pl.lit('0'), pl.col('startTime'))).otherwise(pl.col('startTime')).alias('startTime'),
                        pl.when(pl.col('endTime').str.len_bytes() == 4).then(pl.concat_str(pl.lit('0'), pl.col('endTime'))).otherwise(pl.col('endTime')).alias('endTime')
                    ])
                    .filter((pl.col('startTime_min') != 0) & (pl.col('endTime_min') != 0))
                    .drop('startTime_min', 'endTime_min')
                    .with_columns([
                        (pl.col('firstName') + ' ' + pl.col('lastName')).alias('player_name'),
                        ((pl.col('startTime').str.slice(0, 2).cast(pl.Int32) * 60) + (pl.col('startTime').str.slice(3, 5).cast(pl.Int32))).alias('period_start_seconds'),
                        ((pl.col('endTime').str.slice(0, 2).cast(pl.Int32) * 60) + (pl.col('endTime').str.slice(3, 5).cast(pl.Int32))).alias('period_end_seconds')
                    ])
                    .with_columns([
                        (pl.col('period_start_seconds') + ((pl.col('period') - 1) * 1200)).alias('game_start_seconds'),
                        (pl.col('period_end_seconds') + ((pl.col('period') - 1) * 1200)).alias('game_end_seconds'),
                    ])
                    .rename({
                            'gameId': 'game_id',
                            'id': 'shift_id',
                            'playerId': 'player_id',
                            'teamId': 'team_id',
                            'teamAbbrev': 'team_abbr'
                        })
                    .select([pl.col('game_id').cast(pl.Int32),
                             pl.col('team_id').cast(pl.Utf8),
                             pl.col('player_id').cast(pl.Utf8),
                             pl.col('player_name').str.to_uppercase().cast(pl.Utf8),
                             pl.col('team_abbr').cast(pl.Utf8),
                             pl.col('period').cast(pl.Int32),
                             pl.col('period_start_seconds').cast(pl.Int64),
                             pl.col('period_end_seconds').cast(pl.Int64),
                             pl.col('game_start_seconds').cast(pl.Int64),
                             pl.col('game_end_seconds').cast(pl.Int64)
                             ]) #'shift_id', 'typeCode', 'shift_number', 'eventNumber'
                )

                shift_raw = (
                    # Join and Create team_type
                    shift_raw
                    .join(game_info_slim.select('game_id', 'home_id', 'away_id').unique(), on='game_id', how='left')
                    .filter((pl.col('home_id') == pl.col('team_id')) | (pl.col('away_id') == pl.col('team_id')))
                    .filter(pl.col('game_start_seconds') != pl.col('game_end_seconds') )
                    .with_columns(pl.when(pl.col('home_id') == pl.col('team_id')).then(pl.lit('home'))
                                    .when(pl.col('away_id') == pl.col('team_id')).then(pl.lit('away')).otherwise(pl.lit(None)).alias('team_type'))
                    .drop('home_id', 'away_id')
                    .unique()
                )
                # Combine Consecutive Shifts
                gb_cols = [col for col in shift_raw.columns if col not in ['period_start_seconds', 'game_start_seconds']]
                shift_raw = (
                    shift_raw
                    .sort('game_start_seconds')
                    .with_columns([
                        pl.col('period_start_seconds').max().over(gb_cols).alias('period_start_seconds'),
                        pl.col('game_start_seconds').max().over(gb_cols).alias('game_start_seconds')#,
                        #pl.col('eventNumber').max().over(gb_cols).alias('eventNumber')
                    ])
                    #.unique()
                    # Separate Goalies
                    .join(roster_data.with_columns([
                        (pl.col('player_id').cast(pl.Utf8).alias('player_id')),
                        (pl.col('pos_G').cast(pl.Int32).alias('pos_G'))
                    ])
                    .select('player_id', 'pos_G'), on='player_id', how='left')
                    .unique()
                )
                # Concat Player IDs into lists for each group (i.e. event and seconds)
                result_df = (
                    shift_raw
                    .sort('game_start_seconds')
                    .group_by(['game_id', 'period', 'period_start_seconds', 'period_end_seconds', 'team_type', 'pos_G'])
                    .agg(
                        pl.concat_list('player_id').flatten().unique().alias('player_id_list'),
                        pl.concat_list('player_name').flatten().unique().alias('player_name_list')
                        )
                )
                # Separate and Create Player On Columns
                game_data = (
                     game_info_slim
                    .filter(pl.col('game_id') == i)
                    .sort('game_seconds', 'event_idx')
                )
                def apply_player_lists_pl(x, ty, pos, shift, output):
                    return get_player_lists_pl((x['game_id'], x['period'], x['period_seconds'], ty, pos, shift, output))
                def get_player_lists_pl(x):
                    # Outline Variables
                    g_id, per, p_secs, ty, pos, shift, output = x
                    # Adjust conditions as needed
                    conditions = (
                        (result_df['game_id'] == g_id) &
                        (result_df['period'] == per) &
                        (result_df['team_type'] == ty) &
                        (result_df['pos_G'] == pos)
                    )
                    if shift == 'current':
                        conditions &= (
                            (result_df['period_start_seconds'] < p_secs) &
                            (result_df['period_end_seconds'] > p_secs)
                        )
                    elif shift == 'on':
                        conditions &= (result_df['period_start_seconds'] == p_secs)
                    elif shift == 'off':
                        conditions &= (result_df['period_end_seconds'] == p_secs)
                    filtered_rows = result_df.filter(conditions)
                    if output == 'id':
                        result_list = set(filtered_rows['player_id_list'].explode().to_list())
                    elif output == 'name':
                        result_list = set(filtered_rows['player_name_list'].explode().to_list())
                    return ','.join(str(item) for item in result_list)

                # List of columns to generate
                columns_to_generate = [
                    ('home', 0, 'current', 'id'),
                    ('home', 0, 'current', 'name'),
                    ('home', 0, 'on', 'id'),
                    ('home', 0, 'on', 'name'),
                    ('home', 0, 'off', 'id'),
                    ('home', 0, 'off', 'name'),
                    ('away', 0, 'current', 'id'),
                    ('away', 0, 'current', 'name'),
                    ('away', 0, 'on', 'id'),
                    ('away', 0, 'on', 'name'),
                    ('away', 0, 'off', 'id'),
                    ('away', 0, 'off', 'name'),
                    ('home', 1, 'current', 'id'),
                    ('home', 1, 'current', 'name'),
                    ('home', 1, 'on', 'id'),
                    ('home', 1, 'on', 'name'),
                    ('home', 1, 'off', 'id'),
                    ('home', 1, 'off', 'name'),
                    ('away', 1, 'current', 'id'),
                    ('away', 1, 'current', 'name'),
                    ('away', 1, 'on', 'id'),
                    ('away', 1, 'on', 'name'),
                    ('away', 1, 'off', 'id'),
                    ('away', 1, 'off', 'name')
                ]

                # Generate columns dynamically
                for prefix, pos, shift, output in columns_to_generate:
                    if pos == 1:
                        pos_lab = 'goalie'
                    elif pos == 0:
                        pos_lab = 'skater'
                    col_name = f"{prefix}_{pos_lab}_{shift}_{output}"
                    game_data = game_data.with_columns([
                        pl.struct(["game_id", "period", "period_seconds"]).map_elements(lambda x: apply_player_lists_pl(x, prefix, pos, shift, output)).alias(col_name)
                    ])
                game_start_end = ['GAME_START', 'PERIOD_START', 'GAME_END', 'PERIOD_END']
                game_data =(
                     game_data
                    .sort('game_id', 'period', 'period_seconds', 'event_idx')
                    .filter(~pl.col('event_type').is_in(game_start_end))
                    .with_columns([
                        pl.col('event_idx').max().over(['game_id', 'period', 'period_seconds']).alias('max_event_idx')
                    ])
                    .with_columns([
                        (pl.col('game_id').cast(pl.Utf8) + '-' + pl.col('period').cast(pl.Utf8) + '-' + pl.col('period_seconds').cast(pl.Utf8)).alias('event_seconds_id'),
                        pl.when(pl.col('event_idx') == pl.col('max_event_idx')).then(pl.col('event_type')).otherwise(pl.lit(None)).alias('max_event_type')
                    ])
                    .with_columns([
                        pl.col('event_seconds_id').count().over(['game_id', 'period', 'period_seconds']).alias('count_event_seconds_id')
                    ])
                )

                teams = ['home', 'away']
                positions = ['skater', 'goalie']
                outputvals = ['id', 'name']
                for team in teams:
                    for position in positions:
                        for outputval in outputvals:
                            cur_cols = f"{team}_{position}_current_{outputval}"
                            off_cols = f"{team}_{position}_off_{outputval}"
                            on_cols = f"{team}_{position}_on_{outputval}"
                            label1 = f"{team}_{position}_on_{outputval}"
                            if position == 'goalie':
                                label2 = f"_goalie_{outputval}"
                            else:
                                label2 = f"on_{outputval}"
                            game_data = (
                                game_data
                                .with_columns([
                                    pl.when((pl.col(cur_cols) != "") & (pl.col(on_cols)== "") & (pl.col(off_cols) == "")).then(pl.col(cur_cols))
                                    .when((pl.col(cur_cols) == "") & (pl.col(on_cols)!= "") & (pl.col(off_cols) == "")).then(pl.col(on_cols))
                                    .when((pl.col(cur_cols) == "") & (pl.col(on_cols)== "") & (pl.col(off_cols) != "")).then(pl.col(off_cols))
                                    .when((pl.col(cur_cols) == "") & (pl.col(on_cols)!= "") & (pl.col(off_cols) != "") & (pl.col('event_idx') == pl.col('max_event_idx'))).then(pl.col(on_cols))
                                    .when((pl.col(cur_cols) == "") & (pl.col(on_cols)!= "") & (pl.col(off_cols) != "") & (pl.col('event_idx') != pl.col('max_event_idx'))).then(pl.col(off_cols))
                                    .when((pl.col(cur_cols) != "") & (pl.col(on_cols)!= "") & (pl.col('event_idx') == pl.col('max_event_idx'))).then(pl.concat_str([pl.col(cur_cols),pl.lit(","),pl.col(on_cols)]))
                                    .when((pl.col(cur_cols) != "") & (pl.col(off_cols)!= "") & (pl.col('event_idx') != pl.col('max_event_idx'))).then(pl.concat_str([pl.col(cur_cols),pl.lit(","),pl.col(off_cols)]))
                                    .when((pl.col(cur_cols) != "") & (pl.col(off_cols) != "") & (pl.col('event_idx') == pl.col('max_event_idx'))).then(pl.col(cur_cols))
                                    .otherwise(pl.lit(None))
                                    .alias(label1)
                                ])
                                .with_columns([pl.col(label1).str.split_exact(',', 7)])
                                .unnest(label1)
                                .rename({
                                    "field_0" : f"{team}_1_{label2}",
                                    "field_1" : f"{team}_2_{label2}",
                                    "field_2" : f"{team}_3_{label2}",
                                    "field_3" : f"{team}_4_{label2}",
                                    "field_4" : f"{team}_5_{label2}",
                                    "field_5" : f"{team}_6_{label2}",
                                    "field_6" : f"{team}_7_{label2}",
                                    "field_7" : f"{team}_8_{label2}"
                                })
                            )
                keep_cols = ['game_id', 'period', 'game_seconds', 'period_seconds', 'event_idx',
                             'home_1__goalie_id', 'home_1__goalie_name',
                             'home_1_on_id', 'home_2_on_id', 'home_3_on_id', 'home_4_on_id', 'home_5_on_id', 'home_6_on_id',
                             'home_1_on_name', 'home_2_on_name', 'home_3_on_name', 'home_4_on_name', 'home_5_on_name', 'home_6_on_name',
                             'away_1_on_id', 'away_2_on_id', 'away_3_on_id', 'away_4_on_id', 'away_5_on_id', 'away_6_on_id',
                             'away_1_on_name', 'away_2_on_name', 'away_3_on_name', 'away_4_on_name', 'away_5_on_name', 'away_6_on_name',
                             'away_1__goalie_id', 'away_1__goalie_name']
                game_data = (
                    game_data
                    .select(keep_cols)
                    .rename({
                        'away_1__goalie_id': 'away_goalie',
                        'away_1__goalie_name': 'away_goalie_name',
                        'home_1__goalie_id': 'home_goalie',
                        'home_1__goalie_name': 'home_goalie_name'
                    })
                    .sort('game_id', 'period', 'period_seconds', 'event_idx')
                )

                # Combine DataFrames
                result_df = data.join(game_data, on = ['game_id', 'period', 'game_seconds', 'period_seconds', 'event_idx'], how = "left")

            except Exception as e:
                print('Bad ID:', i, 'Error:', e)
                bad_shift_ids.append(i)
                # Build Empty DF For Append
                result_df = (
                    data
                    .select('game_id', 'period', 'game_seconds', 'period_seconds', 'event_idx')
                )
                columns_with_null = [
                'home_goalie', 'home_goalie_name',
                'home_1_on_id', 'home_2_on_id', 'home_3_on_id', 'home_4_on_id', 'home_5_on_id', 'home_6_on_id',
                'home_1_on_name', 'home_2_on_name', 'home_3_on_name', 'home_4_on_name', 'home_5_on_name', 'home_6_on_name',
                'away_1_on_id', 'away_2_on_id', 'away_3_on_id', 'away_4_on_id', 'away_5_on_id', 'away_6_on_id',
                'away_1_on_name', 'away_2_on_name', 'away_3_on_name', 'away_4_on_name', 'away_5_on_name', 'away_6_on_name',
                'away_goalie', 'away_goalie_name'
                    ]

                # Add null columns to the existing DataFrame
                for column in columns_with_null:
                    result_df = result_df.with_columns(pl.lit(None).alias(column))


            return result_df
        

# Create Load Class
class LoadData:
    """ Used to Load Games and Play By Play For Each Game"""
    def __init__(self, year):
        self.year = year+1
        self.start_year = year

    def load_schedule(self):
        """ Loads a single season schedule. Will loop for each season in load document"""
        start_time = time.time()
        # Load From Parquet
        df = (
        pl.read_parquet(f"{fastR_base}{self.year}.parquet")
        .drop(['season', 'game_date'])
        .with_columns([
            pl.col('game_id').cast(pl.Int32),
            pl.col('season_full').cast(pl.Int32).alias('season'),
            pl.col('away_team_id').cast(pl.Utf8),
            pl.col('home_team_id').cast(pl.Utf8),
            pl.col("game_date_time").dt.convert_time_zone('America/New_York').dt.strftime(time_fmt).alias('start_time_ET'),
            pl.col("game_date_time").dt.convert_time_zone('America/New_York').dt.strftime(day_fmt).alias('game_date'),
            pl.when(pl.col('status_detailed_state') == 'Final').then(pl.lit('OK')).otherwise(pl.col('status_detailed_state')).alias('game_schedule_state')
        ])
        .with_columns([
            pl.concat_str(pl.lit(pbp_link_pre), pl.col('game_id'), pl.lit(pbp_link_suf)).alias('pbp_link'),
            pl.concat_str(pl.lit(shift_link), pl.col('game_id')).alias('shift_link')
        ])
        .rename({'game_type_abbreviation': 'season_type', "game_date_time": 'start_time_utc'})
        .join(home_teams, on='home_team_id', how='left')
        .join(away_teams, on='away_team_id', how='left')
        .select([
            'game_id', 'season', 'game_date', 'start_time_ET', 'season_type', 'game_schedule_state',
            'away_team_id', 'away_score', 'away_abbreviation', 'home_score', 'home_team_id', 'home_abbreviation',
            'pbp_link', 'shift_link', 'start_time_utc'
        ])
        )

        # Add Games If Year Is Current #
        if self.year == 2024:

            # Initalize Load Dates + Empty List
            start_date = df['game_date'].max()
            end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
            game_dfs = []

            # Loop Over Dates For Game Information
            for i in pd.date_range(start=start_date, end=end_date, freq='D'):
                i_str = i.strftime('%Y-%m-%d')
                sched_link = "https://api-web.nhle.com/v1/schedule/"+i_str
                response = requests.get(sched_link).json().get('gameWeek')[0].get('games')
                
                for i, value in enumerate(response):
                    if (value.get('gameType') in [2,3]) & (value.get('gameScheduleState') == 'OK') & (value.get('gameState') == 'OFF'):
                        data = pl.DataFrame({
                            'game_id': value.get('id'),
                            'season': value.get('season'),
                            'game_type_code': value.get('gameType'),
                            'venue_name': value.get('venue').get('default'),
                            'neutral_site': value.get('neutralSite'),
                            'start_time_utc': value.get('startTimeUTC'),
                            'east_offset': value.get('easternUTCOffset'),
                            'local_offset': value.get('venueUTCOffset'),
                            'local_timezone': value.get('venueTimezone'),
                            'game_state': value.get('gameState'),
                            'game_schedule_state': value.get('gameScheduleState'),
                            'away_team_id': value.get('awayTeam').get('id'),
                            'away_abbreviation': value.get('awayTeam').get('abbrev'),
                            'away_team_place': value.get('awayTeam').get('placeName').get('default'),
                            'away_logo': value.get('awayTeam').get('logo'),
                            'away_logo_dark': value.get('awayTeam').get('darkLogo'),
                            'away_score': value.get('awayTeam').get('score'),
                            'home_team_id': value.get('homeTeam').get('id'),
                            'home_abbreviation': value.get('homeTeam').get('abbrev'),
                            'home_team_place': value.get('homeTeam').get('placeName').get('default'),
                            'home_logo': value.get('homeTeam').get('logo'),
                            'home_logo_dark': value.get('homeTeam').get('darkLogo'),
                            'home_score': value.get('homeTeam').get('score'),
                            'period': value.get('periodDescriptor').get('number'),
                            'period_type': value.get('periodDescriptor').get('periodType'),
                            'last_period_type': value.get('gameOutcome').get('lastPeriodType'),
                            'gamecenter_link': value.get('gameCenterLink')
                        })
                    if not data.is_empty():
                        result_df = (
                            data
                            .with_columns([
                                pl.col('game_id').cast(pl.Int32),
                                pl.col('season').cast(pl.Int32),
                                pl.col('away_score').cast(pl.Int32),
                                pl.col('home_score').cast(pl.Int32),
                                pl.when(pl.col('game_type_code') == 2).then(pl.lit('R'))
                                  .when(pl.col('game_type_code') == 3).then(pl.lit('P'))
                                  .alias('season_type'),
                                pl.col("start_time_utc").str.to_datetime("%Y-%m-%dT%H:%M:%SZ").dt.replace_time_zone('UTC'),
                                pl.col('away_team_id').cast(pl.Utf8),
                                pl.col('home_team_id').cast(pl.Utf8)
                            ])
                            .with_columns([
                                pl.col("start_time_utc").dt.convert_time_zone('America/New_York').dt.strftime(time_fmt).alias('start_time_ET'),
                                pl.col("start_time_utc").dt.convert_time_zone('America/New_York').dt.strftime(day_fmt).alias('game_date'),
                                pl.concat_str(pl.lit(pbp_link_pre), pl.col('game_id'), pl.lit(pbp_link_suf)).alias('pbp_link'),
                                pl.concat_str(pl.lit(shift_link), pl.col('game_id')).alias('shift_link')
                            ])
                            .select([
                                'game_id', 'season', 'game_date', 'start_time_ET', 'season_type', 'game_schedule_state',
                                'away_team_id', 'away_score', 'away_abbreviation', 'home_score', 'home_team_id', 'home_abbreviation',
                                'pbp_link', 'shift_link', 'start_time_utc'
                            ])
                            .sort('game_id', 'season', 'start_time_ET')
                        )

                        game_dfs.append(result_df)

                for j in game_dfs:
                    df = df.vstack(j)

        # Sort and Remove Dupes
        df = df.sort('game_id').unique()

        # Save Labels and Metrics
        szn_lab = f"{self.start_year}{self.year}"

        csv_save_url = f"Schedule/csv/NHL_Schedule_{szn_lab}.csv"
        par_save_url = f"Schedule/parquet/NHL_Schedule_{szn_lab}.parquet"

        df.write_csv(csv_save_url)
        df.write_parquet(par_save_url)

        # Metrics
        end_time = time.time()
        elap_time = round((end_time - start_time), 2)
        print(f"{f'{self.start_year}-{self.year}'} NHL Schedule Data Loaded in {elap_time} Seconds | Path: {par_save_url}")

    def load_roster(self):
        """This function will aim to load all rosters from past seasons"""
        # Constants:
        bad_link = [
            'https://api-web.nhle.com/v1/roster/ATL/20112012',
            'https://api-web.nhle.com/v1/roster/ATL/20122013',
            'https://api-web.nhle.com/v1/roster/ATL/20132014',
            'https://api-web.nhle.com/v1/roster/ATL/20142015',
            'https://api-web.nhle.com/v1/roster/ATL/20152016',
            'https://api-web.nhle.com/v1/roster/ATL/20162017',
            'https://api-web.nhle.com/v1/roster/ATL/20172018',
            'https://api-web.nhle.com/v1/roster/ATL/20182019',
            'https://api-web.nhle.com/v1/roster/ATL/20192020',
            'https://api-web.nhle.com/v1/roster/ATL/20202021',
            'https://api-web.nhle.com/v1/roster/ATL/20212022',
            'https://api-web.nhle.com/v1/roster/ATL/20222023',
            'https://api-web.nhle.com/v1/roster/ATL/20232024',
            'https://api-web.nhle.com/v1/roster/ANA/20132014',
            'https://api-web.nhle.com/v1/roster/ANA/20142015',
            'https://api-web.nhle.com/v1/roster/ANA/20152016',
            'https://api-web.nhle.com/v1/roster/ANA/20162017',
            'https://api-web.nhle.com/v1/roster/ANA/20172018',
            'https://api-web.nhle.com/v1/roster/ANA/20182019',
            'https://api-web.nhle.com/v1/roster/ANA/20192020',
            'https://api-web.nhle.com/v1/roster/ANA/20202021',
            'https://api-web.nhle.com/v1/roster/ANA/20212022',
            'https://api-web.nhle.com/v1/roster/ANA/20222023',
            'https://api-web.nhle.com/v1/roster/ANA/20232024',
            'https://api-web.nhle.com/v1/roster/ARI/20092010',
            'https://api-web.nhle.com/v1/roster/ARI/20102011',
            'https://api-web.nhle.com/v1/roster/ARI/20112012',
            'https://api-web.nhle.com/v1/roster/ARI/20122013',
            'https://api-web.nhle.com/v1/roster/ARI/20132014',
            'https://api-web.nhle.com/v1/roster/PHX/20142015',
            'https://api-web.nhle.com/v1/roster/PHX/20152016',
            'https://api-web.nhle.com/v1/roster/PHX/20162017',
            'https://api-web.nhle.com/v1/roster/PHX/20172018',
            'https://api-web.nhle.com/v1/roster/PHX/20182019',
            'https://api-web.nhle.com/v1/roster/PHX/20192020',
            'https://api-web.nhle.com/v1/roster/PHX/20202021',
            'https://api-web.nhle.com/v1/roster/PHX/20212022',
            'https://api-web.nhle.com/v1/roster/PHX/20222023',
            'https://api-web.nhle.com/v1/roster/PHX/20232024',
            'https://api-web.nhle.com/v1/roster/SEA/20092010',
            'https://api-web.nhle.com/v1/roster/SEA/20102011',
            'https://api-web.nhle.com/v1/roster/SEA/20112012',
            'https://api-web.nhle.com/v1/roster/SEA/20122013',
            'https://api-web.nhle.com/v1/roster/SEA/20132014',
            'https://api-web.nhle.com/v1/roster/SEA/20142015',
            'https://api-web.nhle.com/v1/roster/SEA/20152016',
            'https://api-web.nhle.com/v1/roster/SEA/20162017',
            'https://api-web.nhle.com/v1/roster/SEA/20172018',
            'https://api-web.nhle.com/v1/roster/SEA/20182019',
            'https://api-web.nhle.com/v1/roster/SEA/20192020',
            'https://api-web.nhle.com/v1/roster/SEA/20202021',
            'https://api-web.nhle.com/v1/roster/VGK/20092010',
            'https://api-web.nhle.com/v1/roster/VGK/20102011',
            'https://api-web.nhle.com/v1/roster/VGK/20112012',
            'https://api-web.nhle.com/v1/roster/VGK/20122013',
            'https://api-web.nhle.com/v1/roster/VGK/20132014',
            'https://api-web.nhle.com/v1/roster/VGK/20142015',
            'https://api-web.nhle.com/v1/roster/VGK/20152016',
            'https://api-web.nhle.com/v1/roster/VGK/20162017',
            'https://api-web.nhle.com/v1/roster/WPG/20092010',
            'https://api-web.nhle.com/v1/roster/WPG/20102011'
            ]

        start_time = time.time()

        # Team Abbr And Season
        tms_list = list(team_abbr_dict.values())
        szn_lab  = (str(self.start_year)+str(self.year))
        link_list = [f"https://api-web.nhle.com/v1/roster/{team_abbr}/{szn_lab}" for team_abbr in tms_list]

        # Initalize List
        full_df_list = []
        slim_df_list = []

        ## Begin Roster Loading
        for link in link_list:
            response = requests.get(link)
            szn_lab = link[-4:]
            team_lab = link[35:38]
            if response.status_code == 200:
                data = response.json()
                flat_data = {
                    'season': [],
                    'team': [],
                    'position': [],
                    'player_id': [],
                    'headshot': [],
                    'firstName': [],
                    'lastName': [],
                    'positionCode': [],
                    'shootsCatches': [],
                    'sweaterNumber': [],
                    'heightInInches': [],
                    'weightInPounds': [],
                    'heightInCentimeters': [],
                    'weightInKilograms': [],
                    'birthDate': [],
                    'birthCity': [],
                    'birthCountry': [],
                    'birthStateProvince': []
                }

                for position, players in data.items():
                    for player in players:
                        flat_data['season'].append(szn_lab)
                        flat_data['team'].append(team_lab)
                        flat_data['position'].append(position)
                        flat_data['player_id'].append(player['id'])
                        flat_data['headshot'].append(player['headshot'])
                        flat_data['firstName'].append(player['firstName']['default'])
                        flat_data['lastName'].append(player['lastName']['default'])
                        flat_data['positionCode'].append(player['positionCode'])
                        flat_data['shootsCatches'].append(player['shootsCatches'])
                        flat_data['sweaterNumber'].append(player.get('sweaterNumber', 100))
                        flat_data['heightInInches'].append(player['heightInInches'])
                        flat_data['weightInPounds'].append(player['weightInPounds'])
                        flat_data['heightInCentimeters'].append(player['heightInCentimeters'])
                        flat_data['weightInKilograms'].append(player['weightInKilograms'])
                        flat_data['birthDate'].append(player['birthDate'])
                        flat_data['birthCity'].append(player['birthCity']['default'])
                        flat_data['birthCountry'].append(player['birthCountry'])
                        flat_data['birthStateProvince'].append(player.get('birthStateProvince', {}).get('default', ''))
                df = pl.DataFrame(flat_data)
                df = (
                    df
                    .with_columns([
                        pl.col('player_id').cast(pl.Utf8),
                        pl.col('season').cast(pl.Int64),
                        pl.when(pl.col('position') == 'forwards').then(pl.lit(1)).otherwise(pl.lit(0)).alias('pos_F'),
                        pl.when(pl.col('position') == 'defensmen').then(pl.lit(1)).otherwise(pl.lit(0)).alias('pos_D'),
                        pl.when(pl.col('position') == 'goalies').then(pl.lit(1)).otherwise(pl.lit(0)).alias('pos_G'),
                        pl.when(pl.col('shootsCatches') == 'R').then(pl.lit(1)).otherwise(pl.lit(0)).alias('hand_R'),
                        pl.when(pl.col('shootsCatches') == 'L').then(pl.lit(1)).otherwise(pl.lit(0)).alias('hand_L')
                    ])
                    .drop('position')
                )

                # Append Full DF To List For Compilation
                full_df_list.append(df)

                # Create Slim
                slim_df = (
                    df
                    .select([
                        pl.col('player_id').cast(pl.Utf8),
                        pl.col('firstName'),
                        pl.col('lastName'),
                        pl.col('pos_F').cast(pl.Int32),
                        pl.col('pos_D').cast(pl.Int32),
                        pl.col('pos_G').cast(pl.Int32),
                        pl.col('hand_R').cast(pl.Int32),
                        pl.col('hand_L').cast(pl.Int32)])
                    .unique()
                )

                # Append Full DF To List For Compilation
                slim_df_list.append(slim_df)

            elif(link in bad_link):
                pass
            else:
                # If the request was not successful, print the status code and any error message
                print(f"Error Bad Link: {link}")
        
        # Compile DF Lists
        full_df = full_df_list[0]
        for df in full_df_list[1:]:
            full_df = full_df.extend(df)

        slim_df = slim_df_list[0]
        for df in slim_df_list[1:]:
            slim_df = slim_df.extend(df)
        
        
        # Save Full
        full_df.write_csv(f'Rosters/csv/full/NHL_Roster_Full_{f"{self.start_year}{self.year}"}.csv')
        full_df.write_parquet(f'Rosters/parquet/full/NHL_Roster_Full_{f"{self.start_year}{self.year}"}.parquet')

        # Save Slim
        slim_df.write_csv(f'Rosters/csv/slim/NHL_Roster_Slim_{f"{self.start_year}{self.year}"}.csv')
        slim_df.write_parquet(f'Rosters/parquet/slim/NHL_Roster_Slim_{f"{self.start_year}{self.year}"}.parquet')
        
        # Metrics
        end_time = time.time()
        elap_time = round((end_time - start_time), 2)
        print(f"{f'{self.start_year}-{self.year}'} NHL Schedule Data Loaded in {elap_time} Seconds | Path: Rosters/parquet/slim/NHL_Roster_Slim_{self.start_year}{self.year}.parquet")

        return slim_df

    def load_pbp(self, roster_obj):
        season = self.start_year

        # Initialize Variables
        start_time = time.time()
        print(f"Now Loading Play By Play Data From {season}-{season+1} NHL Season")

        bad_ids = []
        shift_len = []
        df_list = []

        # 1) Get Game ID's From Schedule
        game_ids = pl.read_parquet(f"Schedule/parquet/NHL_Schedule_{str(season)+str(season+1)}.parquet").filter((~pl.col('game_id').is_in([2015020497])))['game_id'].unique().to_list()
        print(game_ids[:5])
        n_games = len(game_ids)

        # 2) Loop For Tweaking API Data
        for i in game_ids:
            shift_start = time.time()
            try:
                df_list.append(append_shift_data(align_and_cast_columns(data = ping_nhl_api(i = i), sch = raw_schema), roster_data = roster_obj))
            except ValueError as e:
                bad_ids.append(i)
                print(f"Error In Loading NHL API for GameID: {i} | {e}")
                continue
                
            # Print Intermitent Update
            shift_end = time.time()
            shift_elap = shift_end - shift_start
            shift_len.append(shift_elap)
            average_shift_time = statistics.mean(shift_len)

            if (str(i)[-3:] == "500"):
                print(f"LOAD UPDATE: Game {i} took {round(shift_elap,2)} | Each game is taking ~{round(average_shift_time,2)} Seconds | For {n_games-500} More Games It will Take {round(((average_shift_time*(n_games-500))/60),1)} Minutes")
            if (str(i)[-3:] == "000"):
                print(f"LOAD UPDATE: Game {i} took {round(shift_elap,2)} | Each game is taking ~{round(average_shift_time,2)} Seconds | For {n_games-1000} More Games It will Take {round(((average_shift_time*(n_games-1000))/60),1)} Minutes")

        # 3) Combine DataFrames Into One
        data = df_list[0]
        for df in df_list[1:]:
            try:
                data = data.vstack(df)
            except ValueError as e:
                print(f"Incomplete Data For Game ID: {df['game_id'][0]}")
                print(f"Error: {e}")
                continue
                
        data = data.sort('game_id', 'period', 'event_idx')

        # 4) Save File After VStack
        save_season_path = f"PBP/parquet/API_RAW_PBP_Data_{season}{season+1}.parquet"
        data.write_parquet(
            save_season_path,
            use_pyarrow=True
        )

        # 5) Print Load Metrics
        season_lab = f"{season}-{season+1}"
        end_time = time.time()
        season_elapsed_time = round((end_time - start_time)/60,2)
        bad_games = len(bad_ids)
        games_loaded = len(game_ids) - bad_games
        szn_gpm = ((games_loaded)/(end_time - start_time)*60)
        time_stamp = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Successfully Loaded And Saved {games_loaded} Games From {season_lab} Season in {season_elapsed_time} Minutes ({round(szn_gpm, 2)} GPM) | Path: {save_season_path} | Completed at {time_stamp}")

    def add_missing_roster(self, roster_obj, id_obj):
        # Filter PBP IDs Not in Roster
        missing_ids = id_obj.filter(~pl.col('event_player_1_id').is_in(roster_obj['player_id'])).unique().to_list()

        # Build Player ID Loop To Hit Player Page
        for i in missing_ids:
            link = f'https://api.nhle.com/stats/rest/en/skater/{i}'


        

class Update:
    def __init__(self, year = 2023):
        self.year = year+1
        self.start_year = year
    
    def update_schedule(self):
        start_time = time.time()

        # Load Existing Schedule
        df = pl.read_parquet(f"Schedule/parquet/NHL_Schedule_{self.start_year}{self.year}.parquet")

        # Initalize Update Dates + Empty List
        start_date = df['game_date'].max()
        end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        game_dfs = []
        game_ids_new = []

        # Loop Over Dates For Game Information
        for i in pd.date_range(start=start_date, end=end_date, freq='D'):
            i_str = i.strftime('%Y-%m-%d')
            sched_link = "https://api-web.nhle.com/v1/schedule/"+i_str
            response = requests.get(sched_link).json().get('gameWeek')[0].get('games')

            for i, value in enumerate(response):
                if (value.get('gameType') in [2,3]) & (value.get('gameScheduleState') == 'OK') & (value.get('gameState') == 'OFF'):
                    data = pl.DataFrame({
                        'game_id': value.get('id'),
                        'season': value.get('season'),
                        'game_type_code': value.get('gameType'),
                        'venue_name': value.get('venue').get('default'),
                        'neutral_site': value.get('neutralSite'),
                        'start_time_utc': value.get('startTimeUTC'),
                        'east_offset': value.get('easternUTCOffset'),
                        'local_offset': value.get('venueUTCOffset'),
                        'local_timezone': value.get('venueTimezone'),
                        'game_state': value.get('gameState'),
                        'game_schedule_state': value.get('gameScheduleState'),
                        'away_team_id': value.get('awayTeam').get('id'),
                        'away_abbreviation': value.get('awayTeam').get('abbrev'),
                        'away_team_place': value.get('awayTeam').get('placeName').get('default'),
                        'away_logo': value.get('awayTeam').get('logo'),
                        'away_logo_dark': value.get('awayTeam').get('darkLogo'),
                        'away_score': value.get('awayTeam').get('score'),
                        'home_team_id': value.get('homeTeam').get('id'),
                        'home_abbreviation': value.get('homeTeam').get('abbrev'),
                        'home_team_place': value.get('homeTeam').get('placeName').get('default'),
                        'home_logo': value.get('homeTeam').get('logo'),
                        'home_logo_dark': value.get('homeTeam').get('darkLogo'),
                        'home_score': value.get('homeTeam').get('score'),
                        'period': value.get('periodDescriptor').get('number'),
                        'period_type': value.get('periodDescriptor').get('periodType'),
                        'last_period_type': value.get('gameOutcome').get('lastPeriodType'),
                        'gamecenter_link': value.get('gameCenterLink')
                    })
                if not data.is_empty():
                    result_df = (
                        data
                        .with_columns([
                            pl.col('game_id').cast(pl.Int32),
                            pl.col('season').cast(pl.Int32),
                            pl.col('away_score').cast(pl.Int32),
                            pl.col('home_score').cast(pl.Int32),
                            pl.when(pl.col('game_type_code') == 2).then(pl.lit('R'))
                              .when(pl.col('game_type_code') == 3).then(pl.lit('P'))
                              .alias('season_type'),
                            pl.col("start_time_utc").str.to_datetime("%Y-%m-%dT%H:%M:%SZ").dt.replace_time_zone('UTC'),
                            pl.col('away_team_id').cast(pl.Utf8),
                            pl.col('home_team_id').cast(pl.Utf8)
                        ])
                        .with_columns([
                            pl.col("start_time_utc").dt.convert_time_zone('America/New_York').dt.strftime(time_fmt).alias('start_time_ET'),
                            pl.col("start_time_utc").dt.convert_time_zone('America/New_York').dt.strftime(day_fmt).alias('game_date'),
                            pl.concat_str(pl.lit(pbp_link_pre), pl.col('game_id'), pl.lit(pbp_link_suf)).alias('pbp_link'),
                            pl.concat_str(pl.lit(shift_link), pl.col('game_id')).alias('shift_link')
                        ])
                        .select([
                            'game_id', 'season', 'game_date', 'start_time_ET', 'season_type', 'game_schedule_state',
                            'away_team_id', 'away_score', 'away_abbreviation', 'home_score', 'home_team_id', 'home_abbreviation',
                            'pbp_link', 'shift_link', 'start_time_utc'
                        ])
                        .sort('game_id', 'season', 'start_time_ET')
                    )

                    game_dfs.append(result_df)
                    game_ids_new.append(result_df['game_id'].unique())

        for j in game_dfs:
            df = df.vstack(j)

        # Sort and Remove Dupes
        df = df.sort('game_date').unique()

        # Save Labels and Metrics
        szn_lab = f"{self.start_year}{self.year}"

        csv_save_url = f"Schedule/csv/NHL_Schedule_{szn_lab}.csv"
        par_save_url = f"Schedule/parquet/NHL_Schedule_{szn_lab}.parquet"

        df.write_csv(csv_save_url)
        df.write_parquet(par_save_url)

        # Metrics
        end_time = time.time()
        elap_time = round((end_time - start_time), 2)
        print(f"{f'{self.start_year}-{self.year}'} NHL Schedule Data Loaded in {elap_time} Seconds | Path: {par_save_url}")


    def update_roster(self):
        path = 'Rosters/parquet/all/NHL_Roster_AllSeasons_Slim.parquet'
        existing_df = pl.read_parquet(path)
        new_roster = LoadData(year=self.start_year).load_roster()

        final_df = existing_df.extend(new_roster).unique()

        # Save
        final_df.write_parquet(path)

        return final_df
        
    def update_pbp(self, roster_obj):
        "This function will update the current season PBP with games occuring between the last load and yesterday's date"
        start_time = time.time()
        # Initialize Existing Data Frame + Stats
        df_list = []
        df_list.append(pl.read_parquet(f'PBP/parquet/API_RAW_PBP_Data_{self.start_year}{self.year}.parquet'))
        exist_games = df_list[0]['game_id'].unique()
        exist_rows = df_list[0].height

        # Initialize Load Dates
        last_load = (datetime.strptime(df_list[0]['game_date'].max(), "%Y-%m-%d") + timedelta(days = 1)).strftime('%Y%m%d')

        print(f"Existing DataFrame has {exist_rows} from {len(exist_games)} Games | Last Updated {last_load}")

        season = self.start_year

        # Initialize Variables
        start_time = time.time()
        print(f"Now Loading Play By Play Data From {season}-{season+1} NHL Season")

        bad_ids = []
        shift_len = []

        # 1) Get Game ID's From Schedule
        game_ids = pl.read_parquet(f'Schedule/parquet/NHL_Schedule_{self.start_year}{self.year}.parquet').select('game_id').filter(~pl.col('game_id').is_in(exist_games)).unique()['game_id'].to_list()
        print(game_ids[:5])
        n_games = len(game_ids)

        # 2) Loop For Tweaking API Data
        if n_games >= 1:
            for i in game_ids:
                shift_start = time.time()
                try:
                    df_list.append(append_shift_data(align_and_cast_columns(data = ping_nhl_api(i = i), sch = raw_schema), roster_data=roster_obj))
                except ValueError as e:
                    bad_ids.append(i)
                    print(f"Error In Loading NHL API for GameID: {i} | {e}")
                    continue

                # Print Intermitent Update
                shift_end = time.time()
                shift_elap = shift_end - shift_start
                shift_len.append(shift_elap)
                average_shift_time = statistics.mean(shift_len)

                if (str(i)[-3:] == "500"):
                    print(f"LOAD UPDATE: Game {i} took {round(shift_elap,2)} | Each game is taking ~{round(average_shift_time,2)} Seconds | For {n_games-500} More Games It will Take {round(((average_shift_time*(n_games-500))/60),1)} Minutes")
                if (str(i)[-3:] == "000"):
                    print(f"LOAD UPDATE: Game {i} took {round(shift_elap,2)} | Each game is taking ~{round(average_shift_time,2)} Seconds | For {n_games-1000} More Games It will Take {round(((average_shift_time*(n_games-1000))/60),1)} Minutes")

            # 3) Combine DataFrames Into One
            data = df_list[0]
            for df in df_list[1:]:
                try:
                    data = data.vstack(df)
                except ValueError as e:
                    print(f"Incomplete Data For Game ID: {df['game_id'][0]}")
                    print(f"Error: {e}")
                    continue

            data = data.sort('game_id', 'period', 'event_idx')

            # 4) Save File After VStack
            save_season_path = f"PBP/parquet/API_RAW_PBP_Data_{season}{season+1}.parquet"
            data.write_parquet(
                save_season_path,
                use_pyarrow=True
            )

            # 5) Print Load Metrics
            season_lab = f"{season}-{season+1}"
            end_time = time.time()
            season_elapsed_time = round((end_time - start_time)/60,2)
            bad_games = len(bad_ids)
            games_loaded = len(df_list[1:]) - bad_games
            szn_gpm = ((games_loaded)/(end_time - start_time)*60)
            time_stamp = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')

            print(f"Successfully Loaded And Saved {games_loaded} Games From {season_lab} Season in {season_elapsed_time} Minutes ({round(szn_gpm, 2)} GPM) | Path: {save_season_path} | Completed at {time_stamp}")

        else:
            print("No Games To Update")