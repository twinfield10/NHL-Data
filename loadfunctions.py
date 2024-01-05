# Load Requirements
from requirements import *

### Schedule Load Functions ###

## Constants ##
season_list = [2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,
               2021,2022,2023,2024]

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
    8 : 'MON',           # Montréal Canadiens    
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

# Time Zone Conversions
est = pytz.timezone('US/Eastern')
utc = pytz.utc
fmt = "%Y-%m-%dT%H:%M:%SZ"
day_fmt = "%Y-%m-%d"
time_fmt = "%H:%M"

# Link Base
pbp_link_pre = 'https://api-web.nhle.com/v1/gamecenter/'
pbp_link_suf = '/play-by-play'
shift_link = 'https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId='

# Dates For Scrape:
max_date = pl.read_parquet("Schedule/parquet/NHL_Schedule_20232024.parquet")['game_date'].max()
yday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')


# 1) Load Historical Schedule From Fast R (From 2010 Season To 2023-06-07)
def load_historical_schedule():
    for i in season_list:
        # Load
        url = "https://raw.githubusercontent.com/sportsdataverse/fastRhockey-data/main/nhl/schedules/parquet/nhl_schedule_"+str(i)+".parquet"
        df = pl.read_parquet(url)
        df = (
        df
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
        szn_start = i-1
        szn_lab = str(szn_start)+str(i)
        csv_save_url = f"Schedule/csv/NHL_Schedule_{szn_lab}.csv"
        par_save_url = f"Schedule/parquet/NHL_Schedule_{szn_lab}.parquet"
        df.write_csv(csv_save_url)
        df.write_parquet(par_save_url)
        
        print(f"{szn_start}-{i} NHL Season Schedule Saved | Path: {csv_save_url}")
#load_historical_schedule()

# 2) Load Schedule From Current API (All Data)
def load_current_schedule(start = max_date, end = yday):
    """This function will take a start date and end date and load any NHL Game IDs From The NHL Schedule between those dates

        INPUTS:
        start and end are dates stored in Y%-m%-d% format
    """

    # 1) Initialize Variables (Start Date, End Date, List of IDs, Existing DF)
    start_date = start
    end_date = end
    print(f"Now Loading Games From {start_date} to {end_date}")
    
    # DF List
    game_dfs = []

    #Load/Save
    load_link = 'Schedule/parquet/NHL_Schedule_20232024'
    exist_df = pl.read_parquet(f'{load_link}.parquet')

    # 2) Loop Over Date Range To Get API Response For Schedule
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

    # Build + Manipulate Final DataFrame
    result_df = exist_df
    for df in game_dfs:
        result_df = result_df.vstack(df)
    
    result_df = result_df.unique()

    # Save
    result_df.write_parquet('Schedule/parquet/NHL_Schedule_20232024.parquet')
    result_df.write_csv('Schedule/csv/NHL_Schedule_20232024.csv')

# 3) Load Today's Schedule From Current API
def load_todays_schedule(dte = datetime.today().strftime("%Y-%m-%d")):
    sched_link = "https://api-web.nhle.com/v1/schedule/"+dte
    response = requests.get(sched_link).json().get('gameWeek')[0].get('games')

    # DF List
    game_dfs = []


    for i, value in enumerate(response):
        if (value.get('gameType') in [2,3]) & (value.get('gameState') == 'FUT'):
            data = pl.DataFrame({
                    'game_id': value.get('id'),
                    'season': value.get('season'),
                    'game_type_code': value.get('gameType'),
                    'venue_name': value.get('venue').get('default'),
                    'neutral_site': value.get('neutralSite'),
                    'start_time_utc': value.get('startTimeUTC'),
                    'game_state': value.get('gameState'),
                    'game_schedule_state': value.get('gameScheduleState'),
                    'away_team_id': value.get('awayTeam').get('id'),
                    'away_abbreviation': value.get('awayTeam').get('abbrev'),
                    'away_logo': value.get('awayTeam').get('logo'),
                    'away_logo_dark': value.get('awayTeam').get('darkLogo'),
                    'away_odds': next(item for item in value.get('awayTeam').get('odds') if item['providerId'] == 9)['value'],
                    'home_team_id': value.get('homeTeam').get('id'),
                    'home_abbreviation': value.get('homeTeam').get('abbrev'),
                    'home_logo': value.get('homeTeam').get('logo'),
                    'home_logo_dark': value.get('homeTeam').get('darkLogo'),
                    'home_odds': next(item for item in value.get('homeTeam').get('odds') if item['providerId'] == 9)['value'],
                    'gamecenter_link': value.get('gameCenterLink')
                })
        if not data.is_empty():
            result_df = (
                data
                .with_columns([
                    pl.col('game_id').cast(pl.Int32),
                    pl.col('season').cast(pl.Int32),
                    pl.col('away_odds').cast(pl.Float64),
                    pl.col('home_odds').cast(pl.Float64),
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
                    'venue_name', 'neutral_site', 
                    'away_team_id', 'away_odds', 'away_abbreviation', 'home_odds', 'home_team_id', 'home_abbreviation',
                    'away_logo', 'away_logo_dark', 'home_logo', 'home_logo_dark',
                    'pbp_link', 'shift_link', 'start_time_utc'
                ])
            )

            game_dfs.append(result_df)

    # Build + Manipulate Final DataFrame
    if len(game_dfs) > 1:
        result_df = game_dfs[0]
        for df in game_dfs[1:]:
            result_df = result_df.vstack(df)
    elif len(game_dfs) == 1:
        result_df == game_dfs[0]
    
    result_df = result_df.unique().sort('game_id', 'start_time_ET')

    return result_df

### ROSTERS ###
#def load_rosters(path = 'Data/NHL_Rosters_2014_2024'):
    """Function To load Rosters. If Roster Data Exists, then the table will simply be updatad, rather than re-created every time"""

    

    # Define Historical Load:

### ROSTERS ###
def historical_roster_load():
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
        'https://api-web.nhle.com/v1/roster/VGK/20102011',
        'https://api-web.nhle.com/v1/roster/VGK/20112012',
        'https://api-web.nhle.com/v1/roster/VGK/20122013',
        'https://api-web.nhle.com/v1/roster/VGK/20132014',
        'https://api-web.nhle.com/v1/roster/VGK/20142015',
        'https://api-web.nhle.com/v1/roster/VGK/20152016',
        'https://api-web.nhle.com/v1/roster/VGK/20162017',
        'https://api-web.nhle.com/v1/roster/WPG/20102011'
        ]
    
    print("Now Loading Historical Rosters (2010 - 2024)")
    start_time = time.time()
    # Constant Team Abbr And Season
    tms_list = [['ATL','ANA', 'ARI', 'BOS', 'BUF', 'CAR', 'CBJ', 'CGY', 'CHI', 'COL', 'DAL',
                'DET', 'EDM', 'FLA', 'LAK','MIN', 'MTL', 'NJD', 'NSH', 'NYI', 'NYR',
                'OTT', 'PHI', 'PHX', 'PIT', 'SEA', 'SJS', 'STL', 'TBL', 'TOR', 'VAN', 'VGK', 'WPG', 'WSH']]

    szn_list = ['20102011', '20112012', '20122013','20132014', '20142015', '20152016', '20162017', '20172018', '20182019', '20192020', '20202021', '20212022', '20222023', '20232024']

    # Generate all combinations of teams and seasons
    combinations = list(product(tms_list[0], szn_list))

    # Create a DataFrame
    df = pd.DataFrame(combinations, columns=['teams', 'year'])
    df = df.explode('teams').drop_duplicates()
    df['link'] = "https://api-web.nhle.com/v1/roster/"+df['teams']+'/'+df['year']
    df.dropna(inplace=True)

    ## Begin Roster Loading
    rosters = []
    for link in df['link']:
        response = requests.get(link)
        szn_lab = link[-4:]
        team_lab = link[35:38]
        if response.status_code == 200:
            # Parse the JSON content of the response
            data = response.json()
                        # Flatten the JSON object to make it suitable for DataFrame
            flat_data = {
                'season': [],
                'team': [],
                'position': [],
                'id': [],
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
                    flat_data['id'].append(player['id'])
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
                    pl.col('id').cast(pl.Int32),
                    pl.col('season').cast(pl.Int64),
                    pl.when(pl.col('position') == 'forwards').then(pl.lit(1)).otherwise(pl.lit(0)).alias('pos_F'),
                    pl.when(pl.col('position') == 'defensmen').then(pl.lit(1)).otherwise(pl.lit(0)).alias('pos_D'),
                    pl.when(pl.col('position') == 'goalies').then(pl.lit(1)).otherwise(pl.lit(0)).alias('pos_G'),
                    pl.when(pl.col('shootsCatches') == 'R').then(pl.lit(1)).otherwise(pl.lit(0)).alias('hand_R'),
                    pl.when(pl.col('shootsCatches') == 'L').then(pl.lit(1)).otherwise(pl.lit(0)).alias('hand_L')
                ])
            )
            rosters.append(df)
        elif(link in bad_link):
            pass
        else:
            # If the request was not successful, print the status code and any error message
            print(f"Error Bad Link: {link}")
    # Build Large DF
    final_df = rosters[0]
    for df in rosters[1:]:
        final_df = final_df.vstack(df)   
    final_df = (
        final_df
        .sort('season', descending=True)
        .drop('position')
    )

    end_time = time.time()
    elap_time = round((end_time - start_time), 2)

    print(f"Data Loaded in {elap_time} Seconds")
    print(f"Total Rows in Rosters Data Frame: {final_df.height}")
    print(f"Total PlayerIDs Rosters Data Frame: {len(final_df['id'].unique())}")

    ## Save Full
    final_df.write_csv('Rosters/csv/NHL_Full_Roster_2010_2024.csv')
    final_df.write_parquet('Rosters/parquet/NHL_Full_Roster_2010_2024.parquet')

    ## Create Slim
    slim_df = (
        final_df
        .select([
            pl.col('id').cast(pl.Utf8),
            pl.col('firstName'),
            pl.col('lastName'),
            pl.col('pos_F').cast(pl.Int32),
            pl.col('pos_D').cast(pl.Int32),
            pl.col('pos_G').cast(pl.Int32),
            pl.col('hand_R').cast(pl.Int32),
            pl.col('hand_L').cast(pl.Int32)])
        .unique()
    )

    # Save Slim
    slim_df.write_csv('Rosters/csv/NHL_Slim_Roster_2010_2024.csv')
    slim_df.write_parquet('Rosters/parquet/NHL_Slim_Roster_2010_2024.parquet')


    print(" ")
    print(f"Total Rows in Slim Rosters Data Frame: {slim_df.height}")
    print(f"Total PlayerIDs Slim Rosters Data Frame: {len(slim_df['id'].unique())}")

    return final_df
print(historical_roster_load().head())
    


