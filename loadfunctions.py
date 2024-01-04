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

# 1 Load Historical Schedule From Fast R (From 2010 Season To 2023-06-07)

# 2 Load Schedule From Current API (All Data)

# 3 Load Today's Schedule From Current API


