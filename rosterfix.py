from requirements import *

def get_pids(yr):
    pbp_link = f"PBP/parquet/API_RAW_PBP_Data_{yr}{yr+1}.parquet"
    roster_link = 'Rosters/parquet/all/NHL_Roster_AllSeasons_Full.parquet'

    # Load Slim PBP
    pbp_skater = pl.read_parquet(pbp_link).select('event_player_1_id').rename({"event_player_1_id": "player_id"}).unique()
    pbp_goalie = pl.read_parquet(pbp_link).select('event_goalie_id').rename({"event_goalie_id": "player_id"}).unique()
    pbp = pbp_skater.extend(pbp_goalie).unique()
    roster = pl.read_parquet(roster_link).select('player_id').unique()

    non_matching_values = pbp.join(roster, on=['player_id'], how='anti')['player_id'].unique().to_list()
    print(len(non_matching_values))
    # Display the result
    return non_matching_values

player_ids = get_pids(2023)

full_df_list = []
slim_df_list = []
for i in player_ids:
    link = f'https://api-web.nhle.com/v1/player/{i}/landing'
    response = requests.get(link)
    
    if response.status_code == 200:
        data = response.json()
        flat_data = {
            'season':[],
            'team': [],
            'player_id': [],
            'headshot': [],
            'firstName': [],
            'lastName': [],
            'position': [],
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
    
        flat_data['season'].append(2024)
        flat_data['team'].append(data['currentTeamAbbrev'])
        flat_data['player_id'].append(data['playerId'])
        flat_data['headshot'].append(data['headshot'])
        flat_data['firstName'].append(data['firstName']['default'])
        flat_data['lastName'].append(data['lastName']['default'])
        flat_data['position'].append(data['position'])
        flat_data['positionCode'].append(data['position'])
        flat_data['shootsCatches'].append(data['shootsCatches'])
        flat_data['sweaterNumber'].append(data.get('sweaterNumber', 100))
        flat_data['heightInInches'].append(data['heightInInches'])
        flat_data['weightInPounds'].append(data['weightInPounds'])
        flat_data['heightInCentimeters'].append(data['heightInCentimeters'])
        flat_data['weightInKilograms'].append(data['weightInKilograms'])
        flat_data['birthDate'].append(data['birthDate'])
        flat_data['birthCity'].append(data['birthCity']['default'])
        flat_data['birthCountry'].append(data['birthCountry'])
        flat_data['birthStateProvince'].append(data.get('birthStateProvince', {}).get('default', ''))

        df = pl.DataFrame(flat_data)
        df = (
            df
            .with_columns([
                pl.col('player_id').cast(pl.Utf8),
                pl.col('season').cast(pl.Int64),
                pl.when(pl.col('position').is_in(['R', 'C', 'L'])).then(pl.lit(1)).otherwise(pl.lit(0)).alias('pos_F'),
                pl.when(pl.col('position') == 'D').then(pl.lit(1)).otherwise(pl.lit(0)).alias('pos_D'),
                pl.when(pl.col('position') == 'G').then(pl.lit(1)).otherwise(pl.lit(0)).alias('pos_G'),
                pl.when(pl.col('shootsCatches') == 'R').then(pl.lit(1)).otherwise(pl.lit(0)).alias('hand_R'),
                pl.when(pl.col('shootsCatches') == 'L').then(pl.lit(1)).otherwise(pl.lit(0)).alias('hand_L')
            ])
            .drop(['position'])
        )
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
full_df.write_csv(f'Rosters/csv/full/ADD_NHL_Roster_Full.csv')
full_df.write_parquet(f'Rosters/parquet/full/ADD_NHL_Roster_Full.parquet')
# Save Slim
slim_df.write_csv(f'Rosters/csv/slim/ADD_NHL_Roster_Slim.csv')
slim_df.write_parquet(f'Rosters/parquet/slim/ADD_NHL_Roster_Slim.parquet')

print(slim_df)
# Collapse Rosters To Create Large Roster File
def compile_rosters(type):
    print(f"Now Compiling {type} Rosters From All NHL Seasons")
    season_roster_path = f"Rosters/parquet/all/NHL_Roster_AllSeasons_{type}.parquet"
    df = pl.read_parquet(season_roster_path)

    new = pl.read_parquet(f"Rosters/parquet/{type.lower()}/ADD_NHL_Roster_{type}.parquet")
    df = df.extend(new).unique()
    
    # Save
    save_path = f"Rosters/parquet/all/NHL_Roster_AllSeasons_{type}.parquet"
    df.write_parquet(save_path)
    print(f"All NHL Rosters Loaded | Path: {save_path}")

print(" ")
print("="*34, "Begin Compiling Roster Files", "="*34)
for j in ['Slim', 'Full']:
    compile_rosters(j)