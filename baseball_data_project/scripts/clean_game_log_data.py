import pandas as pd
from baseball_data_project.scripts.utils import extract_team_acronym_and_division, ensure_directory_exists
from loguru import logger
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import toml
pd.options.mode.chained_assignment = None  # default='warn'


# Specify the path to your config file
config_file_path = '/Users/colinclapham/github/baseball-data-project/config.toml'

# Load the TOML file
config_data = toml.load(config_file_path)

'''
The Purpose of this script is to clean GAME LOG DATA

The following programs exist to create datasets which will eventually be SQL tables:

1. Create Game Info:
Dataset of every Team's home games
    - ID (Primary Key): String concatenation of Team Acronym + YYYYMMDD0
    - Game Number: Sequential order of a team's home games
    - Visiting Team
    - Home Team
    - Date: date when game was started
    - DayNight: 'day' if day game, 'night' if night game
    - Winning Pitcher UUID: UUID of the pitcher who earned the Win
    - Losing Pitcher UUID: UUID of the pitcher who earned the Loss
    - Save UUID: UUID of pitcher who earned the Save (Null if ineligible) 
    
2. Create Lineup Info:
Dataset of every Game's starting lineups
    - ID (Primary Key): String concatenation of Team Acronym + YYYYMMDD0
    - Game Number: Sequential order of a team's home games
    - Player UUID: UUID of the player in the starting lineup
    - is_home_team: 0 if visitor, 1 if home team
    - Batting Position: (1-9), NULL if pitcher
    - Fielding Position: starting position in the field
    
3. Create Pitch Info
Dataset of every pitch thrown
    - ID (Primary Key): String concatenation of Team Acronym + YYYYMMDD0
    - Game Number: Sequential order of a team's home games
    - Pitcher UUID: UUID of the active pitcher in the at-bat
    - Pitcher Team: Team Acronym of the active pitcher
    - Batter UUID: UUID of the active batter at-bat
    - Batter Team: Team Acronym of the active batter
    - Inning: Inning the at-bat is taking place in
    - Pitch Event: outcome of the pitch thrown, can be one of:
        - Swinging Strike
        - Called Strike
        - Ball
        - Foul
        - Foul Tip
        - Automatic Strike
        - Automatic Ball
        - Contact, in play
        - Foul Bunt
        - Missed Bunt
        - Hit by Pitch
    - At-Bat Pitch Count: Running Total of pitches thrown in the at bat
    - Total Pitcher Pitch Count: Running total of pitches thrown by active pitcher
    - is_whiff: True/False of if pitch event was Swinging Strike
    - is_called_strike: True/False of if pitch event was Called Strike
    - is_contact: True/False of if pitch event was Contact or Foul (Tip, Bunt, or otherwise)
'''


def pitcher_or_hitter(value):
    # In the game log dataset, pitchers are included in the starting lineup but rarely are in the batting order
    if value == 0:
        # If a pitcher is not hitting then they are flagged as 0 Batting Order in the raw data
        return None
    else:
        return value


def fielding_mapping(value):
    # Fielding mapping matches traditional baseball box scoring
    if value == '1':
        return 'P'
    elif value == '2':
        return 'C'
    elif value == '3':
        return '1B'
    elif value == '4':
        return '2B'
    elif value == '5':
        return '3B'
    elif value == '6':
        return 'SS'
    elif value == '7':
        return 'LF'
    elif value == '8':
        return 'CF'
    elif value == '9':
        return 'RF'
    elif value == '10':
        return 'DH'
    else:
        return None


def pitch_mapping(value):
    # 11 different scenarios that a pitch can lead to
    if value == 'B':
        return 'Ball'
    elif value == 'C':
        return 'Called Strike'
    elif value == 'F':
        return 'Foul'
    elif value == 'S':
        return 'Swinging Strike'
    elif value == 'T':
        return 'Foul Tip'
    elif value == 'A':
        return 'Automatic Strike'
    elif value == 'V':
        return 'Automatic Ball'
    elif value == 'X':
        return 'Contact, in play'
    elif value == 'L':
        return 'Foul Bunt'
    elif value == 'M':
        return 'Missed Bunt'
    elif value == 'H':
        return 'Hit by Pitch'
    else:
        return value


def create_game_info(df):
    # High level win/loss information
    d = []

    for k in range(1, df['game_number'].max() + 1):  # Loop through each game of a team's home schedule
        d.append(
            {
                'ID': df[
                    (df['game_number'] == k) &
                    (df['data_type'] == 'id')  # each file should have 81 ID rows for a full season
                    ]['metadata_1'].values[0],
                'Game Number': k,
                'Visiting Team': df[
                    (df['game_number'] == k) &
                    (df['data_type'] == 'info') &
                    (df['metadata_1'] == 'visteam')
                    ]['metadata_2'].values[0],
                'Home Team': df[
                    (df['game_number'] == k) &
                    (df['data_type'] == 'info') &
                    (df['metadata_1'] == 'hometeam')
                    ]['metadata_2'].values[0],
                'Date': df[
                    (df['game_number'] == k) &
                    (df['data_type'] == 'info') &
                    (df['metadata_1'] == 'date')
                    ]['metadata_2'].values[0],
                'DayNight': df[
                    (df['game_number'] == k) &
                    (df['data_type'] == 'info') &
                    (df['metadata_1'] == 'daynight')  # unclear what the time cutoff is for day vs. night
                    ]['metadata_2'].values[0],
                'Winning Pitcher UUID': df[
                    (df['game_number'] == k) &
                    (df['data_type'] == 'info') &
                    (df['metadata_1'] == 'wp')
                    ]['metadata_2'].values[0],
                'Losing Pitcher UUID': df[
                    (df['game_number'] == k) &
                    (df['data_type'] == 'info') &
                    (df['metadata_1'] == 'lp')
                    ]['metadata_2'].values[0],
                'Save UUID': df[
                    (df['game_number'] == k) &
                    (df['data_type'] == 'info') &
                    (df['metadata_1'] == 'save')
                    ]['metadata_2'].values[0],  # Can be none
            }
        )
    return pd.DataFrame(d)


def create_lineup_info(df):
    # Starting lineup infor for every game
    f = []

    for k in range(1, df['game_number'].max() + 1):  # Loop through every game
        for p in (df[(df['game_number'] == k) &
                     (df['data_type'] == 'start')]['metadata_3'].unique()):  # Home/Away Team
            for m in (df[(df['game_number'] == k) &
                         (df['data_type'] == 'start') &
                         (df['metadata_3'] == p)]['metadata_4'].unique()):  # Batting Order
                f.append(
                    {
                        'ID': df[
                            (df['game_number'] == k) &
                            (df['data_type'] == 'id')
                            ]['metadata_1'].values[0],
                        'Game Number': k,
                        'Player UUID': df[
                            (df['game_number'] == k) &
                            (df['data_type'] == 'start') &
                            (df['metadata_3'] == p) &
                            (df['metadata_4'] == m)
                            ]['metadata_1'].values[0],
                        'is_home_team': p,
                        'Batting Position': pitcher_or_hitter(
                            m
                        ),
                        'Fielding Position': fielding_mapping(
                            df[
                                (df['game_number'] == k) &
                                (df['data_type'] == 'start') &
                                (df['metadata_3'] == p) &
                                (df['metadata_4'] == m)
                                ]['metadata_5'].values[0]
                        )
                    }
                )
    return pd.DataFrame(f)


def create_pitch_info(df):
    # Pitch level data
    # metadata_6 contains play level data, some of which we want to filter out (for now)
    # TODO: extract events from metadata_6
    # These events cause a duplication of rows
    events_to_filter_out = ['NP', 'WP', 'SB', 'PB', 'PO', 'BK', 'CS', 'OA', 'DI', 'FLE']
    pattern = '|'.join(events_to_filter_out)
    cleaned_df = df[~df['metadata_6'].str.contains(pattern, case=False, na=False)]

    cleaned_df['Cleaned Pitch Sequence'] = (
        cleaned_df['metadata_5'].replace('[^a-zA-Z]', '', regex=True).astype(str).apply(list)
    )  # metadata_5 needs to be cleaned to remove special character and numbers

    g = []

    for k in range(1, cleaned_df['game_number'].max() + 1):  # Loop through each game

        # save the game_id
        game_id = (cleaned_df[
                (cleaned_df['game_number'] == k) &
                (cleaned_df['data_type'] == 'id')
                ]['metadata_1'].values[0])
        # save the visiting team
        vis_team = (cleaned_df[
                    (cleaned_df['game_number'] == k) &
                    (cleaned_df['data_type'] == 'info') &
                    (cleaned_df['metadata_1'] == 'visteam')
                    ]['metadata_2'].values[0])
        # save the home team
        home_team = (cleaned_df[
            (cleaned_df['game_number'] == k) &
            (cleaned_df['data_type'] == 'info') &
            (cleaned_df['metadata_1'] == 'hometeam')
            ]['metadata_2'].values[0])

        def at_bat_mapper(value):
            # metadata_2 contains information on the at-bat team
            # this function allows us to map the batter team to the situation
            if value == '0':
                return vis_team
            elif value == '1':
                return home_team
            else:
                return None

        def pitching_mapper(value):
            # metadata_2 contains information on the at-bat team
            # this function allows us to map the pitcher team to the situation
            if value == '0':
                return home_team
            elif value == '1':
                return vis_team
            else:
                return None

        def pitcher_mapper(value):
            # metadata_2 contains information on the at-bat team
            # this function allows us to map the pitcher player to the situation
            if value == '0':
                return home_pitcher
            elif value == '1':
                return visiting_pitcher
            else:
                return value

        def total_pitch_count_mapper(value):
            # metadata_2 contains information on the at-bat team
            # this function allows us to keep a running total of pitches thrown
            if value == '0':
                return home_pitcher_pitch_count
            elif value == '1':
                return visiting_pitcher_pitch_count
            else:
                return value
        # Save the starting pitcher of the home team and their pitch count
        home_pitcher = (cleaned_df[
            (cleaned_df['game_number'] == k) &
            (cleaned_df['data_type'] == 'start') &
            (cleaned_df['metadata_3'] == '1') &
            (cleaned_df['metadata_5'] == '1')
            ]['metadata_1'].values[0])
        home_pitcher_pitch_count = 1
        # Save the starting pitcher of the visiting team and their pitch count
        visiting_pitcher = (cleaned_df[
            (cleaned_df['game_number'] == k) &
            (cleaned_df['data_type'] == 'start') &
            (cleaned_df['metadata_3'] == '0') &
            (cleaned_df['metadata_5'] == '1')
            ]['metadata_1'].values[0])
        visiting_pitcher_pitch_count = 1
        # explode the game info so each pitch has its own row
        # only keep 'play' and 'sub' data where the sub is a pitcher
        exploded = (
            cleaned_df[
                (
                        (cleaned_df['game_number'] == k) &
                        (cleaned_df['data_type'] == 'play')
                )
                | (
                        (cleaned_df['game_number'] == k) &
                        (cleaned_df['data_type'] == 'sub') &
                        (cleaned_df['metadata_5'] == '1')
                )]
            [[
                'data_type',
                'metadata_1',
                'metadata_2',
                'metadata_3',
                'game_number',
                'Cleaned Pitch Sequence'
            ]].explode('Cleaned Pitch Sequence').reset_index()
        )
        # save a variable for hitter for us in keeping track of at-bat pitches, set pitch count to 0 to start
        hitter = None
        at_bat_pitch_count = 0

        for index, row in exploded.iterrows():  # loop through each row of exploded dataset
            if (row['data_type'] == 'sub') & (row['metadata_3'] == '1'):
                home_pitcher = row['metadata_1']  # if there is a sub, change the home pitcher
                home_pitcher_pitch_count = 1  # if there is a sub, reset the pitch count
                pass
            elif (row['data_type'] == 'sub') & (row['metadata_3'] == '0'):
                visiting_pitcher = row['metadata_1']  # if there is a sub, change the visiting pitcher
                visiting_pitcher_pitch_count = 1  # if there is a sub, reset the pitch count
                pass
            else:

                if row['metadata_3'] != hitter:  # check if the current hitter is not the same as the previous hitter
                    hitter = row['metadata_3']  # if true, reassign the hitter
                    at_bat_pitch_count = 1  # reset the pitch count of the at-bat
                else:
                    at_bat_pitch_count += 1  # if the hitter is the same, add 1 to the at-bat pitch count

                g.append(
                    {
                        'ID': game_id,
                        'Game Number': k,
                        'Pitcher UUID': pitcher_mapper(row['metadata_2']),
                        'Pitcher Team': pitching_mapper(row['metadata_2']),
                        'Batter UUID': row['metadata_3'],
                        'Batter Team': at_bat_mapper(row['metadata_2']),
                        'Inning': row['metadata_1'],
                        'Pitch Event': pitch_mapping(row['Cleaned Pitch Sequence']),
                        'At-Bat Pitch Count': at_bat_pitch_count,
                        'Total Pitcher Pitch Count': total_pitch_count_mapper(row['metadata_2']),
                        'is_whiff': row['Cleaned Pitch Sequence'] == 'S',
                        'is_called_strike': row['Cleaned Pitch Sequence'] == 'C',
                        'is_contact': any(row['Cleaned Pitch Sequence'] == s for s in ['X', 'F', 'T', 'L'])
                    }
                )
                if row['metadata_2'] == '0':
                    home_pitcher_pitch_count += 1
                elif row['metadata_2'] == '1':
                    visiting_pitcher_pitch_count += 1
    return pd.DataFrame(g)


def extract_game_log_data(year, team_acronym):
    my_file = Path(f'{config_data["input_file_path"]}/game_log_data/{year}/{team_acronym}{year}_game_log_data.csv')
    if my_file.is_file():
        logger.info(f'{year} Team Data Exists!')
        game_log_raw_data = pd.read_csv(f'{config_data["input_file_path"]}/game_log_data/{year}/{team_acronym}{year}_game_log_data.csv')
    else:
        logger.info(f'{team_acronym}{year} Data does not exist - run extract_game_log_data.py')

    return game_log_raw_data


def run_clean_game_log_data(game_log_years=[2023], is_read_team_data=True, is_create_game_info=True, is_create_lineup_info=True, is_create_pitch_info=True):
    if is_read_team_data:
        for i in game_log_years:
            team_acronyms = extract_team_acronym_and_division(i)
            for j in team_acronyms:

                logger.info(f'Cleaning {j[0]}{i} Game Log Data')

                if is_create_game_info:
                    csv_file = f'{config_data["output_file_path"]}/game_info/{i}/{j[0]}{i}_game_info_data.csv'

                    ensure_directory_exists(csv_file)

                    table = create_game_info(extract_game_log_data(i, j[0]))
                    # Write the Table to a csv
                    table.to_csv(csv_file)
                    logger.info(f"Game Info has been written to '{csv_file}'")
                else:
                    logger.info('Do Not Create Game Info')

                if is_create_lineup_info:
                    csv_file = f'{config_data["output_file_path"]}/lineup_info/{i}/{j[0]}{i}_lineup_info_data.csv'

                    ensure_directory_exists(csv_file)

                    table = create_lineup_info(extract_game_log_data(i, j[0]))

                    # Write the Table to a csv
                    table.to_csv(csv_file)
                    logger.info(f"Lineup Info has been written to '{csv_file}'")
                else:
                    logger.info('Do Not Create Lineup Info')

                if is_create_pitch_info:
                    # Define the path for the Parquet file
                    parquet_file = f'{config_data["output_file_path"]}/pitch_info/{i}/{j[0]}{i}_pitch_info_data.parquet'

                    ensure_directory_exists(parquet_file)
                    # Convert the pandas DataFrame to a pyarrow Table
                    table = pa.Table.from_pandas(create_pitch_info(extract_game_log_data(i, j[0])))

                    # Write the Table to a Parquet file
                    pq.write_table(table, parquet_file)
                    logger.info(f"Data has been written to '{parquet_file}' in Parquet format.")
                else:
                    logger.info('Do Not Create Pitch Info')
    else:
        logger.info('Skip Game Log Data')
        pass


if __name__ == "__main__":

    # game_log_years = [
    #     2022,
    #     # 2024 ### not yet available
    # ]
    #
    # is_read_team_data = True
    # is_create_game_info = True
    # is_create_lineup_info = True
    # is_create_pitch_info = True

    run_clean_game_log_data()
