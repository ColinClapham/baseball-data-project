import requests
import zipfile
import io
import pandas as pd
from utils import delete_file, extract_team_acronym_and_division
from loguru import logger

'''
The Purpose of this script is to extract GAME LOG DATA

The data includes a multitude of fields which need to be processed individually
The are broken up into subgroups:
    - id: string which includes Team Acronym + YYYYDDMM0
    - version: all values set to '2' - likely refers to upload version for auditing purposes
    - info: overarching game info which includes things like Home Team, Away Team, Start Time, etc.
    - start: players starting the game, includes: 
        - uuid, 
        - Full Player Name, 
        - 0 (Away Team) or 1 (Home Team), 
        - Order in Batting Lineup (1-9, 0 for pitchers), 
        - Place in Field (10 for DH)
    - play: Individual play level data
    - sub: any offensive, defensive, or pitching substitutions
    - data: pitching data for the game, includes data indicator ('er' = Earned Runs), uuid, and Runs Earned (integer) 
    - badj: 'batter adjustment', indicates if batter is batting from side that is not expected; 
        - only one record is tagged, will ignore
    - com: play challenges
    - radj: 'runner adjustment', field begins in 2020, indicates a runner starting on 2nd in extra innings; 
        - only 8 records are tagged, will ignore
'''


def download_and_unzip_csv(url, raw_file_name, n=10):

    response = requests.get(url)

    # Step 1: Download the ZIP file from the URL
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        # Step 2: Extract the team data file from the directory
        for file in zip_file.namelist():
            if file.endswith(f'{raw_file_name}'):
                zip_file.extract(file)

        # Step 3: Read the CSV data using pandas
        logger.info(f'Storing {raw_file_name}')
        df = pd.read_csv(
            raw_file_name,
            header=None,
            names=range(n)
        )
        df.rename(
            columns={
                0: 'data_type',
                1: 'metadata_1',
                2: 'metadata_2',
                3: 'metadata_3',
                4: 'metadata_4',
                5: 'metadata_5',
                6: 'metadata_6'
            },
            inplace=True
        )

        # Step 4: Delete File from path
        logger.info(f'Removing {raw_file_name}')
        delete_file(raw_file_name)

        # Step 5: Process the Data

        game_number = 0
        df['game_number'] = 0

        for k in range(len(df)):
            if df.loc[k, 'data_type'] == 'id':
                game_number += 1
            df.loc[k, 'game_number'] = game_number

        return df


def extract_game_log_data(year, team_acronym, division):
    # URL of raw data
    url = f'https://www.retrosheet.org/events/{year}eve.zip'
    # year of team data
    raw_file_name = f'{year}{team_acronym}.EV{division}'
    team_data = download_and_unzip_csv(url, raw_file_name)
    logger.info(f'{team_acronym}{year} Complete!')

    return team_data


game_log_years = [
    2023,
    # 2024 ### not yet available
]

is_read_team_data = True

if is_read_team_data:
    for i in game_log_years:
        team_acronyms = extract_team_acronym_and_division(i)
        for j in team_acronyms:

            logger.info(f'Reading {j[0]}{i} Roster Data')
            # Define the path for the csv file
            csv_file = f'../inputs/game_log_data/{j[0]}{i}_game_log_data.csv'

            table = (extract_game_log_data(i, j[0], j[1]))
            # Write the Table to a csv
            table.to_csv(csv_file)
            logger.info(f"Data has been written to '{csv_file}'")
else:
    logger.info('Skip Game Log Data')
    pass