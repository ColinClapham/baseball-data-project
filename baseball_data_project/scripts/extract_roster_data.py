import requests
import zipfile
import io
import pandas as pd
from utils import delete_file, extract_team_acronym_and_division
from loguru import logger

'''
The Purpose of this script is to extract data relating to how PLAYERS are referenced in game log data

The data includes the following fields:
    - uuid: unique string id for each player (Primary Key)
    - Last Name
    - First Name
    - Bat: L (Left) or R (Right)
    - Throw: L (Left) or R (Right)
    - Team Acronym: three letter string
    - Position

The Following field is added for clarity:
    - Full Team Name: concatenation of Team City and Team Name
'''


def download_and_unzip_csv(url, raw_file_name):

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
            header=None
        )
        df.columns = ['uuid', 'Last Name', 'First Name', 'Bat', 'Throw', 'Team Acronym', 'Position']

        # Step 4: Delete File from path
        logger.info(f'Removing {raw_file_name}')
        delete_file(raw_file_name)

        return df


def extract_roster_data(year, team_acronym):
    # URL of raw data
    url = f'https://www.retrosheet.org/events/{year}eve.zip'
    # year of team data
    raw_file_name = f'{team_acronym}{year}.ROS'
    team_data = download_and_unzip_csv(url, raw_file_name)
    logger.info(f'{team_acronym}{year} Complete!')

    return team_data


roster_years = [
    2023,
    # 2024 ### not yet available
]

is_read_team_data = True

if is_read_team_data:
    for i in roster_years:
        team_acronyms = extract_team_acronym_and_division(i)
        for j in team_acronyms:

            logger.info(f'Reading {j[0]}{i} Roster Data')
            # Define the path for the csv file
            csv_file = f'../inputs/roster_data/{j[0]}{i}_roster_index_data.csv'

            table = (extract_roster_data(i, j[0]))
            # Write the Table to a csv
            table.to_csv(csv_file)
            logger.info(f"Data has been written to '{csv_file}'")
else:
    logger.info('Skip Roster Data')
    pass
