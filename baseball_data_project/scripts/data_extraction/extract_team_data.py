import requests
import zipfile
import io
import pandas as pd
from baseball_data_project.scripts.utils import delete_file
from loguru import logger
import ssl

ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)  # Use PROTOCOL_TLS_CLIENT for client connections

'''
The Purpose of this script is to extract data relating to how TEAMS are referenced in game log data

The data includes the following fields:
    - Team Acronym: three letter string
    - Division: A for American League, N for National League
    - Team City: North American city in which the team is located
    - Team Name

The Following field is added for clarity:
    - Full Team Name: concatenation of Team City and Team Name
'''


def clean_team_file(raw_file_name):
    df = pd.read_csv(
        raw_file_name,
        header=None
    )
    df.columns = ["Team Acronym", "Division", "Team City", "Team Name"]

    # Step 5: Process the Data
    df['Full Team Name'] = df['Team City'] + ' ' + df['Team Name']

    return df


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
        df.columns = ["Team Acronym", "Division", "Team City", "Team Name"]

        # Step 4: Delete File from path
        logger.info(f'Removing {raw_file_name}')
        delete_file(raw_file_name)

        # Step 5: Process the Data
        df['Full Team Name'] = df['Team City'] + ' ' + df['Team Name']

        return df


def extract_team_data(year, ssl_block=True):
    # Starting March 26, 2024 - retrosheet updated their SSL Certification making it impossible to connect to their site
    if ssl_block:
        team_data_raw_file_path = f'../../inputs/raw_files/{year}eve/TEAM{year}'
        team_data = clean_team_file(team_data_raw_file_path)
    else:
        # URL of raw data
        url = f'https://www.retrosheet.org/events/{year}eve.zip'
        # year of team data
        raw_file_name = f'TEAM{year}'
        team_data = download_and_unzip_csv(url, raw_file_name)
        logger.info(f'Year {year} Complete!')

    return team_data


game_log_years = [
    2022,
    # 2024 ### not yet available
]

is_read_team_data = True

if is_read_team_data:
    for i in game_log_years:
        logger.info(f'Reading {i} Team Data')
        # Define the path for the csv file
        csv_file = f'../../inputs/team_data/{i}/{i}_team_index_data.csv'

        table = (extract_team_data(i))
        # Write the Table to a csv
        table.to_csv(csv_file)
        logger.info(f"Data has been written to '{csv_file}'")
else:
    logger.info('Skip Team Data')
    pass
