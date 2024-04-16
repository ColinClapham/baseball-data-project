import requests
import zipfile
import io
import pandas as pd
from baseball_data_project.scripts.utils import delete_file, extract_team_acronym_and_division, ensure_directory_exists
from loguru import logger
import toml


# Specify the path to your config file
config_file_path = '/Users/colinclapham/github/baseball-data-project/config.toml'

# Load the TOML file
config_data = toml.load(config_file_path)


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


def clean_roster_file(raw_file_name):
    df = pd.read_csv(
        raw_file_name,
        header=None
    )
    df.columns = ['uuid', 'Last Name', 'First Name', 'Bat', 'Throw', 'Team Acronym', 'Position']

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
        df.columns = ['uuid', 'Last Name', 'First Name', 'Bat', 'Throw', 'Team Acronym', 'Position']

        # Step 4: Delete File from path
        logger.info(f'Removing {raw_file_name}')
        delete_file(raw_file_name)

        return df


def extract_roster_data(year, team_acronym, ssl_block=True, env='prod'):
    if ssl_block:
        if env == 'prod':
            roster_data_raw_file_path = f'{config_data["input_file_path"]}/raw_files/{year}eve/{team_acronym}{year}.ROS'
        elif env == 'dev':
            roster_data_raw_file_path = f'{config_data["dev_input_file_path"]}/raw_files/{year}eve/{team_acronym}{year}.ROS'

        team_data = clean_roster_file(roster_data_raw_file_path)
    else:
        # URL of raw data
        url = f'https://www.retrosheet.org/events/{year}eve.zip'
        # year of team data
        raw_file_name = f'{team_acronym}{year}.ROS'
        team_data = download_and_unzip_csv(url, raw_file_name)
        logger.info(f'{team_acronym}{year} Complete!')

    return team_data


def run_extract_roster_data(roster_years=[2023], is_read_team_data=True, env='prod'):
    if is_read_team_data:
        for i in roster_years:
            team_acronyms = extract_team_acronym_and_division(i)
            for j in team_acronyms:
                logger.info(f'Reading {j[0]}{i} Roster Data')
                # Define the path for the csv file
                if env == 'prod':
                    csv_file = f'{config_data["input_file_path"]}/roster_data/{i}/{j[0]}{i}_roster_index_data.csv'
                elif env == 'dev':
                    csv_file = f'{config_data["dev_input_file_path"]}/roster_data/{i}/{j[0]}{i}_roster_index_data.csv'

                ensure_directory_exists(csv_file)

                table = (extract_roster_data(i, j[0]))
                # Write the Table to a csv
                table.to_csv(csv_file, index=False)
                logger.info(f"Data has been written to '{csv_file}'")
    else:
        logger.info('Skip Roster Data')
        pass


if __name__ == "__main__":

    # roster_years = [
    #     2022,
    #     # 2024 ### not yet available
    # ]
    #
    # is_read_team_data = True

    run_extract_roster_data()
