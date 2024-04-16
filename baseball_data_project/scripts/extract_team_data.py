import requests
import zipfile
import io
import pandas as pd
from loguru import logger
import toml
from baseball_data_project.scripts.utils import delete_file, ensure_directory_exists

# Specify the path to your config file
config_file_path = '/Users/colinclapham/github/baseball-data-project/config.toml'

# Load the TOML file
config_data = toml.load(config_file_path)


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


def extract_team_data(year, ssl_block=True, env='prod'):
    # Starting March 26, 2024 - retrosheet updated their SSL Certification making it impossible to connect to their site
    if ssl_block:
        # Get absolute path to the file
        # team_data_raw_file_path = os.path.abspath(f'../TEAM{year}')
        if env == 'prod':
            team_data_raw_file_path = f'{config_data["input_file_path"]}/raw_files/{year}eve/TEAM{year}'
        elif env == 'dev':
            team_data_raw_file_path = f'{config_data["dev_input_file_path"]}/raw_files/{year}eve/TEAM{year}'

        team_data = clean_team_file(team_data_raw_file_path)
    else:
        # URL of raw data
        url = f'https://www.retrosheet.org/events/{year}eve.zip'
        # year of team data
        raw_file_name = f'TEAM{year}'
        team_data = download_and_unzip_csv(url, raw_file_name)
        logger.info(f'Year {year} Complete!')

    return team_data


def run_extract_team_data(game_log_years=[2023], is_read_team_data=True, env='prod'):
    if is_read_team_data:
        for i in game_log_years:
            logger.info(f'Reading {i} Team Data')
            # Define the path for the csv file
            if env == 'prod':
                csv_file = f'{config_data["input_file_path"]}/team_data/{i}/{i}_team_index_data.csv'
            elif env == 'dev':
                csv_file = f'{config_data["dev_input_file_path"]}/team_data/{i}/{i}_team_index_data.csv'

            ensure_directory_exists(csv_file)

            table = (extract_team_data(i))
            # Write the Table to a csv
            table.to_csv(csv_file, index=False)
            logger.info(f"Data has been written to '{csv_file}'")
    else:
        logger.info('Skip Team Data')
        pass


if __name__ == "__main__":

    # game_log_years = [
    #     2023,
    #     # 2024 ### not yet available
    # ]
    #
    # is_read_team_data = True

    run_extract_team_data()
