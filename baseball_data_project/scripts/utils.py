from pathlib import Path
from loguru import logger
import pandas as pd
import os


def ensure_directory_exists(file_path):
    """
    Ensure that the directory of the given file path exists.
    If the directory doesn't exist, create it.
    """
    directory = os.path.dirname(file_path)

    # Check if the directory already exists
    if not os.path.exists(directory):
        try:
            # Create the directory and any missing parent directories
            os.makedirs(directory)
            print(f"Directory '{directory}' created successfully.")
        except OSError as e:
            print(f"Error: Failed to create directory '{directory}'.")
            print(f"Reason: {e}")
            return False

    return True


def delete_file(file_path):
    try:
        os.remove(file_path)
        logger.info(f"File '{file_path}' deleted successfully.")
    except FileNotFoundError:
        logger.info(f"File '{file_path}' not found.")
    except Exception as e:
        logger.info(f"Error occurred while deleting the file: {e}")


def extract_team_acronym_and_division(year):
    my_file = Path(f'/Users/colinclapham/github/baseball-data-project/baseball_data_project/inputs/team_data/{year}/{year}_team_index_data.csv')
    if my_file.is_file():
        logger.info(f'{year} Team Data Exists!')
        team_data = pd.read_csv(f'/Users/colinclapham/github/baseball-data-project/baseball_data_project/inputs/team_data/{year}/{year}_team_index_data.csv')
        acronyms = team_data[['Team Acronym', 'Division']].values.tolist()
    else:
        logger.info(f'{year} Team Data does not exist - run extract_team_data.py')

    return acronyms

ensure_directory_exists('/Users/colinclapham/github/baseball-data-project/baseball_data_project/inputs/team_data/2020/2020_team_index_data.csv')