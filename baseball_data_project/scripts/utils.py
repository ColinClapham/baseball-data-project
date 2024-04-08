import os
from pathlib import Path
from loguru import logger
import pandas as pd


def delete_file(file_path):
    try:
        os.remove(file_path)
        logger.info(f"File '{file_path}' deleted successfully.")
    except FileNotFoundError:
        logger.info(f"File '{file_path}' not found.")
    except Exception as e:
        logger.info(f"Error occurred while deleting the file: {e}")


def extract_team_acronym_and_division(year):
    my_file = Path(f'../inputs/{year}_team_index_data.csv')
    if my_file.is_file():
        logger.info(f'{year} Team Data Exists!')
        team_data = pd.read_csv(f'../inputs/{year}_team_index_data.csv')
        acronyms = team_data[['Team Acronym', 'Division']].values.tolist()
    else:
        logger.info(f'{year} Team Data does not exist - run extract_team_data.py')

    return acronyms
