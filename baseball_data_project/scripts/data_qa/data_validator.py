import pandas as pd
from baseball_data_project.scripts.utils import extract_team_acronym_and_division
from loguru import logger

'''
The Purpose of this script is to validate Team and game Log Data

The following checks currently exist:
    - game_log_validator: determines if all teams have the same number of desired games (81 max)
    - team_validator: determines if 30 teams exist for each year that exists
'''

data_years = [
    2023,
    # 2024 ### not yet available
]

is_game_log_validator = True
desired_game_total = 81

is_team_validator = True
desired_team_total = 30

if is_game_log_validator:
    for i in data_years:
        team_acronyms = extract_team_acronym_and_division(i)
        for j in team_acronyms:

            game_log_df = pd.read_csv(f'/Users/colinclapham/github/baseball-data-project/baseball_data_project/inputs/game_log_data/{i}/{j[0]}{i}_game_log_data.csv')

            if game_log_df['game_number'].max() != desired_game_total:
                logger.info(f'{j[0]}{i} Game Log file only has {game_log_df["game_number"].max()} Games')
            else:
                pass

        # logger.info(f"Game Log Validator is complete - all teams have {desired_game_total} Games")
else:
    logger.info('Do not validate Game Log Data')


if is_team_validator:
    for i in data_years:
        team_index_df = pd.read_csv(f'/Users/colinclapham/github/baseball-data-project/baseball_data_project/inputs/team_data/{i}/{i}_team_index_data.csv')

        if team_index_df['Team Acronym'].nunique() != desired_team_total:
            logger.info(f'{i} Team index file only has {team_index_df["Team Acronym"].nunique()} Teams')
        else:
            pass

        logger.info(f"Team Validator is complete - {desired_team_total} Teams are Present")
else:
    logger.info('Do not validate Team Data')
