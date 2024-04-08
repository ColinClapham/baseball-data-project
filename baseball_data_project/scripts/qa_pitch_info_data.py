import pandas as pd
from utils import extract_team_acronym_and_division


game_log_years = [
    2023,
    # 2024 ### not yet available
]

for i in game_log_years:
    team_acronyms = extract_team_acronym_and_division(i)
    # team_acronyms = [['ANA', 'A']]
    for j in team_acronyms:
        # Specify the path to your Parquet file
        parquet_file_path = f'../deliverables/pitch_info/{j[0]}{i}_pitch_info_data.parquet'

        # Read the Parquet file into a DataFrame
        pitch_info_df = pd.read_parquet(parquet_file_path)

        # Check for missing values by column
        missing_values_by_column = pitch_info_df.isnull().sum()

        # Check if any missing values exist
        if missing_values_by_column.any():
            print(f"Missing Values by Column for {j[0]}{i}:")
            print(missing_values_by_column)
        else:
            pass

        # Get summary statistics using describe()
        description = pitch_info_df.describe()
        # Extract the maximum value
        max_at_bat_value = description.loc['max', 'At-Bat Pitch Count']
        max_total_pitch_value = description.loc['max', 'Total Pitcher Pitch Count']
        # Print the maximum value
        if max_at_bat_value > 15:
            print(f"Maximum At-Bat value for {j[0]}{i}: ", max_at_bat_value)
        else:
            pass
        if max_total_pitch_value > 120:
            print(f"Maximum Total-Pitch value for {j[0]}{i}: ", max_total_pitch_value)
        else:
            pass
