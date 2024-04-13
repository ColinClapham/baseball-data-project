#!/usr/bin/env python
import argparse
from baseball_data_project.scripts.extract_team_data import run_extract_team_data
from baseball_data_project.scripts.extract_roster_data import run_extract_roster_data
from baseball_data_project.scripts.extract_game_log_data import run_extract_game_log_data
from baseball_data_project.scripts.clean_game_log_data import run_clean_game_log_data
import toml

# Specify the path to your config file
config_file_path = '/Users/colinclapham/github/baseball-data-project/config.toml'

# Load the TOML file
config_data = toml.load(config_file_path)


# Define the command line argument parser
def parse_arguments():
    parser = argparse.ArgumentParser(description="Tool to Extract Roster, Team, and Game Log data - structure pitch data")

    # Define command line arguments
    parser.add_argument('argument1', type=int, default=config_data["data_years"], help='Desired Year of Data to Extract')
    parser.add_argument('--option1', type=int, default=True, help='Set to false to skip team data extract')
    parser.add_argument('--option2', type=int, default=True, help='Set to false to skip create game info')
    parser.add_argument('--option3', type=int, default=True, help='Set to false to skip create lineup info')
    parser.add_argument('--option4', type=int, default=True, help='Set to false to skip create pitch info')

    return parser.parse_args()


# Function to handle the CLI logic
def main():
    args = parse_arguments()

    # Access parsed arguments
    arg_value = args.argument1
    option_value_1 = args.option1
    option_value_2 = args.option2
    option_value_3 = args.option3
    option_value_4 = args.option4

    # Implement your CLI logic based on the arguments
    print(f"Argument 1: {arg_value}")
    print(f"Option 1: {option_value_1}")
    print(f"Option 1: {option_value_2}")
    print(f"Option 1: {option_value_3}")
    print(f"Option 1: {option_value_4}")

    # Add more functionality based on the arguments and options
    run_extract_team_data([arg_value], option_value_1)
    run_extract_roster_data([arg_value], option_value_1)
    run_extract_game_log_data([arg_value], option_value_1)
    run_clean_game_log_data([arg_value], option_value_1, option_value_2, option_value_3, option_value_4)


# Entry point of the script
if __name__ == "__main__":

    main()
