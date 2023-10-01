import json
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from time import sleep, time
import pandas as pd
import warnings
from bs4 import BeautifulSoup
import requests
import time
from tqdm import tqdm

# Start the timer
start_time = time.time()

warnings.filterwarnings('ignore')

# create an empty list to store urls.
base_urls = []
urls = []
errors = []

seasons = [2014,2015,2016,2017,2018,2019,2020,2021,2022,2023]

##'La_Liga', 'Bundesliga', 'Ligue_1', 'Serie_A'

competitions = ['EPL']

for competition in competitions:
    for season in seasons:
        base_url = f'https://understat.com/league/{competition}/{season}'
        base_urls.append(base_url)

        season_col_value = int(base_url[-4:])
        competition_value = competition

        option = Options()
        option.headless = True
        driver = webdriver.Chrome("C:/Users/paulc/Documents/Understat_xG/chromedriver.exe", options=option)
        driver.get(base_url)

        print("Collecting Urls for", base_url)

        # Loop until the button is disabled
        while True:

            # Fetch the current page URLs
            elements = driver.find_elements(By.CSS_SELECTOR, 'a.match-info')
            page_urls = [element.get_attribute('href') for element in elements]

            # Store the URLs in the main list
            urls.extend(page_urls)

            # Scroll down or perform any necessary actions to load the next page

            # Wait for a short duration to allow the next page to load
            sleep(3)

            # Click the button to navigate to the next page
            next_button = driver.find_element(By.XPATH, "/html/body/div[1]/div[3]/div[2]/div/div/button[1]")
            next_button.click()

            # Check if the button is disabled
            button_disabled = driver.find_element(By.XPATH,
                                                  "/html/body/div[1]/div[3]/div[2]/div/div/button[1]").get_attribute(
                "disabled")
            if button_disabled:
                # Fetch the current page URLs
                elements = driver.find_elements(By.CSS_SELECTOR, 'a.match-info')
                page_urls = [element.get_attribute('href') for element in elements]

                # Store the URLs in the main list
                urls.extend(page_urls)
                break

        driver.quit()

print("The number of games extracted for data cleaning is: ", len(urls))

game_dataframe = pd.DataFrame()

for match in tqdm(urls, desc="Scraping Matches"):
    match_data = []
    try:
        # Use requests to get the webpage and BeautifulSoup to parse the page
        res = requests.get(match)
        soup = BeautifulSoup(res.content, 'lxml')
        scripts = soup.find_all('script')
        # Get only the shotsData
        strings = scripts[1].string
        # Strip unnecessary symbols and get only JSON data
        ind_start = strings.index("('") + 2
        ind_end = strings.index("')")
        json_data = strings[ind_start:ind_end]
        json_data = json_data.encode('utf8').decode('unicode_escape')
        # Convert string to JSON format
        data = json.loads(json_data)
        # Iterate JSON and extend match_data with a list of dicts
        match_data.extend([d for k in data.keys() for d in data[k]])

        df = pd.DataFrame(match_data)

        df['minute'] = df['minute'].astype(int)
        df['xG'] = df['xG'].astype(float)
        df['fixtureName'] = df['h_team'] + ' ' + '--' + ' ' + df['a_team']

        df['competition'] = competition_value

        # Create values for getting the shots in the game per team.
        df['shot_num'] = 1

        df['shotontarget_num'] = np.where((df['result'] == 'SavedShot') | (df['result'] == 'Goal'), 1, 0)

        df['Goal'] = np.where(df['result'] == 'Goal', 1, 0)

        df = df.sort_values(by='minute', ascending=True).drop_duplicates().reset_index()

        # Initialize columns with default values
        df['no_goal_xg_home'] = 0.0
        df['no_goal_xg_away'] = 0.0
        df['losing_xg_home'] = 0.0
        df['losing_xg_away'] = 0.0
        df['winning_xg_home'] = 0.0
        df['winning_xg_away'] = 0.0
        df['draw_xg_home'] = 0.0
        df['draw_xg_away'] = 0.0
        df['home_goals'] = 0
        df['away_goals'] = 0
        df['home_shots_on_target'] = 0
        df['away_shots_on_target'] = 0
        df['home_shots'] = 0
        df['away_shots'] = 0

        home_goals = 0
        away_goals = 0

        # Initialize a flag to track if a goal was scored in the previous row
        goal_scored = False

        # Iterate through the rows of the DataFrame
        for index, row in df.iterrows():
            if row['h_a'] == 'h' and row['result'] == 'Goal':
                home_goals += 1
                goal_scored = True
            elif row['h_a'] == 'a' and row['result'] == 'Goal':
                away_goals += 1
                goal_scored = True
            else:
                goal_scored = False  # Reset the flag if no goal was scored in this row

            # Update 'home_goals' and 'away_goals' for the next row
            if index + 1 < len(df):
                df.at[index + 1, 'home_goals'] = home_goals
                df.at[index + 1, 'away_goals'] = away_goals

        for index, row in df.iterrows():
            if row['h_a'] == 'h':
                if row['home_goals'] == 0 and row['away_goals'] == 0:
                    df.at[index, 'no_goal_xg_home'] = row['xG']
                elif row['home_goals'] < row['away_goals']:
                    df.at[index, 'losing_xg_home'] = row['xG']
                elif row['home_goals'] > row['away_goals']:
                    df.at[index, 'winning_xg_home'] = row['xG']
                elif row['home_goals'] == row['away_goals'] and row['home_goals'] != 0 and row['away_goals'] != 0:
                    df.at[index, 'draw_xg_home'] = row['xG']

            if row['h_a'] == 'a':
                if row['home_goals'] == 0 and row['away_goals'] == 0:
                    df.at[index, 'no_goal_xg_away'] = row['xG']
                elif row['away_goals'] < row['home_goals']:
                    df.at[index, 'losing_xg_away'] = row['xG']
                elif row['away_goals'] > row['home_goals']:
                    df.at[index, 'winning_xg_away'] = row['xG']
                elif row['away_goals'] == row['home_goals'] and row['away_goals'] != 0 and row['home_goals'] != 0:
                    df.at[index, 'draw_xg_away'] = row['xG']

            if row['h_a'] == 'h':
                df['home_shots'] = 1

            if row['h_a'] == 'a':
                df['away_shots'] = 1

            if row['h_a'] == 'h' and row['result'] in ['SavedShot', 'Goal']:
                df.at[index, 'home_shots_on_target'] = 1

            if row['h_a'] == 'a' and row['result'] in ['SavedShot', 'Goal']:
                df.at[index, 'away_shots_on_target'] = 1

        # Set 'no_goal_xg_home' to 0 if home_goals or away_goals != 0
        df['no_goal_xg_home'] = df.apply(
            lambda row: 0.0 if row['home_goals'] != 0 or row['away_goals'] != 0 else row['no_goal_xg_home'], axis=1)

        # Set 'no_goal_xg_away' to 0 if home_goals or away_goals != 0
        df['no_goal_xg_away'] = df.apply(
            lambda row: 0.0 if row['home_goals'] != 0 or row['away_goals'] != 0 else row['no_goal_xg_away'], axis=1)

        # create a scored first column
        df['home_team_first_goal_min'] = np.where((df['result'] == 'Goal') & (df['h_a'] == 'h'), df['minute'], 200).min()
        df['away_team_first_goal_min'] = np.where((df['result'] == 'Goal') & (df['h_a'] == 'a'), df['minute'], 200).min()

        # create a column if the team received a penalty
        df['home_team_penalty'] = np.where((df['situation'] == 'Penalty') & (df['h_a'] == 'h'), 1, 0).max()
        df['away_team_penalty'] = np.where((df['situation'] == 'Penalty') & (df['h_a'] == 'a'), 1, 0).max()

        # create a column on who scored first
        df['home_score_first'] = np.where(df['home_team_first_goal_min'] < df['away_team_first_goal_min'], 1, 0)
        df['away_score_first'] = np.where(df['home_team_first_goal_min'] > df['away_team_first_goal_min'], 1, 0)

        # extract time of game
        df['date'] = pd.to_datetime((df['date']))
        df['time'] = df['date'].dt.time

        # Create a DataFrame for home
        home_df = df[df['h_a'] == 'h']
        # Create a DataFrame for away
        away_df = df[df['h_a'] == 'a']

        # Select the relevant columns
        selected_columns_home = ['date', 'time', 'season', 'h_team', 'no_goal_xg_home', 'losing_xg_home', 'winning_xg_home',
                                 'draw_xg_home', 'home_shots', 'home_shots_on_target']
        selected_columns_away = ['date', 'time', 'season', 'a_team', 'no_goal_xg_away', 'losing_xg_away', 'winning_xg_away',
                                 'draw_xg_away', 'away_shots', 'away_shots_on_target']

        selected_columns_home_goals = ['date', 'time', 'season', 'h_team', 'home_goals', 'home_team_first_goal_min',
                                       'home_team_penalty', 'home_score_first']
        selected_columns_away_goals = ['date', 'time', 'season', 'a_team', 'away_goals', 'away_team_first_goal_min',
                                       'away_team_penalty', 'away_score_first']

        # Group by 'h_team' (home team) and calculate the sum for each xG category
        home_team_totals = home_df[selected_columns_home].groupby('h_team').sum().reset_index()
        # Group by 'h_team' (home team) and calculate the max of home goals
        home_goals_max = home_df[selected_columns_home_goals].groupby('h_team').max().reset_index()

        # Merge the two DataFrames on 'h_team' to combine the totals and max home goals
        home_team_summary = home_team_totals.merge(home_goals_max, on='h_team', how='left')
        home_team_summary['total_home_xG'] = home_team_summary[
            ['no_goal_xg_home', 'losing_xg_home', 'winning_xg_home', 'draw_xg_home']].sum(axis=1)

        # Group by 'a_team' (away team) and calculate the sum for each xG category
        away_team_totals = away_df[selected_columns_away].groupby('a_team').sum().reset_index()
        # Group by 'a_team' (home team) and calculate the max of home goals
        away_goals_max = away_df[selected_columns_away_goals].groupby('a_team').max().reset_index()

        # Merge the two DataFrames on 'h_team' to combine the totals and max home goals
        away_team_summary = away_team_totals.merge(away_goals_max, on='a_team', how='left')
        away_team_summary['total_away_xG'] = away_team_summary[
            ['no_goal_xg_away', 'losing_xg_away', 'winning_xg_away', 'draw_xg_away']].sum(axis=1)

        combined_game = pd.concat([home_team_summary, away_team_summary], axis=1)
        # Remove duplicate columns in the pandas DataFrame
        combined_game = combined_game.loc[:, ~combined_game.columns.duplicated()]

        combined_game['competition'] = competition_value

        combined_game = combined_game[
            ['date', 'time', 'competition', 'season', 'h_team', 'no_goal_xg_home', 'losing_xg_home', 'winning_xg_home',
             'draw_xg_home', 'home_goals', 'home_team_first_goal_min', 'home_team_penalty', 'home_score_first',
             'home_shots', 'home_shots_on_target',
             'total_home_xG', 'a_team', 'no_goal_xg_away', 'losing_xg_away', 'winning_xg_away', 'draw_xg_away',
             'away_goals', 'away_team_first_goal_min', 'away_team_penalty', 'away_score_first', 'away_shots',
             'away_shots_on_target', 'total_away_xG']]

        # Append the processed data to game_dataframe
        game_dataframe = pd.concat([game_dataframe, combined_game], axis=0)


        # Reset the index of the final DataFrame
        game_dataframe.reset_index(drop=True, inplace=True)

        # Set 'away_team_first_goal_min' and 'home_team_first_goal_min' to 0 where the value is 200
        game_dataframe.loc[game_dataframe['away_team_first_goal_min'] == 200, 'away_team_first_goal_min'] = 0
        game_dataframe.loc[game_dataframe['home_team_first_goal_min'] == 200, 'home_team_first_goal_min'] = 0

    except Exception as e:
        print(f"Error encountered for match: {match}")
        print(f"Error message: {str(e)}")
        errors.append(match)
        continue

game_dataframe.to_csv('epl_gameState_extra.csv', index=False)


# End the timer
end_time = time.time()

# Calculate the total time taken
total_time = end_time - start_time

# Print the total time taken
print("Total time taken: {:.2f} seconds".format(total_time))
print("Process completed and csv exported!.")
