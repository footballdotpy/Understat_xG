import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from time import sleep, time
import pandas as pd
import warnings
from bs4 import BeautifulSoup
import requests

warnings.filterwarnings('ignore')

# create an empty list to store urls.
urls = []

base_url = f'https://understat.com/league/EPL/2022'

option = Options()
option.headless = False
driver = webdriver.Chrome("##############",options=option)
driver.get(base_url)


# Loop until the button is disabled
while True:
    # Check if the button is disabled
    button_disabled = driver.find_element(By.XPATH, "/html/body/div[1]/div[3]/div[2]/div/div/button[1]").get_attribute(
        "disabled")
    if button_disabled:
        break

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

driver.quit()


print("Urls extracted,Game scraping and cleaning commencing.")

# create list to store match data.
match_data = []

for match in urls:
    base_urls = match
#Use requests tuo get the webpage and BeautifulSoup to parse the page
    res = requests.get(base_urls)
    soup = BeautifulSoup(res.content, 'lxml')
    scripts = soup.find_all('script')
#get only the shotsData
    strings = scripts[1].string

# strip unnecessary symbols and get only JSON data
    ind_start = strings.index("('")+2
    ind_end = strings.index("')")
    json_data = strings[ind_start:ind_end]
    json_data = json_data.encode('utf8').decode('unicode_escape')

#convert string to json format
    data = json.loads(json_data)
#iterate JSON and extend match_data with a list of dicts
    match_data.extend([d for k in data.keys() for d in data[k]])

global epl
epl = pd.DataFrame(match_data)
epl['xG'] = epl['xG'].astype(float)

epl['fixtureName'] = epl['h_team'] + ' ' + 'v' + ' ' + epl['a_team']



# Create separate DataFrames for home and away teams
home_df = epl[epl['h_a'] == 'h'].copy()
away_df = epl[epl['h_a'] == 'a'].copy()

# Create separate columns for each team and situation
home_aggregated_df = home_df.pivot_table(index=['date','h_team', 'a_team','fixtureName'], columns='situation', values='xG', aggfunc='sum', fill_value=0).reset_index()
home_aggregated_df.columns = ['date','home_team','away_team','fixture','home_directfk_xG','home_corner_xG','home_op_xG','home_pen_xG','home_setpiece_xG']

away_aggregated_df = away_df.pivot_table(index=['date','h_team', 'a_team','fixtureName'], columns='situation', values='xG', aggfunc='sum', fill_value=0).reset_index()
away_aggregated_df.columns = ['date','home_team','away_team','fixture','away_directfk_xG','away_corner_xG','away_op_xG','away_pen_xG','away_setpiece_xG']

# Merge the home and away DataFrames based on team names
aggregated_df = pd.merge(home_aggregated_df, away_aggregated_df, on=['home_team', 'away_team'])

aggregated_df = aggregated_df.drop(['fixture_y','date_y'],axis=1)
aggregated_df = aggregated_df.rename({'fixture_x':'fixture',
                                      'date_x':'date'},axis=1)

#extract the date
aggregated_df['date'] = pd.to_datetime(aggregated_df['date'])
aggregated_df['date'] = aggregated_df['date'].dt.date

# Create 'home_xG' column by summing up the individual home xG columns
aggregated_df['home_xG'] = aggregated_df['home_directfk_xG'] + aggregated_df['home_corner_xG'] + aggregated_df['home_op_xG'] + aggregated_df['home_pen_xG'] + aggregated_df['home_setpiece_xG']

# Create 'away_xG' column by summing up the individual away xG columns
aggregated_df['away_xG'] = aggregated_df['away_directfk_xG'] + aggregated_df['away_corner_xG'] + aggregated_df['away_op_xG'] + aggregated_df['away_pen_xG'] + aggregated_df['away_setpiece_xG']

# non pen xG
aggregated_df['home_np_xG'] = abs(aggregated_df['home_xG'] - aggregated_df['home_pen_xG'])
aggregated_df['away_np_xG'] = abs(aggregated_df['away_xG'] - aggregated_df['away_pen_xG'])

# Create 'home_sp_xG' column by summing up the individual non pen and openplay columns
aggregated_df['home_sp_xG'] = aggregated_df['home_directfk_xG'] + aggregated_df['home_corner_xG'] + aggregated_df['home_setpiece_xG']

# Create 'away_sp_xG' column by summing up the individual non pen and openplay columns
aggregated_df['away_sp_xG'] = aggregated_df['away_directfk_xG'] + aggregated_df['away_corner_xG'] + aggregated_df['away_setpiece_xG']

aggregated_df.to_csv('epl2223.csv',index=False)

print("Process completed and csv exported!.")
