# TO DO
# 1. Fair probability
# 2. Hedge opportunities
# 3. Datapane map
# 4. Change since prior poll

# Import modules
import json
import requests
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
pd.set_option('display.max_rows', None) #print all rows without truncating
pd.options.mode.chained_assignment = None #hide SettingWithCopyWarning
import numpy as np
import datetime
import os
import zipfile #Economist
import urllib.request #Economist



# Pull in market data from PredictIt's API
Predictit_URL = "https://www.predictit.org/api/marketdata/all/"
Predictit_response = requests.get(Predictit_URL)
jsondata = Predictit_response.json()

# Replace null values with zero
def dict_clean(items):
    result = {}
    for key, value in items:
        if value is None:
            value = 0
        result[key] = value
    return result
dict_str = json.dumps(jsondata)
jsondata = json.loads(dict_str, object_pairs_hook=dict_clean)

# Market data by contract/price in dataframe
data = []
for p in jsondata['markets']:
    for k in p['contracts']:
        data.append([p['id'],p['name'],k['id'],k['name'],k['bestBuyYesCost'],k['bestBuyNoCost'],k['bestSellYesCost'],k['bestSellNoCost']])

# Pandas dataframe named 'predictit_df'
predictit_df = pd.DataFrame(data)

# Update dataframe column names
predictit_df.columns=['Market_ID','Market_Name','Contract_ID','Contract_Name','PredictIt_Yes','bestBuyNoCost','BestSellYesCost','BestSellNoCost']

# Filter PredicitIt dataframe to presidential state markets/contracts
predictit_df = predictit_df[predictit_df['Market_Name'].str.contains("Which party will win") & predictit_df['Market_Name'].str.contains("2020 presidential election?")]

# Fix annoying typo (double space) in congressional district market names
predictit_df['Market_Name'] = predictit_df['Market_Name'].str.replace('in the  2020','in the 2020')

# Split Market_Name column into state name column
start_string = "Which party will win"
end_string = "in the 2020 presidential election?"
predictit_df['a'], predictit_df['state'] = predictit_df['Market_Name'].str.split(start_string, 1).str
predictit_df['state'], predictit_df['b'] = predictit_df['state'].str.split(end_string, 1).str
del predictit_df['a']
del predictit_df['b']

# Create answer column from contract names
predictit_df['answer'] = predictit_df['Contract_Name'].str.replace('Republican','Trump').str.replace('Democratic','Biden')

# Strip trailing/leading whitespaces in answer and state columns
predictit_df['state'] = predictit_df['state'].str.strip()
predictit_df['answer'] = predictit_df['answer'].str.strip()



# Pull in polling data from 538
pres_polling = pd.read_csv('https://projects.fivethirtyeight.com/polls-page/president_polls.csv')
pres_polling = pres_polling.dropna(subset=['state'])

# Drop extraneous columns
pres_polling = pres_polling.drop(['pollster_id', 'pollster','sponsor_ids','sponsors','display_name', 'pollster_rating_id', 'pollster_rating_name', 'fte_grade', 'sample_size', 'population', 'population_full', 'methodology', 'seat_number', 'seat_name', 'start_date', 'sponsor_candidate', 'internal', 'partisan', 'tracking', 'nationwide_batch', 'ranked_choice_reallocated', 'notes', 'url'], axis=1)

# Standardize congressional district names in 538 with PredictIt
pres_polling['state'] = pres_polling['state'].str.replace('Maine CD-1','ME-01')
pres_polling['state'] = pres_polling['state'].str.replace('Maine CD-2','ME-02')
pres_polling['state'] = pres_polling['state'].str.replace('Nebraska CD-2','NE-02')

# Filter to most recent poll for Biden & Trump
# create a count column for 'question_id' to work around "Delaware problem": multiple matchups in same survey
pres_polling['created_at'] = pd.to_datetime(pres_polling['created_at']) #convert 'created_at' to datetime
recent_pres_polling = pres_polling[pres_polling['answer'].isin(['Biden', 'Trump'])]
recent_pres_polling['Count'] = recent_pres_polling.groupby('question_id')['question_id'].transform('count')
recent_pres_polling = recent_pres_polling[(recent_pres_polling.Count > 1)]
recent_pres_polling = recent_pres_polling.sort_values(by=['question_id'], ascending=False).drop_duplicates(['state', 'candidate_name'], keep='first')

# Rename 538 'pct' column to '538_latest_poll'
recent_pres_polling = recent_pres_polling.rename({'pct': '538_latest_poll'}, axis=1)

# Rename 538 'end_date' column to '538_poll_date'
recent_pres_polling = recent_pres_polling.rename({'end_date': '538_poll_date'}, axis=1)



# Pull in polling data from 538 polling averages
pres_poll_avg = pd.read_csv('https://projects.fivethirtyeight.com/2020-general-data/presidential_poll_averages_2020.csv')

# Drop extraneous columns
pres_poll_avg = pres_poll_avg.drop(['cycle'], axis=1)

# Standardize congressional district names in 538 polling averages with PredictIt
pres_poll_avg['state'] = pres_poll_avg['state'].str.replace('Maine CD-1','ME-01')
pres_poll_avg['state'] = pres_poll_avg['state'].str.replace('Maine CD-2','ME-02')
pres_poll_avg['state'] = pres_poll_avg['state'].str.replace('Nebraska CD-2','NE-02')

# Standarize candidate names and column name
pres_poll_avg.replace({'candidate_name' : { 'Joseph R. Biden Jr.' : 'Biden', 'Donald Trump' : 'Trump'}})
pres_poll_avg['answer'] = pres_poll_avg['candidate_name']

# Filter to most recent poll for Biden & Trump
pres_poll_avg['modeldate'] = pd.to_datetime(pres_poll_avg['modeldate']) #convert 'modeldate' to datetime
pres_poll_avg = pres_poll_avg.sort_values(by=['modeldate']).drop_duplicates(['state', 'candidate_name'], keep='last')
pres_poll_avg = pres_poll_avg[pres_poll_avg['answer'].isin(['Biden', 'Trump'])]

# Round pct_estimate and pct_trend_adjusted to 2 decimal places
pres_poll_avg['pct_estimate'] = pres_poll_avg['pct_estimate'].round(2)
pres_poll_avg['pct_trend_adjusted'] = pres_poll_avg['pct_trend_adjusted'].round(2)

# Merge 538 poll and 538 poll averages dataframes together
recent_pres_polling = pd.merge(recent_pres_polling, pres_poll_avg, on=['state', 'answer'], how='left')



# Pull in most recent state-level model data from 538
pres_model = pd.read_csv('https://projects.fivethirtyeight.com/2020-general-data/presidential_state_toplines_2020.csv')

# Only keep latest models
pres_model = pres_model.sort_values(by=['modeldate'], ascending=False).drop_duplicates(['state', 'branch'], keep='first')

#Split into 2 dataframes for Trump and Biden
pres_model_inc = pres_model[['candidate_inc', 'state', 'winstate_inc', 'voteshare_inc', 'voteshare_inc_hi', 'voteshare_inc_lo', 'win_EC_if_win_state_inc', 'win_state_if_win_EC_inc']]
pres_model_chal = pres_model[['candidate_chal', 'state', 'winstate_chal', 'voteshare_chal', 'voteshare_chal_hi', 'voteshare_chal_lo', 'win_EC_if_win_state_chal', 'win_state_if_win_EC_chal']]

# Remove _inc and _chal from column names
pres_model_inc = pres_model_inc.rename(columns={'candidate_inc': 'answer', 'winstate_inc': 'winstate', 'voteshare_inc': 'voteshare', 'voteshare_inc_hi': 'voteshare_hi', 'voteshare_inc_lo': 'voteshare_lo', 'win_EC_if_win_state_inc': 'win_EC_if_win_state', 'win_state_if_win_EC_inc': 'win_state_if_win_EC'} )
pres_model_chal = pres_model_chal.rename(columns={'candidate_chal': 'answer', 'winstate_chal': 'winstate','voteshare_chal': 'voteshare', 'voteshare_chal_hi': 'voteshare_hi', 'voteshare_chal_lo': 'voteshare_lo', 'win_EC_if_win_state_chal': 'win_EC_if_win_state', 'win_state_if_win_EC_chal': 'win_state_if_win_EC'} )

# Concatenate Trump and Biden dataframes together
frames = [pres_model_inc, pres_model_chal]
pres_model = pd.concat(frames)

# Change 'District of Columbia' to 'DC'
pres_model['state'] = pres_model['state'].str.replace('District of Columbia','DC')

# Standardize congressional district names
pres_model['state'] = pres_model['state'].str.replace('ME-1','ME-01')
pres_model['state'] = pres_model['state'].str.replace('ME-2','ME-02')
pres_model['state'] = pres_model['state'].str.replace('NE-1','NE-01')
pres_model['state'] = pres_model['state'].str.replace('NE-2','NE-02')
pres_model['state'] = pres_model['state'].str.replace('NE-3','NE-03')

# Rename 538 'end_date' column to '538_poll_date'
pres_model = pres_model.rename({'winstate': '538_model'}, axis=1)



# Pull in most recent state-level model data from The Economist
url = 'https://cdn.economistdatateam.com/us-2020-forecast/data/president/economist_model_output.zip'
remote = urllib.request.urlopen(url)  # read remote file
data = remote.read()  # read from remote file
remote.close()  # close urllib request
local = open('economist_model_output.zip', 'wb')  # write binary to local file
local.write(data)
local.close()  # close file
zf = zipfile.ZipFile('economist_model_output.zip') 
econ_df = pd.read_csv(zf.open('output/site_data//state_averages_and_predictions_topline.csv'))

# Rename columns in econ_df
#econ_df = econ_df.rename({'projected_win_prob': 'dem_projected_win_prob'})

# Create Trump dataframe from Biden dataframe
econ_df_trump = econ_df.copy()

# Add answer column
econ_df['answer'] = 'Biden'
econ_df_trump['answer'] = 'Trump'

# Drop extraneous columns
econ_df = econ_df.drop(columns=['dem_average_low', 'dem_average_mean', 'dem_average_high', 'projected_vote_low', 'projected_vote_high', 'projected_vote_mean'])
econ_df_trump = econ_df_trump.drop(columns=['dem_average_low', 'dem_average_mean', 'dem_average_high', 'projected_vote_low', 'projected_vote_high', 'projected_vote_mean'])

# Calculate Trump probabilities from Biden probabilities
econ_df_trump['projected_win_prob'] = 1 - econ_df_trump['projected_win_prob']

# Concatenate dataframes
frames = [econ_df, econ_df_trump]
econ_df = pd.concat(frames)

# Standardize state names in econ_df
econ_df['state'] = econ_df['state'].map({
						'AL':'Alabama',
						'AK':'Alaska',
						'AZ':'Arizona',
						'AR':'Arkansas',
						'CA':'California',
						'CO':'Colorado',
						'CT':'Connecticut',
						'DE':'Delaware',
						'DC':'DC',
						'FL':'Florida',
						'GA':'Georgia',
						'HI':'Hawaii',
						'ID':'Idaho',
						'IL':'Illinois',
						'IN':'Indiana',
						'IA':'Iowa',
						'KS':'Kansas',
						'KY':'Kentucky',
						'LA':'Louisiana',
						'ME':'Maine',
						'MD':'Maryland',
						'MA':'Massachusetts',
						'MI':'Michigan',
						'MN':'Minnesota',
						'MS':'Mississippi',
						'MO':'Missouri',
						'MT':'Montana',
						'NE':'Nebraska',
						'NV':'Nevada',
						'NH':'New Hampshire',
						'NJ':'New Jersey',
						'NM':'New Mexico',
						'NY':'New York',
						'NC':'North Carolina',
						'ND':'North Dakota',
						'OH':'Ohio',
						'OK':'Oklahoma',
						'OR':'Oregon',
						'PA':'Pennsylvania',
						'RI':'Rhode Island',
						'SC':'South Carolina',
						'SD':'South Dakota',
						'TN':'Tennessee',
						'TX':'Texas',
						'UT':'Utah',
						'VT':'Vermont',
						'VA':'Virginia',
						'WA':'Washington',
						'WV':'West Virginia',
						'WI':'Wisconsin',
						'WY':'Wyoming'})

# Change column names
econ_df = econ_df.rename(columns={"projected_win_prob": "Econ_model"})
econ_df = econ_df.rename(columns={"date": "Econ_date"})



# Pull in gambling odds
odds_df = pd.read_csv('https://raw.githubusercontent.com/mauricebransfield/predictit_538_odds/master/odds_state_presidential.csv', index_col=[0]) # error_bad_lines=False,

# Replace hyphen in state names with space
odds_df['state'] = odds_df['state'].str.replace('-',' ') 

# Standardize Washington DC & Washington State
odds_df['state'] = odds_df['state'].str.replace('Washington Dc','DC')
odds_df['state'] = odds_df['state'].str.replace('Washington State','Washington')

# Replace party with candidate names
odds_df['answer'] = odds_df['answer'].str.replace('Republicans','Trump')
odds_df['answer'] = odds_df['answer'].str.replace('Democratic','Biden')
odds_df['answer'] = odds_df['answer'].str.replace('Democrats','Biden')
odds_df['answer'] = odds_df['answer'].str.replace('Democrat','Biden')

# Drop columns with all nan values
odds_df = odds_df.dropna(axis=1, how='all')

# Convert odds_df column headers to list
odds_df_columns = list(odds_df.columns.values)
odds_df_columns.remove('answer')
odds_df_columns.remove('state')
odds_df_loop = odds_df.copy()
del odds_df_loop['answer']
del odds_df_loop['state']

# denominator / (denominator + numerator) = implied probability
# Loop through odds columns to convert fractional odds to new column of implied probability
for i in odds_df_columns:
	odds_df_loop['numerator'], odds_df_loop['denominator'] = odds_df_loop[i].str.split('/', 1).str
	odds_df_loop['denominator'] = pd.to_numeric(odds_df_loop['denominator'], errors='coerce').fillna(0).astype(np.int64)
	odds_df_loop['denominator'] = odds_df_loop['denominator'].mask(odds_df_loop['denominator']==0).fillna(1) # workaround
	odds_df_loop['numerator'] = pd.to_numeric(odds_df_loop['numerator'], errors='coerce').fillna(0).astype(np.int64)
	odds_df_loop[str(i) + '_imp_prob'] = (odds_df_loop['denominator'] / (odds_df_loop['denominator'] + odds_df_loop['numerator'])).round(2)

# Concatenate imp_prob columns with 'answer' and 'state' columns
asdf = [odds_df['answer'], odds_df['state']]
headers = ["answer", "state"]
as_df = pd.concat(asdf, axis=1, keys=headers)
odds_imp_prob_df = pd.concat([odds_df_loop, as_df], axis=1)

# Merge PredictIt and odds dataframes together
df = pd.merge(predictit_df, odds_imp_prob_df, on=['state', 'answer'], how='left')

# Merge 538 polls into new dataframe
df = pd.merge(df, recent_pres_polling, on=['state', 'answer'], how='left')

# Merge 538 models into new dataframe
df = pd.merge(df, pres_model, on=['state', 'answer'], how='left')

# Merge Economist models into new dataframe
df = pd.merge(df, econ_df, on=['state', 'answer'], how='left')

# workaround to fix previous workaround
for i in odds_df_columns:
	mask = df[i].isnull()
	column_name = str(i) + '_imp_prob'
	df.loc[mask, column_name] = np.nan

# Find average of all implied probabilities
m = df.loc[:, df.columns.str.contains('_imp_prob')]
odds_df_columns2 = list(m.columns.values)
df['ari_mean_imp_prob'] = df[odds_df_columns2].mean(1).round(2)

# Sort alphabetically by state and answer
df = df.sort_values(["state", "answer"])

# Create column matching Trump Yes cost with Biden No cost, and vice versa
trump = (df['answer']=='Trump')
df.loc[trump,'PredictIt_Oppo_No'] = df.loc[df['answer'] == 'Biden','bestBuyNoCost'].values
biden = (df['answer']=='Biden')
df.loc[biden,'PredictIt_Oppo_No'] = df.loc[df['answer'] == 'Trump','bestBuyNoCost'].values

# Create column of difference in betting odds & PredictIt
df['ari_mean_imp_prob-PredictIt_Yes'] = (df['ari_mean_imp_prob']-df['PredictIt_Yes']).round(2)

# Create column of difference in 538 & PredictIt
df['538-PredictIt_Yes'] = (df['538_model']-df['PredictIt_Yes']).round(2)

# Create column of difference in 538 & betting odds
df['538-ari_mean_imp_prob'] = (df['538_model']-df['ari_mean_imp_prob']).round(2)

# Create column of difference in 538 & Economist
df['538-Econ'] = (df['538_model']-df['Econ_model']).round(2)

# Print out select columns
print(df[['state', 
			'answer', 
			'538_latest_poll',
			'538_poll_date',
			#'pct_estimate',
			#'pct_trend_adjusted',
			'538_model',
			'Econ_model',
			'PredictIt_Yes',
			'PredictIt_Oppo_No',
			'betfair',
			'betfair_imp_prob',
			#'WilliamHill',
			#'WilliamHill_imp_prob',
			#'skybet_imp_prob',
			#'bet365_imp_prob',
			#'888sport_imp_prob',
			#'betvictor_imp_prob',
			#'paddypower_imp_prob',
			#'unibet_imp_prob',
			#'betfred_imp_prob',
			#'betway_imp_prob',
			#'10bet_imp_prob',
			#'BoyleSports_imp_prob',
			#'vbet_imp_prob',
			#'gentingbet_imp_prob', 
			#'SportPesa_imp_prob', 
			#'sportnation_imp_prob', 
			#'spreadex_imp_prob',
			#'betfairexchange_imp_prob',
			#'smarkets_imp_prob',
			#'novibet_imp_prob',
			#'matchbook_imp_prob',
			'ari_mean_imp_prob',
			'ari_mean_imp_prob-PredictIt_Yes',
			'538-PredictIt_Yes',
			'538-ari_mean_imp_prob',
			'538-Econ']])

# Write dataframe to CSV file in working directory
df.to_csv(r'C:/Users/Mick/Documents/Python/Python/predictit_538_odds/predictit_538_odds.csv', sep=',', encoding='utf-8', header='true')

# Write dataframe with timestamp to archive folder
snapshotdate = datetime.datetime.today().strftime('%Y-%m-%d')
os.chdir('C:/Users/Mick/Documents/Python/Python/predictit_538_odds/Archive')
df.to_csv(open(snapshotdate+'.csv','w'))
