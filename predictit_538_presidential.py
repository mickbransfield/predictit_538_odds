# TO DO
# 1. Clarify column names
# 2. Fix workaround in nan values for implied probability
# 3. Create key by parsing PredictIt presidential market/contracts
# 4. Fair probability

# Import modules
import json
import requests
import pandas as pd
pd.set_option('display.max_rows', None) #print all rows without truncating
import numpy as np

# Create unique key to link 538 polls with PredictIt markets
states = [[6212, 'Biden', 22223],   #AL
        [6212, 'Trump', 22222],     #AL
        [6211, 'Biden', 22094], #AK
        [6211, 'Trump', 22093], #AK
        [6214, 'Biden', 16000], #AZ
        [6214, 'Trump', 15999], #AZ
        [6215, 'Biden', 22160], #CA
        [6215, 'Trump', 22159], #CA
        [6216, 'Biden', 16018], #CO
        [6216, 'Trump', 16017], #CO
        [6217, 'Biden', 22081], #CT
        [6217, 'Trump', 22080], #CT
        [6219, 'Biden', 22264], #DE 
        [6219, 'Trump', 22263], #DE
        [6220, 'Biden', 15707], #FL
        [6220, 'Trump', 15706], #FL
        [6221, 'Biden', 16016], #GA
        [6221, 'Trump', 16015], #GA
        [6226, 'Biden', 22034], #IN
        [6226, 'Trump', 22033], #IN
        [6223, 'Biden', 16013], #IA
        [6223, 'Trump', 16014], #IA
        [6227, 'Biden', 22227], #KS
        [6227, 'Trump', 22226], #KS
        [6228, 'Biden', 22096], #KY
        [6228, 'Trump', 22095], #KY
        [6232, 'Biden', 22032], #ME
        [6232, 'Trump', 22031], #ME
        [8422, 'Biden', 22291], #ME CD-1
        [8422, 'Trump', 22290], #ME CD-1
        [8423, 'Biden', 19490], #ME CD-2
        [8423, 'Trump', 19489], #ME CD-2
        [6231, 'Biden', 22098], #MD
        [6231, 'Trump', 22097], #MD
        [6230, 'Biden', 22110], #MA
        [6230, 'Trump', 22109], #MA
        [6233, 'Biden', 15709], #MI
        [6233, 'Trump', 15708], #MI
        [6234, 'Biden', 16002], #MN
        [6234, 'Trump', 16001], #MN
        [6236, 'Biden', 22229], #MS
        [6236, 'Trump', 22228], #MS
        [6235, 'Biden', 22063], #MO
        [6235, 'Trump', 22062], #MO
        [6237, 'Biden', 22127], #MT
        [6237, 'Trump', 22126], #MT
        [8718, 'Biden', 22138], #NE CD-2
        [8718, 'Trump', 22137], #NE CD-2
        [6244, 'Biden', 16010], #NV
        [6244, 'Trump', 16009], #NV
        [6241, 'Biden', 16004], #NH
        [6241, 'Trump', 16003], #NH
        [6242, 'Biden', 22061], #NJ
        [6242, 'Trump', 22060], #NJ
        [6243, 'Biden', 22036], #NM
        [6243, 'Trump', 22035], #NM
        [6245, 'Biden', 22162], #NY
        [6245, 'Trump', 22161], #NY
        [6238, 'Biden', 16006], #NC
        [6238, 'Trump', 16005], #NC
        [6239, 'Biden', 22266], #ND
        [6239, 'Trump', 22265], #ND
        [6246, 'Biden', 16008], #OH
        [6246, 'Trump', 16007], #OH
        [6247, 'Biden', 22177], #OK
        [6247, 'Trump', 22176], #OK
        [6249, 'Biden', 15705], #PA
        [6249, 'Trump', 15704], #PA
        [6251, 'Biden', 22140], #SC
        [6251, 'Trump', 22139], #SC
        [6253, 'Biden', 22079], #TN
        [6253, 'Trump', 22078], #TN
        [6254, 'Biden', 16949], #TX
        [6254, 'Trump', 16948], #TX
        [6255, 'Biden', 22077], #UT
        [6255, 'Trump', 22076], #UT
        [6256, 'Biden', 16012], #VA
        [6256, 'Trump', 16011], #VA
        [6258, 'Biden', 22114], #WA
        [6258, 'Trump', 22113], #WA
        [6260, 'Biden', 22175], #WV
        [6260, 'Trump', 22174], #WV
        [6259, 'Biden', 15703], #WI
        [6259, 'Trump', 15702]]     #WI
 
# Create the pandas DataFrame key
df = pd.DataFrame(states, columns = ['race_id', 'answer', 'Contract_ID'])

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
predictit_df.columns=['Market_ID','Market_Name','Contract_ID','Contract_Name','bestBuyYesCost','bestBuyNoCost','BestSellYesCost','BestSellNoCost']

# Pull in polling data from 538
pres_polling = pd.read_csv('https://projects.fivethirtyeight.com/polls-page/president_polls.csv')
#pres_polling = pres_polling.dropna(subset=['state'])

# Drop extraneous columns
pres_polling = pres_polling.drop(['pollster_id', 'pollster','sponsor_ids','sponsors','display_name', 'pollster_rating_id', 'pollster_rating_name', 'fte_grade', 'sample_size', 'population', 'population_full', 'methodology', 'seat_number', 'seat_name', 'start_date', 'end_date', 'sponsor_candidate', 'internal', 'partisan', 'tracking', 'nationwide_batch', 'ranked_choice_reallocated', 'notes', 'url'], axis=1)

# Filter to most recent poll for Biden & Trump
pres_polling['created_at'] = pd.to_datetime(pres_polling['created_at']) #convert 'created_at' to datetime
recent_pres_polling = pres_polling.sort_values(by=['created_at']).drop_duplicates(['state', 'candidate_name'], keep='last')
recent_pres_polling = recent_pres_polling[recent_pres_polling['answer'].isin(['Biden', 'Trump'])]

# Pull in odds
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

# Merge key and 538 dataframes together
df = pd.merge(df, recent_pres_polling, on=['race_id', 'answer'])

# Merge key and PredicitIt
df = pd.merge(df, predictit_df, on=['Contract_ID'])

# Merge df and trump odds
df = pd.merge(df, odds_df, on=['state', 'answer'], how='left')

# Convert fractional odds to column of implied probability
# denominator / (denominator + numerator) = implied probability
df['numerator'], df['denominator'] = df['betfair'].str.split('/', 1).str
df['denominator'] = pd.to_numeric(df['denominator'], errors='coerce').fillna(0).astype(np.int64)
df['denominator'] = df['denominator'].mask(df['denominator']==0).fillna(1) # workaround
df['numerator'] = pd.to_numeric(df['numerator'], errors='coerce').fillna(0).astype(np.int64)
df['betfair_imp_prob'] = (df['denominator'] / (df['denominator'] + df['numerator']))

# workaround to fix previous workaround
mask = df['betfair'].isnull()
column_name = 'betfair_imp_prob'
df.loc[mask, column_name] = np.nan

# Create column of difference in betfair & PredictIt
df['betfair-PredicitIt'] = df['betfair_imp_prob']-df['bestBuyYesCost']

# Round 2 decimal places
df['betfair_imp_prob'] = df['betfair_imp_prob'].round(2)
df['betfair-PredicitIt'] = df['betfair-PredicitIt'].round(2)

#print out select columns
print(df[['state', 
            'answer', 
            'pct', 
            'bestBuyYesCost', 
            'betfair',
            'betfair_imp_prob',
            'betfair-PredicitIt']])

# Write dataframe to CSV file in working directory
df.to_csv(r'./predictit_538_odds.csv', sep=',', encoding='utf-8', header='true')