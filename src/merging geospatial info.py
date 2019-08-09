

import pandas as pd
import pickle


rg = pd.read_pickle('data/rg.pkl')

get_ipython().run_line_magic('run', 'src/pipeline')
D = FraudData('data/data.json')
df = D.get_enriched_df()

loc = df[['venue_latitude','venue_longitude','object_id']]
loc = loc[~loc['venue_latitude'].isnull()]

loc2 = loc.join(rg)
loc3 = loc2.drop(['venue_latitude', 'venue_longitude'],axis = 1)
df_geo = df.merge(loc3, how='left', right_on = 'object_id', left_on = 'object_id')



