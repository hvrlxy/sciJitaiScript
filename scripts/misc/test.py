import numpy as np
import pandas as pd
import datetime
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots as subplots
import os
import warnings
warnings.filterwarnings("ignore")

# initialize the root path of the project and the dataset path
# root is 3 parent directories above the current directory
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATASET_PATH = os.path.join(ROOT, 'data', 'filtered', 'battery.csv')

# read the data with 3 columns: timestamp, info, battery and is_charging
df = pd.read_csv(DATASET_PATH, names=['timestamp', 'info', 'battery', 'is_charging'])
# convert timestamp to string
df['timestamp'] = df['timestamp'].astype(str)
# convert battery to float
df['battery'] = df['battery'].astype(float)

# get the first item in the timestamp column, and convert it to datetime
first_item = df['timestamp'][0]
print(datetime.datetime.strptime(first_item, "%a %b %d %H:%M:%S %Z %Y"))
# convert the timestamp column to datetime, with format %a %b %d %H:%M:%S %Z %Y
df['timestamp'] = df['timestamp'].apply(lambda x: datetime.datetime.strptime(x, "%a %b %d %H:%M:%S %Z %Y"))

df['timestamp'] = pd.to_datetime(df['timestamp'], format="%a %b %d %H:%M:%S %Z %Y")
# only keep the timestmp between 7:30pm April 15 and 4:30 am April 16
df = df[(df['timestamp'] >= '2023-04-15 19:30:00')  & (df['timestamp'] <= '2023-04-16 04:30:00')]
df = df.reset_index(drop=True)
# create a new column called watch-phone connected, and set every thing to true except for 7:30-10:30pm April 15
df['watch-phone connected'] = True
df.loc[(df['timestamp'] >= '2023-04-15 19:30:00') & (df['timestamp'] <= '2023-04-15 22:30:00'), 'watch-phone connected'] = False

#create a new column called phone_wifi_connected, and set every thing to true except for 10:30pm April 15 to 1:30am April 16
df['phone_wifi_connected'] = True
df.loc[(df['timestamp'] >= '2023-04-15 22:30:00') & (df['timestamp'] <= '2023-04-16 01:30:00'), 'phone_wifi_connected'] = False

# group the data into 3 periods: 7:30pm-10:30pm, 10:30pm-1:30am, and 1:30am-4:30am
period1 = df[(df['timestamp'] >= '2023-04-15 19:30:00') & (df['timestamp'] <= '2023-04-15 22:30:00')]
period2 = df[(df['timestamp'] >= '2023-04-15 22:30:00') & (df['timestamp'] <= '2023-04-16 01:30:00')]
period3 = df[(df['timestamp'] >= '2023-04-16 01:30:00') & (df['timestamp'] <= '2023-04-16 04:30:00')]

#split the data into 3 time periods
df_1 = df[(df['timestamp'] >= '2023-04-15 19:30:00') & (df['timestamp'] <= '2023-04-15 22:30:00')]
df_2 = df[(df['timestamp'] >= '2023-04-15 22:30:00') & (df['timestamp'] <= '2023-04-16 01:30:00')]
df_3 = df[(df['timestamp'] >= '2023-04-16 01:30:00') & (df['timestamp'] <= '2023-04-16 04:30:00')]

#create subplots with 3 rows and 1 column
fig = subplots.make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05)

#plot battery level for each time period
fig.add_trace(go.Scatter(x=df_1['timestamp'], y=df_1['battery'], name='Battery Level 1'), row=1, col=1)
fig.add_trace(go.Scatter(x=df_2['timestamp'], y=df_2['battery'], name='Battery Level 2'), row=2, col=1)
fig.add_trace(go.Scatter(x=df_3['timestamp'], y=df_3['battery'], name='Battery Level 3'), row=3, col=1)

#fit a linear regression line for each time period
m1, b1 = np.polyfit(df_1.index, df_1['battery'], 1)
m2, b2 = np.polyfit(df_2.index, df_2['battery'], 1)
m3, b3 = np.polyfit(df_3.index, df_3['battery'], 1)

#add regression lines to the plot
fig.add_trace(go.Scatter(x=df_1['timestamp'], y=m1*df_1.index+b1, mode='lines', name='Regression Line 1'), row=1, col=1)
fig.add_trace(go.Scatter(x=df_2['timestamp'], y=m2*df_2.index+b2, mode='lines', name='Regression Line 2'), row=2, col=1)
fig.add_trace(go.Scatter(x=df_3['timestamp'], y=m3*df_3.index+b3, mode='lines', name='Regression Line 3'), row=3, col=1)

#update the layout
fig.update_layout(title='Battery Testing Results', height=800)

#show the plot
fig.show()