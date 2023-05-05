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

# plot the battery level
fig = px.line(df, x='timestamp', y='battery', title='Battery Testing Results')
# create an opaque rectangle to represent the time when the watch-phone is connected (filter rows where watch-phone connected is true)
fig.add_shape(type="rect", xref="x", yref="paper", x0='2023-04-15 22:30:00', y0=0, x1='2023-04-16 04:30:00', y1=1, fillcolor="LightSkyBlue", opacity=0.5, layer="below", line_width=0)
# create an opaque rectangle to represent the time when the phone is connected to wifi (filter rows where phone_wifi_connected is true)
fig.add_shape(type="rect", xref="x", yref="paper", x0='2023-04-15 19:30:00', y0=0, x1='2023-04-16 01:30:00', y1=1, fillcolor="LightSalmon", opacity=0.5, layer="below", line_width=0)
# set height and width of the figure
fig.update_layout(height=500, width=1000)

# add annotations on the top, outside the plot to show the time when the watch-phone is connected and the phone is connected to wifi
fig.add_annotation(x='2023-04-15 22:30:00', y=1.1, xref="x", yref="paper", text="Watch-Phone Connected", showarrow=False, font=dict(size=10))
fig.add_annotation(x='2023-04-15 22:30:00', y=1.05, xref="x", yref="paper", text="Phone Wifi Disconnected", showarrow=False, font=dict(size=10))

fig.add_annotation(x='2023-04-15 19:30:00', y=1.05, xref="x", yref="paper", text="Phone Wifi Disconnected", showarrow=False, font=dict(size=10))
fig.add_annotation(x='2023-04-15 19:30:00', y=1.1, xref="x", yref="paper", text="Watch-Phone Disconnected", showarrow=False, font=dict(size=10))

fig.add_annotation(x='2023-04-16 01:30:00', y=1.05, xref="x", yref="paper", text="Phone Wifi Connected", showarrow=False, font=dict(size=10))
fig.add_annotation(x='2023-04-16 01:30:00', y=1.1, xref="x", yref="paper", text="Watch-Phone Connected", showarrow=False, font=dict(size=10))

# add a dotted line at 22:30pm April 15 to show the time when the watch-phone is connected
fig.add_shape(type="line", xref="x", yref="y", x0='2023-04-15 22:30:00', y0=60, x1='2023-04-15 22:30:00', y1=100, line=dict(color="black", width=2, dash="dot"))
# add a dotted line at 01:30 April 16 to show the time when the phone is connected to wifi
fig.add_shape(type="line", xref="x", yref="y", x0='2023-04-16 01:30:00', y0=60, x1='2023-04-16 01:30:00', y1=100, line=dict(color="black", width=2, dash="dot"))

# add battery level annotation on 3 points, 7:30pm April 15, 10:30pm April 15, 1:30am April 16 on top of the line
fig.add_annotation(x='2023-04-15 19:30:00', y=93, xref="x", yref="y", text="92%", showarrow=False, font=dict(size=12))
fig.add_annotation(x='2023-04-15 22:30:00', y=93, xref="x", yref="y", text="92%", showarrow=False, font=dict(size=12))
fig.add_annotation(x='2023-04-16 01:30:00', y=83, xref="x", yref="y", text="82%", showarrow=False, font=dict(size=12))
fig.add_annotation(x='2023-04-16 04:30:00', y=73, xref="x", yref="y", text="70%", showarrow=False, font=dict(size=12))

print(ROOT + "/battery.png")
# save the plot to a png file, increase the resolution to 300 dpi
fig.write_image(ROOT + "/battery.png", scale=5)