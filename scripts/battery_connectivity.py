import numpy as np
import pandas as pd
import datetime
from unzip_all import UnZip
import os
import logging
import traceback
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots as subplots

# get today's date as format YYYY-MM-DD
today = datetime.datetime.today().strftime('%Y-%m-%d')

logs_path = os.path.dirname(os.path.abspath(__file__)) + '/..' + '/logs/' + today

# create a logs folder of today's date if it doesn't exist
if not os.path.exists(logs_path):
    os.makedirs(logs_path)

# set up a logger for the schedule class
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# set the log file
log_file = logs_path + '/battery_connectivity.log'
# set the log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# set the log file handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

# put the file handler in the logger
logger.addHandler(file_handler)

class BatteryConnectivity:
    def __init__(self):
        # initialize the project root
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'

        # initialize the data path
        self.DATA_PATH = self.ROOT_DIR + '/data/raw/'

        # create a path to the reports folder
        self.REPORTS_PATH = self.ROOT_DIR + '/reports/'

        # initialize the schedule path
        self.SCHEDULE_PATH = self.ROOT_DIR + '/reports/battery/'

        # initialize a connectivity path
        self.CONNECTIVITY_PATH = self.ROOT_DIR + '/reports/connectivity/'

        # initialize a figure path
        self.FIGURE_PATH = self.ROOT_DIR + '/figures/'

        # initialize the unzip class
        self.unzip = UnZip()
    
    def get_battery_data(self, subject, date):
        '''
        unzip the raw auc file
        :return: None
        '''
        subject_full = subject + "@scijitai_com"
        # initialize the battery_df with columns: HEADER_TIME_STAMP,START_TIME,STOP_TIME,BATTERY_LEVEL,BATTERY_CHARGING
        battery_df = pd.DataFrame(columns=['HEADER_TIME_STAMP', 'START_TIME', 'STOP_TIME', 'BATTERY_LEVEL', 'BATTERY_CHARGING'])
        # unzip the raw auc file
        self.unzip.unzip_data_watch_folder(subject=subject, date=date)

        # initialize the data-watch path
        data_watch_path = self.DATA_PATH + subject_full + '/data-watch/' + date + '/'
        # get all folders in the data-watch path
        data_watch_folders = os.listdir(data_watch_path)
        # only grab folder that are not .zip 
        data_watch_folders = [folder for folder in data_watch_folders if not folder.endswith('.zip')]
        # loop through all the folders in the data-watch path
        for folder in data_watch_folders:
            try:
                # get all files in the folder
                files = os.listdir(data_watch_path + folder)
                # only grab the files that are csv
                csv_files = [file for file in files if file.endswith('.csv')]
                # loop through all the csv files
                for file in csv_files:
                    # read the csv file
                    df = pd.read_csv(data_watch_path + folder + '/' + file)
                    # get the battery_df
                    battery_df = battery_df.append(df, ignore_index=True)
            except Exception as e:
                logger.error('Error in unziping hourly foler: ' + str(e))
                logger.error(traceback.format_exc())

        # convert the HEADER_TIME_STAMP to datetime
        battery_df['HEADER_TIME_STAMP'] = pd.to_datetime(battery_df['HEADER_TIME_STAMP'])
        # sort the battery_df by HEADER_TIME_STAMP
        battery_df = battery_df.sort_values(by=['HEADER_TIME_STAMP'])
        # remove the other time columns
        battery_df = battery_df.drop(columns=['START_TIME', 'STOP_TIME'])
        # create a date folder if it doesn't exist
        if not os.path.exists(self.SCHEDULE_PATH + date + '/'):
            os.makedirs(self.SCHEDULE_PATH + date + '/')        

        # save the battery_df to a csv file
        battery_df.to_csv(self.SCHEDULE_PATH + date + '/' + subject + '.csv', index=False)
        return battery_df

    def plotting_battery(self, subject, date):
        '''
        plot the battery level
        :return: None
        '''
        # get the battery_df
        battery_df = self.get_battery_data(subject=subject, date=date)

        connectivity_df = self.get_connectivity_data(subject=subject, date=date)
        processed_df = self.process_connectivity_df(connectivity_df=connectivity_df)

        reboot_list = self.get_reboot_time(subject=subject, date=date)
        screen_on_list = self.get_screen_on_time(subject=subject, date=date)
        
        # plot the battery level in a plotly figure with under the area shaded
        fig = px.line(battery_df, x='HEADER_TIME_STAMP', y='BATTERY_LEVEL', title=f'Battery Level for {subject} on {date}')
        # set the range of the y axis
        fig.update_yaxes(range=[0, 120])

        # for each row in the processed_df
        for index, row in processed_df.iterrows():
            # if the connectivity is True
            if row['connectivity']:
                # add a vrect from start_time to end_time with color green
                fig.add_vrect(x0=row['start_time'], x1=row['end_time'], fillcolor="green", opacity=0.25, line_width=0)
            else:
                # add a vrect from start_time to end_time with color red
                fig.add_vrect(x0=row['start_time'], x1=row['end_time'], fillcolor="red", opacity=0.25, line_width=0)

        # for each reboot time, add a vertical rectangle with width = 0
        for reboot_time in reboot_list:
            fig.add_vrect(x0=reboot_time, x1=reboot_time, line_width=1.5, annotation_text="phone rebooted", 
                annotation_position="bottom right", annotation=dict(font_size=10, textangle=-90), line_dash='dash', 
                line_color='black')

        # add a rectangle around the vertical line
        fig.add_hrect(y0=110, y1=120, line_width=0, fillcolor="purple")
        # for each screen on time, add a vertical line with width = 0
        for screen_on_time in screen_on_list:
            fig.add_shape(type="line", x0=screen_on_time, y0=110, x1=screen_on_time, y1=120, 
            line=dict(color="yellow", width=1.5, dash='solid'), name = 'screen on')

        # add annotations text to the top right corner outside the plot with test = 'screen on' and the direction of the text is normal
        fig.add_annotation(x=1, y=1.1, xref='paper', yref='paper', text='screen on/off', showarrow=False, font=dict(size=10),
            textangle=0)
        # give the plot a title
        fig.update_layout(
            title_text=f'Battery Level for {subject} on {date}',
            height=400,
            width=1000,
            margin=dict(t=150)
            )
        # create a battery folder inside the figures folder if it doesn't exist
        if not os.path.exists(self.FIGURE_PATH + 'battery/' + date + '/'):
            os.makedirs(self.FIGURE_PATH + 'battery/' + date + '/')

        # save the figure in png and html format
        fig.write_image(self.FIGURE_PATH + 'battery/' + date + '/' + subject + '.png', scale = 5)
        fig.write_html(self.FIGURE_PATH + 'battery/' + date + '/' + subject + '.html')

    def get_connectivity_data(self, subject, date):
        connectivity_df = pd.DataFrame()
        # unzip the logs folder
        self.unzip.unzip_logs_folder(subject=subject, date=date)
        # initialize the logs path
        logs_path = self.DATA_PATH + subject + '@scijitai_com/logs/' + date + '/'
        # get all the folders in the logs path
        logs_folders = os.listdir(logs_path)
        # only grab the folders that are not .zip
        logs_folders = [folder for folder in logs_folders if not folder.endswith('.zip')]
        # loop through all the folders in the logs path
        for folder in logs_folders:
            # list all the files in the folder
            files = os.listdir(logs_path + folder)
            # only grab the file with the name ConnectivityManager.log.csv
            csv_files = [file for file in files if file == 'ConnectivityManager.log.csv']
            if len(csv_files) == 0:
                continue
            csv_files = csv_files[0]
            # read the csv file
            df = pd.read_csv(logs_path + folder + '/' + csv_files, names=['timestamp', 'info', 'subject', 'log', 'message'])
            # append the df to the connectivity_df
            connectivity_df = connectivity_df.append(df, ignore_index=True)
            
        # convert the timestamp to datetime
        connectivity_df['timestamp'] = connectivity_df['timestamp'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f %Z'))
        # trim the whitespace in the message column
        connectivity_df['message'] = connectivity_df['message'].str.strip()
        # split the message column by - and get the last element
        connectivity_df['message'] = connectivity_df['message'].str.split('-').str[-1]
        # drop every columns except timestamp and message
        connectivity_df = connectivity_df.drop(columns=['info', 'subject', 'log'])
        # rename the column message to connectivity
        connectivity_df = connectivity_df.rename(columns={'message': 'connectivity'})
        # create a connectivity folder inside the figures folder if it doesn't exist
        if not os.path.exists(self.CONNECTIVITY_PATH + date + '/'):
            os.makedirs(self.CONNECTIVITY_PATH + date + '/')

        # sort the connectivity_df by timestamp
        connectivity_df = connectivity_df.sort_values(by=['timestamp'])
        # for the connectivity column, trim the whitespace
        connectivity_df['connectivity'] = connectivity_df['connectivity'].str.strip()
        # turn the connectivity column to boolean if value = true
        connectivity_df['connectivity'] = connectivity_df['connectivity'].apply(lambda x: True if x == 'true' else False)

        # save the connectivity_df to a csv file
        connectivity_df.to_csv(self.CONNECTIVITY_PATH + date + '/' + subject + '.csv', index=False)
        return connectivity_df

    def process_connectivity_df(self, connectivity_df):
        '''
        process the connectivity_df to get the start and end time of each of the connection and disconnection
        :return: processed_df
        '''
        # create a new column called connectivity_change
        connectivity_df['connectivity_change'] = connectivity_df['connectivity'].shift(1) != connectivity_df['connectivity']
        # get the rows where the connectivity_change is True
        connectivity_change_df = connectivity_df[connectivity_df['connectivity_change'] == True]
        # create a new dataframe called processed_df
        processed_df = pd.DataFrame(columns = ['start_time', 'end_time', 'connectivity'])
        # reindex the connectivity_change_df from 1 to the length of the connectivity_change_df
        connectivity_change_df = connectivity_change_df.reset_index(drop=True)
        # loop through the connectivity_change_df
        for index, row in connectivity_change_df.iterrows():
            # get the start time
            start_time = row['timestamp']
            # get the connectivity
            connectivity = row['connectivity']
            # get the end time
            # if index is not the last row, get the next row's timestamp
            if index != len(connectivity_change_df) - 1:
                end_time = connectivity_change_df.iloc[index + 1]['timestamp']
            # else, end_time is the next day's 00:00:00
            else:
                current_date = start_time.date()
                end_time = datetime.datetime.combine(current_date + datetime.timedelta(days=1), datetime.time(0, 0, 0))
            # append the start_time, end_time, and connectivity to the processed_df
            processed_df = processed_df.append({'start_time': start_time, 'end_time': end_time, 'connectivity': connectivity}, ignore_index=True)
        return processed_df

    def get_reboot_time(self, subject, date):
        # unzip the logs folder
        self.unzip.unzip_logs_folder(subject=subject, date=date)
        # initialize the logs path
        logs_path = self.DATA_PATH + subject + '@scijitai_com/logs/' + date + '/'
        # get all the folders in the logs path
        logs_folders = os.listdir(logs_path)
        # only grab the folders that are not .zip
        logs_folders = [folder for folder in logs_folders if not folder.endswith('.zip')]
        reboot_list = []
        # loop through all the folders in the logs path
        for folder in logs_folders:
            # list all the files in the folder
            files = os.listdir(logs_path + folder)
            # only grab the file with the name SystemBroadcastReceiver.log.csv
            csv_files = [file for file in files if file == 'SystemBroadcastReceiver.log.csv']
            if len(csv_files) == 0:
                continue
            # read the csv file
            df = pd.read_csv(logs_path + folder + '/' + csv_files[0], names=['timestamp', 'info', 'subject', 'log', 'message'])
            # convert the timestamp to datetime
            df['timestamp'] = df['timestamp'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f %Z'))
            # filter out the rows with message contains the string REBOOT
            df = df[df['message'].str.contains('BOOT_COMPLETED')]
            # add the timestamp to the reboot_list
            reboot_list.extend(df['timestamp'].tolist())
        return reboot_list

    def get_screen_on_time(self, subject, date):
        # unzip the logs folder
        self.unzip.unzip_logs_watch_folder(subject=subject, date=date)
        # initialize the logs path
        logs_path = self.DATA_PATH + subject + '@scijitai_com/logs-watch/' + date + '/'
        # get all the folders in the logs path
        logs_folders = os.listdir(logs_path)
        # only grab the folders that are not .zip
        logs_folders = [folder for folder in logs_folders if not folder.endswith('.zip')]
        screen_on_list = []
        # loop through all the folders in the logs path
        for folder in logs_folders:
            # list all the files in the folder
            try:
                files = os.listdir(logs_path + folder)
            except Exception as e:
                continue
            # only grab the file with the name SystemBroadcastReceiver.log.csv
            csv_files = [file for file in files if file == 'Watch-EMAAlwaysOnService.log.csv']
            if len(csv_files) == 0:
                continue
            # read the csv file
            df = pd.read_csv(logs_path + folder + '/' + csv_files[0], names=['timestamp', 'info', 'message'])
            # convert the timestamp to datetime
            df['timestamp'] = df['timestamp'].apply(lambda x: datetime.datetime.strptime(x, "%a %b %d %H:%M:%S %Z %Y"))
            # print(df)
            # filter out the rows with message contains the string REBOOT
            df = df[df['message'].str.contains('SCREEN_ON')]
            # add the timestamp to the reboot_list
            screen_on_list.extend(df['timestamp'].tolist())
        return screen_on_list

test = BatteryConnectivity()
test.plotting_battery(subject='user06', date='2023-05-19')