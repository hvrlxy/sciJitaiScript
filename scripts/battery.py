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
log_file = logs_path + '/battery.log'
# set the log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# set the log file handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

# put the file handler in the logger
logger.addHandler(file_handler)

class Battery:
    def __init__(self):
        # initialize the project root
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'

        # initialize the data path
        self.DATA_PATH = self.ROOT_DIR + '/data/raw/'

        # initialize the schedule path
        self.SCHEDULE_PATH = self.ROOT_DIR + '/reports/battery/'

        # initialize a figure path
        self.FIGURE_PATH = self.ROOT_DIR + '/figures/'

        # initialize the unzip class
        self.unzip = UnZip()
    
    def unzip_raw_auc_file(self, subject, date):
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
        battery_df = self.unzip_raw_auc_file(subject=subject, date=date)
        
        # plot the battery level in a plotly figure with under the area shaded
        fig = px.area(
            battery_df, 
            x='HEADER_TIME_STAMP', 
            y='BATTERY_LEVEL', 
            color='BATTERY_CHARGING',
            color_discrete_map={
                'YES': 'green',
                'NO': 'red'
            }
        )
        # set the range of the y axis
        fig.update_yaxes(range=[0, 120])
        # give the plot a title
        fig.update_layout(
            title_text=f'Battery Level for {subject} on {date}',
            height=400,
            width=1000
            )
        # create a battery folder inside the figures folder if it doesn't exist
        if not os.path.exists(self.FIGURE_PATH + 'battery/' + date + '/'):
            os.makedirs(self.FIGURE_PATH + 'battery/' + date + '/')

        # save the figure in png and html format
        fig.write_image(self.FIGURE_PATH + 'battery/' + date + '/' + subject + '.png')
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
            csv_files = [file for file in files if file == 'ConnectivityManager.log.csv'][0]
            # read the csv file
            df = pd.read_csv(logs_path + folder + '/' + csv_files, names=['timestamp', 'info', 'subject', 'log', 'message'])
            # append the df to the connectivity_df
            connectivity_df = connectivity_df.append(df, ignore_index=True)
            
        # convert the timestamp to datetime
        connectivity_df['timestamp'] = pd.to_datetime(connectivity_df['timestamp'], format='%Y-%m-%d %H:%M:%S.%f EST')
        # trim the whitespace in the message column
        connectivity_df['message'] = connectivity_df['message'].str.strip()
        # split the message column by - and get the last element
        connectivity_df['message'] = connectivity_df['message'].str.split('-').str[-1]
        # drop every columns except timestamp and message
        connectivity_df = connectivity_df.drop(columns=['info', 'subject', 'log'])
        # rename the column message to connectivity
        connectivity_df = connectivity_df.rename(columns={'message': 'connectivity'})
        return connectivity_df
# test
test = Battery()
test.plotting_battery(subject='user01', date='2023-02-03')
# print(
# test.get_connectivity_data(subject='user01', date='2023-02-05')
# )

    