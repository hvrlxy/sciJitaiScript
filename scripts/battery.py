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
        self.DATA_PATH = '/opt/sci_jitai/'

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
        #get the year from the date
        year = int(date.split('-')[0])
        # search for the Common folder inside the logs-watch/date folder
        common_folder_path = '/home/hle5/sciJitaiScript/logs-watch/Common/'
        # check if the Common folder exists
        if not os.path.exists(common_folder_path):
            logger.error("get_battery_data(): Common folder not found in subject logs-watch folder")
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['timestamp', 'info', 'battery_level', 'charging'])

        # search for the Watch-GoalSettingsEMA.log.csv file inside the Common folder
        battery_file_path = common_folder_path + 'Watch-BatteryLogger.log.csv'
        # check if the Watch-GoalSettingsEMA.log.csv file exists
        if not os.path.exists(battery_file_path):
            logger.error("get_battery_data(): BatteryLogger file not found in Common folder")
            logger.error(traceback.format_exc())
            return battery_df
        
        # read the battery file into a pandas dataframe
        battery_df = pd.read_csv(battery_file_path, names=['timestamp', 'info', 'battery_level', 'charging'])
        # replace EDT to EST
        battery_df['timestamp'] = battery_df['timestamp'].apply(lambda x: x.rsplit(' ', 1)[0])
        battery_df['timestamp'] = battery_df['timestamp'].apply(lambda x: x.rsplit(' ', 1)[0])
        # turn the timestamp column into a datetime object
        battery_df['timestamp'] = pd.to_datetime(battery_df['timestamp'], format='%a %b %d %H:%M:%S')
        # set the year of the timestamp column
        battery_df['timestamp'] = battery_df['timestamp'].apply(lambda x: x.replace(year=year))
        #create epoch column
        battery_df['epoch'] = battery_df['timestamp'].astype(np.int64) // 10 ** 6
        # remove info and charging columns
        battery_df = battery_df.drop(['info', 'charging'], axis=1)
        return battery_df