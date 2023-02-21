import numpy as np
import pandas as pd
import datetime
from unzip_all import UnZip
import os
import logging
import traceback
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
log_file = logs_path + '/pabouts.log'
# set the log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# set the log file handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

# put the file handler in the logger
logger.addHandler(file_handler)

class PAbouts:
    def __init__(self, subject, date):
        self.subject = subject
        self.subject_full = subject + '@scijitai_com'
        self.date = date

        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/../'

        self.LOGS_WATCH_PATH = self.ROOT_DIR + "data/raw/" + self.subject_full + '/logs-watch/' + self.date

    def retrieve_auc_data(self):
        unzip = UnZip()
        unzip.unzip_logs_watch_folder(self.subject, self.date)

        auc_df = pd.DataFrame(columns=['timestamp', 'type', 'epoch', 'sr', 'x', 'y', 'z'])

        # get all the non-zip files in the logs-watch folder's day folder
        files = [f for f in os.listdir(self.LOGS_WATCH_PATH) if not f.endswith('.zip')]
        # sort the files
        files.sort()
        for folder in files:
            # check if the file Watch-AccelSampling.log.csv exists
            if not os.path.exists(self.LOGS_WATCH_PATH + '/' + folder + '/Watch-AccelSampling.log.csv'):
                logger.error(f"read_auc_df(): Watch-AccelSampling.log.csv not found in logs-watch {folder}")
                continue
            # read the Watch-AccelSampling.log.csv file
            df = pd.read_csv(self.LOGS_WATCH_PATH + '/' + folder + '/Watch-AccelSampling.log.csv',
                                header=None, names=['timestamp', 'type', 'epoch', 'sr', 'x', 'y', 'z', 'unknown', 'unknown2', 'unknown3', 'unknown4'])
            #remove all the rows with unknown values not NaN
            df = df[df['unknown'].isna()]

            auc_df = auc_df.append(df)

        # sort by epoch
        auc_df = auc_df.sort_values(by=['epoch'])
        auc_df['AUCsum'] = auc_df['x'] + auc_df['y'] + auc_df['z']
        # remove all columns except epoch, and AUCsum
        auc_df = auc_df[['epoch', 'AUCsum']]

        return auc_df

    def calculate_PA(self):
        auc_df = self.retrieve_auc_data()

        # initalize the final df
        final_df = pd.DataFrame(columns=['epoch', 'AUCsum', 'PA'])

        last_epoch = auc_df.iloc[0]['epoch']
        current_PA = 0
        auc_list = []
        threshold = 2000
        last_detected_epoch = 0
        last_detection_run = auc_df.iloc[0]['epoch']
        bout3 = 0
        bout1 = 0
        # loop through the rows of the df
        for index, row in auc_df.iterrows():
            # if auc_list is empty, append the auc value to the list
            if not auc_list:
                auc_list.append((row['epoch'], row['AUCsum']))
                continue
            # check if last detection_run and current epoch is in the same hour (EDT timezone)
            if datetime.datetime.fromtimestamp(last_detection_run / 1000).hour != datetime.datetime.fromtimestamp(row['epoch'] / 1000).hour:
                # if not, reset the last_detection_run to the current epoch
                last_detection_run = row['epoch']
            # if the current time is 1.5 minutes in millisecond after the last_detection_run, run the detection algorithm
            if row['epoch'] - last_detection_run >= 90000:
                # calculate the number of data points below the threshold
                below_threshold = len([auc for auc in auc_list if auc[1] < threshold])
                if below_threshold < 4 and len(auc_list) >= 18:
                    if row['epoch'] - last_detected_epoch >= 180000:
                        bout3 += 1
                        current_PA += 3
                    else:
                        bout1 += 1
                        current_PA += 1.5

                    last_detected_epoch = row['epoch']
                # set the last_detection_run to the current epoch
                last_detection_run = row['epoch']
                # append the row to the final df
                final_df = final_df.append([{'epoch': row['epoch'], 'AUCsum': row['AUCsum'], 'PA': current_PA}])
            # append the auc value to the auc_list
            auc_list.append((row['epoch'], row['AUCsum']))
            # remove all the auc values that are older than 3 minutes
            auc_list = [auc for auc in auc_list if row['epoch'] - auc[0] <= 180000]

        daily_PA = final_df.iloc[-1]['PA']
        return final_df, daily_PA





