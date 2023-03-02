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

    def calculate_PA(self, epoch_list, threshold = 2000):
        auc_df = self.retrieve_auc_data()

        is_beginning = True 
        daily_PA = 0
        final_df = pd.DataFrame(columns=['epoch', 'PA'])
        for epoch in epoch_list:
            # filter out the rows that is within 3 minutes after the epoch (in milliseconds)
            epoch_df = auc_df[(auc_df['epoch'] >= epoch) & (auc_df['epoch'] <= epoch + 181000)]
            total_samples = epoch_df.shape[0]
            # count the number of rows that has AUCsum > threshold
            num_samples_above_threshold = epoch_df[epoch_df['AUCsum'] > threshold].shape[0]
            # check if the percentage of samples above threshold is greater than 75%
            if total_samples == 0:
                is_pa = False
            else:
                is_pa = num_samples_above_threshold / total_samples >= 0.75
            # if is_beginning is True, then add 3 minutes to the daily_PA
            if is_beginning and is_pa:
                daily_PA += 3
                is_beginning = False
            elif is_pa:
                daily_PA += 1.5
            else:
                is_beginning = True
            # add a new row to the final_df
            final_df = final_df.append({'epoch': epoch, 'PA': daily_PA}, ignore_index=True)
        return final_df, daily_PA





