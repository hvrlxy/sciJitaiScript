import numpy as np
import pandas as pd
import datetime
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots as subplots
from unzip_all import unzip_logs_watch_folder
import os
import logging
import traceback
import warnings
warnings.filterwarnings("ignore")

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
log_file = logs_path + '/plot_subject.log'
# set the log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# set the log file handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

# put the file handler in the logger
logger.addHandler(file_handler)

class PlotSubject:
    def __init__(self):
        # initialize the project root
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'

        # initialize the data path
        self.DATA_PATH = self.ROOT_DIR + '/data/raw/'

        # initialize the schedule path
        self.FIGURES_PATH = self.ROOT_DIR + '/figures/auc/'

    def impute_auc_df(self, df):
        '''
        impute the auc dataframe with NANs value in between datapoints 1 minutes apart
        :param df: pd.DataFrame
        :return: pd.DataFrame
        '''
        
        # sort the dataframe by the time colume
        df.sort_values(by=['epoch'], inplace=True)
        # reset the index
        df.reset_index(inplace=True)
        # drop the old index column
        df.drop(columns='index', inplace=True)
        # look at the time colume of the df dataframe with the format of %H:%M:%S
        # for every consecutive time, if the difference between the two times is more than 20 seconds,
        # append a new row to the dataframe with the time in between the two times and the auc values
        # to be NaN
        for i in range(len(df) - 1):
            time1 = df['epoch'][i]
            time2 = df['epoch'][i + 1]
            time_diff = time2 - time1
            if time_diff > 60 * 1000:
                # print(time1, time2, 1)
                # keep increamenting the time by 10 seconds until the time is equal or greater to the time2
                while time1 < time2:
                    time1 = time1 + 60 * 1000
                    # if the time is equal to the time2, break the loop
                    if time1 >= time2:
                        break
                    # append the new row to the dataframe, in between the two times
                    df.loc[len(df)] = [time1, np.nan]
                    # sort the dataframe by the time colume
                    df.sort_values(by=['epoch'], inplace=True)
                    # reset the index
                    df.reset_index(inplace=True)
                    # drop the old index column
                    df.drop(columns='index', inplace=True)
        # return the dataframe and the total number of data points
        return df

    def read_auc_df(self, subject, day):
        '''
        process the auc data
        :param subject: str
        :param day: str
        :return: pd.DataFrame
        '''
        subject_full = subject + '@scijitai_com'
        # check if the day format is YYYY-MM-DD by converting it to datetime
        try:
            datetime.datetime.strptime(day, '%Y-%m-%d')
        except ValueError:
            logger.error("read_auc_df(): Incorrect data format, should be YYYY-MM-DD")
            raise ValueError("read_auc_df(): Incorrect data format, should be YYYY-MM-DD")

        # check if the subject is in the data/raw folder
        if subject_full not in os.listdir(self.DATA_PATH):
            logger.error("read_auc_df(): Subject not found in data/raw folder")
            raise ValueError("read_auc_df(): Subject not found in data/raw folder")

        # check if the day is in the subject folder
        if day not in os.listdir(self.DATA_PATH + subject_full + '/logs-watch/'):
            logger.error("read_auc_df(): Day not found in subject logs-watch folder")
            raise ValueError("read_auc_df(): Day not found in subject logs-watch folder")

        # unzipping the logs-watch folder
        try:
            unzip_logs_watch_folder(subject, day)
        except Exception as e:
            logger.error("read_auc_df(): Error unzipping logs-watch folder")
            logger.error(traceback.format_exc())
            print(traceback.format_exc())
            raise ValueError(f'Error unzipping {subject} on {day}')
        logger.info(f"read_auc_df(): Unzipping logs-watch folder for {subject} on {day} done")

        # get all the non-zip files in the logs-watch folder's day folder
        files = [f for f in os.listdir(self.DATA_PATH + subject_full + '/logs-watch/' + day) if
                 f.endswith('.zip') == False]
        # sort the files
        files.sort()

        auc_df = pd.DataFrame(columns=['timestamp', 'type', 'epoch', 'sr', 'x', 'y', 'z'])

        for folder in files:
            # check if the file Watch-AccelSampling.log.csv exists
            if not os.path.exists(self.DATA_PATH + subject_full + '/logs-watch/' + day + '/' + folder + '/Watch-AccelSampling.log.csv'):
                logger.error(f"read_auc_df(): Watch-AccelSampling.log.csv not found in logs-watch {folder}")
                continue
            # read the Watch-AccelSampling.log.csv file
            df = pd.read_csv(self.DATA_PATH + subject_full + '/logs-watch/' + day + '/' + folder + '/Watch-AccelSampling.log.csv',
                             header=None, names=['timestamp', 'type', 'epoch', 'sr', 'x', 'y', 'z'])

            auc_df = auc_df.append(df)

        # add up the x, y, z columns
        auc_df['AUC'] = auc_df['x'] + auc_df['y'] + auc_df['z']
        # only takes the epoch and AUC columns
        auc_df = auc_df[['epoch', 'AUC']]
        # sort the dataframe by epoch
        auc_df = auc_df.sort_values(by=['epoch'])
        auc_df = self.impute_auc_df(auc_df)
        return auc_df

    def read_pa_df(self, subject, day):
        '''
        process the pa data
        :param subject: str
        :param day: str
        :return: pd.DataFrame
        '''

        subject_full = subject + '@scijitai_com'
        # check if the day format is YYYY-MM-DD by converting it to datetime
        try:
            datetime.datetime.strptime(day, '%Y-%m-%d')
        except ValueError:
            logger.error("read_pa_df(): Incorrect data format, should be YYYY-MM-DD")
            raise ValueError("read_pa_df(): Incorrect data format, should be YYYY-MM-DD")

        # check if the subject is in the data/raw folder
        if subject_full not in os.listdir(self.DATA_PATH):
            logger.error("read_pa_df(): Subject not found in data/raw folder")
            raise ValueError("read_pa_df(): Subject not found in data/raw folder")

        # check if the day is in the subject folder
        if day not in os.listdir(self.DATA_PATH + subject_full + '/logs-watch/'):
            logger.error("read_pa_df(): Day not found in subject logs-watch folder")
            raise ValueError("read_pa_df(): Day not found in subject logs-watch folder")

        # unzipping the logs-watch folder
        try:
            unzip_logs_watch_folder(subject, day)
        except Exception as e:
            logger.error("read_pa_df(): Error unzipping logs-watch folder")
            logger.error(traceback.format_exc())
            print(traceback.format_exc())
            raise ValueError(f'Error unzipping {subject} on {day}')
        logger.info(f"read_pa_df(): Unzipping logs-watch folder for {subject} on {day} done")

        # get all the non-zip files in the logs-watch folder's day folder
        files = [f for f in os.listdir(self.DATA_PATH + subject_full + '/logs-watch/' + day) if
                 f.endswith('.zip') == False]
        # sort the files
        files.sort()

        pa_df = pd.DataFrame(columns=['timestamp', 'type', 'pa'])

        for folder in files:
            # check if the file Watch-PAMinutes.log.csv exists
            if not os.path.exists(self.DATA_PATH + subject_full + '/logs-watch/' + day + '/' + folder + '/Watch-PAMinutes.log.csv'):
                logger.error(f"read_pa_df(): Watch-PAMinutes.log.csv not found in logs-watch {folder}")
                continue
            
            # read the Watch-PAMinutes.log.csv file
            df = pd.read_csv(self.DATA_PATH + subject_full + '/logs-watch/' + day + '/' + folder + '/Watch-PAMinutes.log.csv',
                                header=None, names=['timestamp', 'type', 'pa'])

            pa_df = pa_df.append(df)
        
        # only takes the timestamp and pa columns
        pa_df = pa_df[['timestamp', 'pa']]

        # convert the timestamp to datetime from string
        pa_df['timestamp'] = pd.to_datetime(pa_df['timestamp'], format='%a %b %d %H:%M:%S %Z %Y')
        # sort the pa_df by timestamp
        pa_df = pa_df.sort_values(by=['timestamp'])
        return pa_df

    def plot_subject(self, subject, day):
        '''
        plot the subject's data
        :param subject: str
        :param day: str
        :return: None
        '''

        # get the auc_df
        auc_df = self.read_auc_df(subject, day)
        # get the pa_df
        pa_df = self.read_pa_df(subject, day)

        # convert the timestamp to datetime from epoch milliseconds
        auc_df['epoch'] = pd.to_datetime(auc_df['epoch'], unit='ms')
        # convert to EST timezone
        auc_df['epoch'] = auc_df['epoch'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

        # create a plotly plot with 4 subplots
        fig = go.Figure()
        fig = subplots.make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05, subplot_titles=("AUC", "Minutes of PA"))

        # add the auc_df to the plot
        fig.add_trace(go.Scatter(x=auc_df['epoch'], y=auc_df['AUC'], mode='lines', name='AUC'), row=1, col=1)
        # add the pa_df to the plot
        fig.add_trace(go.Scatter(x=pa_df['timestamp'], y=pa_df['pa'], mode='lines', name='PA'), row=2, col=1)

        # update the layout
        fig.update_layout(
            title=f"Subject {subject} on {day}",
            height=1000,
            width=1200
        )

        # show the plot
        # fig.show()

        # check if the date folder exists in the figures folder
        if not os.path.exists(self.FIGURES_PATH + day):
            # create the date folder
            os.mkdir(self.FIGURES_PATH + day)
            logger.info(f"plot_subject(): Created {day} folder in figures folder")
        # save the plot
        fig.write_html(self.FIGURES_PATH + day + '/' + subject + '.html')
        logger.info(f"plot_subject(): Saved {subject}.html plot in {day} folder")
        fig.write_image(self.FIGURES_PATH + day + '/' + subject + '.png')
        logger.info(f"plot_subject(): Saved {subject}.html plot in {day} folder")
        

test = PlotSubject()
test.plot_subject('user01', '2023-01-26')