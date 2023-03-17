import numpy as np
import pandas as pd
import datetime
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots as subplots
from unzip_all import UnZip
from schedule import Schedule
import os
import logging
import traceback
import warnings
from prompts import Prompts
from pa_algorithm import PAbouts
from battery import Battery
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

        # initialize the unzip class
        self.unzip = UnZip()

        # initiahe the schedule class
        self.schedule = Schedule()

        # initialize the battery class
        self.battery = Battery()

    def impute_auc_df(self, df):
        '''
        impute the auc dataframe with NANs value in between datapoints 1 minutes apart
        :param df: pd.DataFrame
        :return: pd.DataFrame
        '''
        # print(df)
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
            self.unzip.unzip_logs_watch_folder(subject, day)
        except Exception as e:
            logger.error("read_auc_df(): Error unzipping logs-watch folder")
            logger.error(traceback.format_exc())
            # print(traceback.format_exc())
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
        # remove NaN values
        auc_df = auc_df.dropna()
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
            self.unzip.unzip_logs_watch_folder(subject, day)
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
            
            try:
                # read the Watch-PAMinutes.log.csv file
                df = pd.read_csv(self.DATA_PATH + subject_full + '/logs-watch/' + day + '/' + folder + '/Watch-PAMinutes.log.csv',
                                    header=None, names=['timestamp', 'type', 'epoch', 'pa'])
            except Exception as e:
                continue
            pa_df = pa_df.append(df)
        
        # only takes the timestamp and pa columns
        pa_df = pa_df[['epoch', 'pa']]
        # pa_df = self.process_pa_df(pa_df)
        # sort the pa_df by timestamp
        pa_df = pa_df.sort_values(by=['epoch'])
        # add 3 minutes to the epoch time
        # pa_df['epoch'] = pa_df['epoch'].apply(lambda x: x + 180000)
        # turn the epoch milliseconds to datetime
        pa_df['timestamp'] = pd.to_datetime(pa_df['epoch'], unit='ms')
        # get the epoch list 
        epoch_list = pa_df['epoch'].tolist()
        # remove epoch column
        pa_df = pa_df.drop(columns=['epoch'])
        # convert to EDT timezone
        pa_df['timestamp'] = pa_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        # remove NaN values
        pa_df = pa_df.dropna()
        return pa_df, epoch_list

    def process_pa_df(self, pa_df):
        '''
        process the pa_df
        :param pa_df: pd.DataFrame
        :return: pd.DataFrame
        '''

        # convert the timestamp to epoch time in milliseconds
        pa_df['timestamp'] = pa_df['timestamp'].apply(lambda x: int(x.timestamp() * 1000))
        # save the df to a test file
        pa_df.to_csv(self.ROOT_DIR + '/test.csv')
        # loop through the pa_df
        for index, row in pa_df.iterrows():
            # if index is 0, skip
            if index == 0:
                continue
            # if the current epoch is the same as the previous epoch, increment the epoch time of the current row by 1
            if row['timestamp'] == pa_df.iloc[index - 1]['timestamp']:
                pa_df.at[index, 'timestamp'] += 30000
        # convert the epoch time to datetime in EST
        pa_df['timestamp'] = pa_df['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000))
        # convert to EDT
        pa_df['timestamp'] = pa_df['timestamp'].apply(lambda x: x - datetime.timedelta(hours=4))
        return pa_df

    def retrieve_prompts_df(self, subject, day):
        prompt_obj = Prompts()
        try:
            all_messages = prompt_obj.read_all_message_df(subject, day)
            return all_messages
        except Exception as e:
            logger.error(f"retrieve_prompts_df(): No prompts on day {day} for {subject}")
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['timestamp', 'epoch', 'message_type'])

    def get_auc_summary(self, auc_df):
        # get the minimum timestamp
        start_timestamp = auc_df['epoch'].min()
        # get the final timestamp (index - 1)
        end_timestamp = auc_df['epoch'].max()

        # calculate the number of minutes in between
        minutes = (end_timestamp - start_timestamp) / 60000
        # total of samples is minutes * 6 (10 seconds)
        total_samples = minutes * 6
        #get the number of samples in the auc_df
        samples = auc_df.shape[0]
        #calculate the percentage of samples
        percentage = samples / total_samples * 100
        # return 3 values
        return total_samples, samples, percentage

    def plot_subject(self, subject, day, show = False):
        '''
        plot the subject's data
        :param subject: str
        :param day: str
        :return: None
        '''

        # get the auc_df
        auc_df = self.read_auc_df(subject, day)
        # get the pa_df
        try:
            pa_df, epoch_list = self.read_pa_df(subject, day)
        except Exception as e:
            pa_df = pd.DataFrame(columns=['timestamp', 'pa'])
            # get the start timestamp of the day in pa_df
            start_timestamp = auc_df['epoch'].min()
            # get the end timestamp of the day in auc_df
            end_timestamp = auc_df['epoch'].max()
            #append 2 rows to offline_df
            pa_df = pa_df.append({'timestamp': start_timestamp, 'pa': 0}, ignore_index=True)
            pa_df = pa_df.append({'timestamp': end_timestamp, 'pa': 0}, ignore_index=True)
            epoch_list = []

        # get the total samples, samples, and percentage
        total_samples, samples, percentage = self.get_auc_summary(auc_df)
        # convert the timestamp to datetime from epoch milliseconds
        auc_df['epoch'] = pd.to_datetime(auc_df['epoch'], unit='ms')
        # convert to EST timezone
        auc_df['epoch'] = auc_df['epoch'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

        # get the prompts_df
        prompts_df = self.retrieve_prompts_df(subject, day)
        # convert the epoch to datetime from epoch milliseconds
        prompts_df['epoch'] = pd.to_datetime(prompts_df['epoch'], unit='ms')
        # convert to EST timezone
        prompts_df['epoch'] = prompts_df['epoch'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

        # get the schedule_df
        try:
            schedule_df = self.schedule.process_schedule_generation(subject, day)
            # get the first item in the week column
            week = schedule_df['week'].values[0]
            # get the first item in the day column
            dayIndex = schedule_df['day'].values[0]
            # get message_note from message_type goal_settings
            goal_type = schedule_df[schedule_df['message_type'] == 'goal_settings']['message_note'].values[0]
            # get message_note from message_type first_jitai
            jitai1 = schedule_df[schedule_df['message_type'] == 'first_jitai']['message_note'].values[0]
            # get message_note from message_type second_jitai
            jitai2 = schedule_df[schedule_df['message_type'] == 'second_jitai']['message_note'].values[0]
            # convert the start_prompt_epoch to datetime from epoch milliseconds
            schedule_df['start_prompt_epoch'] = pd.to_datetime(schedule_df['start_prompt_epoch'], unit='ms')
            # convert to EST timezone
            schedule_df['start_prompt_epoch'] = schedule_df['start_prompt_epoch'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
            # replace the entry of message_type with first_jitai to jitai1
            schedule_df['message_type'] = schedule_df['message_type'].replace('first_jitai', 'jitai1')
            # replace the entry of message_type with second_jitai to jitai2
            schedule_df['message_type'] = schedule_df['message_type'].replace('second_jitai', 'jitai2')
            # replace the entry of message_type with eod_message to eod
            schedule_df['message_type'] = schedule_df['message_type'].replace('eod_message', 'eod')
        except Exception as e:
            goal_type = 'check if Common folder exists'
            jitai1 = 'N/A'
            jitai2 = 'N/A'
            week = 'N/A'
            dayIndex = 'N/A'
            logger.error(f"plot_subject(): No schedule on day {day} for {subject}")
            logger.error(traceback.format_exc())
            schedule_df = pd.DataFrame(columns=['start_prompt_epoch', 'message_type'])

        # calculate the daily_PA
        bout = PAbouts(subject, day)
        try:
            offline_df , daily_PA = bout.calculate_PA(epoch_list)
        except Exception as e:
            daily_PA = 0
            offline_df = pd.DataFrame(columns=['epoch', 'PA'])
            # get the start timestamp of the day in pa_df
            start_timestamp = pa_df['timestamp'].min()
            # get the end timestamp of the day in pa_df
            end_timestamp = pa_df['timestamp'].max()
            #append 2 rows to offline_df
            offline_df = offline_df.append({'epoch': start_timestamp, 'PA': 0}, ignore_index=True)
            offline_df = offline_df.append({'epoch': end_timestamp, 'PA': 0}, ignore_index=True)
            logger.error(f"plot_subject(): No bouts on day {day} for {subject} - offline")
            logger.error(traceback.format_exc())
        # turn the epoch to datetime in EDT timezone
        offline_df['epoch'] = pd.to_datetime(offline_df['epoch'], unit='ms')
        # print(offline_df['epoch'])
        try:
            offline_df['epoch'] = offline_df['epoch'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        except Exception as e:
            logger.error(f"plot_subject(): No offline bouts on day {day} for {subject}")

        # append a colume which calculate the differences between the PA in consecutive rows
        offline_df['diff'] = offline_df['PA'].diff()
        pa_df['diff'] = pa_df['pa'].diff()

        # for rows with NAN in diff, replace it with 0
        offline_df['diff'] = offline_df['diff'].fillna(0)
        pa_df['diff'] = pa_df['diff'].fillna(0)

        # every value > 1 is considered as a bout and value set to 1
        offline_df['diff'] = offline_df['diff'].apply(lambda x: 1 if x > 1 else x)
        pa_df['diff'] = pa_df['diff'].apply(lambda x: 1 if x > 1 else x)
        pa_df['diff'] = pa_df['diff'].apply(lambda x: 0 if x < 0 else x)
        offline_df['diff'] = offline_df['diff'].apply(lambda x: 0 if x < 0 else x)

        # print count of bouts
        offline_bouts = offline_df['diff'].sum()
        online_bouts = pa_df['diff'].sum()

        # move all timestamp from offline and online 3 minutes ahead
        offline_df['epoch'] = offline_df['epoch'] + pd.Timedelta(minutes=3)
        pa_df['timestamp'] = pa_df['timestamp'] + pd.Timedelta(minutes=3)
        
        # real time PA is the max entry in the pa_df
        try:
            real_time_PA = pa_df['pa'].iloc[-1]
        except Exception as e:
            real_time_PA = 0
            logger.error(f"plot_subject(): No bouts on day {day} for {subject} - online")
            logger.error(traceback.format_exc())
        # create a plotly plot with 4 subplots
        fig = go.Figure()
        fig = subplots.make_subplots(rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.08, 
                                     subplot_titles=("AUC", f"Offline PA: {daily_PA} minutes | Offline Bouts: {offline_bouts} bouts", 
                                    f"Online PA: {real_time_PA} minutes | Online Bouts: {online_bouts} bouts", "Battery Level: 100-Green, 50-Yellow, 0-Red"), row_heights=[0.7, 0.05, 0.05, 0.1])

        # add the auc_df to the plot, color the area under the curve
        fig.add_trace(go.Scatter(x=auc_df['epoch'], y=auc_df['AUC'], mode='lines', name='AUC'), row=1, col=1)
        # update the range of the y-axis to 8000 or 100 + max value of auc_df's AUC column
        fig.update_yaxes(range=[0, max(9000, 100 + auc_df['AUC'].max())], row=1, col=1)
        # add a 1 row heatmap in the second subplot with values from offline_df's diff column
        fig.add_trace(go.Heatmap(x=offline_df['epoch'], y = ["PA"], z=[offline_df['diff']], colorscale='Picnic', 
                                 showscale=False, name=f"Offline PA"), row=2, col=1)
        # add a 1 row heatmap in the third subplot with values from pa_df's diff column
        fig.add_trace(go.Heatmap(x=pa_df['timestamp'], y = ["pa"], z=[pa_df['diff']], colorscale='Electric', 
                                 showscale=False, name=f"Online PA"), row=3, col=1)

        # read the battery_df
        battery_df = self.battery.get_battery_data(subject, day)

        # add a heatmap in the fourth subplot with values from battery_df's battery_level column
        fig.add_trace(go.Heatmap(x=battery_df['timestamp'], y = ["battery"], z=[battery_df['battery_level']],
                                    showscale=False, name=f"Battery", colorscale=[(0, "red"), (0.2, "yellow"), (0.7, "green"), (1, '#014705')]), row=4, col=1)
        # for each entry in schedule_df, add a vertical line with x-axis as start_prompt_epoch and y-axis as message_type
        for index, row in schedule_df.iterrows():
            fig.add_vrect(x0=row['start_prompt_epoch'], x1=row['start_prompt_epoch'], row=1, col=1, line_width=1.5, annotation_text=row['message_type'], 
                annotation_position="top right", annotation=dict(font_size=10, textangle=-90), line_dash='dash', line_color='green')

        # did the same for the prompts_df but no annotation and different edge color
        for index, row in prompts_df.iterrows():
            # if message_type is wi
            if row['message_type'] == 'wi':
                fig.add_vrect(x0=row['epoch'], x1=row['epoch'], row=1, col=1, fillcolor='purple', line_width=1.5, annotation_text="WI notification", 
                annotation_position="top right", annotation=dict(font_size=10, textangle=-90), line_color='purple', line_dash='dot')
            else:
                fig.add_vrect(x0=row['epoch'], x1=row['epoch'], row=1, col=1, fillcolor='purple', line_width=1.5, line_color='purple', line_dash='dot')

        # add a horizontal line at y=2000 and mark it as AUC threshold
        fig.add_hline(y=2000, row=1, col=1, line_width=1, line_dash='dash', line_color='red', annotation_text='AUC threshold', annotation_position='bottom left', annotation=dict(font_size=10))

        ### Add annotation to display number of data points
        start_timestamp = pa_df['timestamp'].min()
        fig.add_annotation(text=str(round(samples, 2)) + " Data Points Available Out of " + str(round(total_samples,2)),
                        x=start_timestamp, y=max(9000, 100 + auc_df['AUC'].max()), showarrow=False, bgcolor='lightblue', opacity = 0.95)
        fig.add_annotation(text=str(round(percentage)) + " Percent of Total Data Points",
                        x=start_timestamp, y=max(9000, 100 + auc_df['AUC'].max())*0.92, showarrow=False, bgcolor='lightblue', opacity = 0.95)
        # create a subtitle for the entire plot
        fig.update_layout(
            title=go.layout.Title(
                text=f"Subject {subject} for day {day} <br><sup>GOAL: {goal_type}, JITAI1: {jitai1}, JITAI2: {jitai2}, WEEK: {week}, DAY_INDEX: {dayIndex}</sup>",
                xref="paper",
                x=0
            ),
            height=700,
            width=1200
        )

        # check if the date folder exists in the figures folder
        if not os.path.exists(self.FIGURES_PATH + day):
            # create the date folder
            os.mkdir(self.FIGURES_PATH + day)
            logger.info(f"plot_subject(): Created {day} folder in figures folder")
        # save the plot
        fig.write_html(self.FIGURES_PATH + day + '/' + subject + '.html')
        logger.info(f"plot_subject(): Saved {subject}.html plot in {day} folder")
        fig.write_image(self.FIGURES_PATH + day + '/' + subject + '.png', scale = 5)
        logger.info(f"plot_subject(): Saved {subject}.html plot in {day} folder")

        if show:
            fig.show()

# test = PlotSubject()
# test.plot_subject('user01', '2023-03-03', show=True)