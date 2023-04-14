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
log_file = logs_path + '/prompts.log'
# set the log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# set the log file handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

# put the file handler in the logger
logger.addHandler(file_handler)

class Prompts:
    def __init__(self):
        # initialize the project root
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'

        # initialize the data path
        self.DATA_PATH = self.ROOT_DIR + '/data/raw/'

        # initialize the schedule path
        self.SCHEDULE_PATH = self.ROOT_DIR + '/reports/prompts/'

        # initialize the unzip class
        self.unzip = UnZip()

    def read_goal_settings_df(self, subject, day):
        '''
        read the goal settings df for a subject
        :param subject: str
        :param date: str
        :return: pd.DataFrame
        '''
        subject_full = subject + '@scijitai_com'
        # check if the day format is YYYY-MM-DD by converting it to datetime
        try:
            datetime.datetime.strptime(day, '%Y-%m-%d')
        except ValueError:
            logger.error("read_goal_settings_df(): Incorrect data format, should be YYYY-MM-DD")
            raise ValueError("read_goal_settings_df(): Incorrect data format, should be YYYY-MM-DD")

        # check if the subject is in the data/raw folder
        if subject_full not in os.listdir(self.DATA_PATH):
            logger.error("read_goal_settings_df(): Subject not found in data/raw folder")
            raise ValueError("read_goal_settings_df(): Subject not found in data/raw folder")

        # check if the day is in the subject folder
        if day not in os.listdir(self.DATA_PATH + subject_full + '/logs-watch/'):
            logger.error("read_goal_settings_df(): Day not found in subject logs-watch folder")
            raise ValueError(f"read_goal_settings_df(): {day} not found in subject logs-watch folder")

        # unzipping the logs-watch folder
        try:
            self.unzip.unzip_logs_watch_folder(subject, day)
        except Exception as e:
            logger.error("read_goal_settings_df(): Error unzipping logs-watch folder")
            logger.error(traceback.format_exc())
            print(traceback.format_exc())
            raise ValueError(f'Error unzipping {subject} on {day}')
        logger.info(f"read_goal_settings_df(): Unzipping logs-watch folder for {subject} on {day} done")

        # search for the Common folder inside the logs-watch/day folder
        common_folder_path = self.DATA_PATH + subject_full + '/logs-watch/' + day + '/Common/'
        # check if the Common folder exists
        if not os.path.exists(common_folder_path):
            logger.error("read_goal_settings_df(): Common folder not found in subject logs-watch folder")
            raise ValueError("read_goal_settings_df(): Common folder not found in subject logs-watch folder with path: " + common_folder_path)

        # search for the Watch-GoalSettingsEMA.log.csv file inside the Common folder
        goal_settings_file_path = common_folder_path + 'Watch-GoalSettingsEMA.log.csv'
        # check if the Watch-GoalSettingsEMA.log.csv file exists
        if not os.path.exists(goal_settings_file_path):
            logger.error("read_goal_settings_df(): GoalSettingsEMA file not found in Common folder")
            return None
        
        # read the Watch-GoalSettingsEMA.log.csv file
        goal_settings_df = pd.read_csv(goal_settings_file_path, names=['timestamp', 'type', "message"], header=None)
        # check if the goal_settings_df is empty
        if goal_settings_df.empty:
            logger.error("read_goal_settings_df(): GoalSettingsEMA file is empty")
            return None
        # if there is a message contains the string prompt_answered:wakeEMA then set answered to true
        answered = goal_settings_df['message'].str.contains('prompt_answered:goal_settings').any()
        # filter out the rows that has substring prompt_appear:goal_settings~ in the message column
        goal_settings_df = goal_settings_df[goal_settings_df['message'].str.contains('prompt_appear:goal_settings~')]
        # create a epoch column with the value of spltting the message column by ~ and taking the second element
        goal_settings_df['epoch'] = goal_settings_df['message'].str.split('~').str[1]
        # delete the message column
        del goal_settings_df['message']
        del goal_settings_df['type']
        # create a message_type column and set it to goal_settings
        goal_settings_df['message_type'] = 'goal_settings'

        # create a status column, set all value to false except for the last row to answered
        goal_settings_df['status'] = False
        goal_settings_df['status'].iloc[-1] = answered
        return goal_settings_df

    def read_jitai1_df(self, subject, day):
        '''
        read the jitai1 df for a subject
        :param subject: str
        :param date: str
        :return: pd.DataFrame
        '''
        subject_full = subject + '@scijitai_com'
        # check if the day format is YYYY-MM-DD by converting it to datetime
        try:
            datetime.datetime.strptime(day, '%Y-%m-%d')
        except ValueError:
            logger.error("read_jitai1_df(): Incorrect data format, should be YYYY-MM-DD")
            raise ValueError("read_jitai1_df(): Incorrect data format, should be YYYY-MM-DD")

        # check if the subject is in the data/raw folder
        if subject_full not in os.listdir(self.DATA_PATH):
            logger.error("read_jitai1_df(): Subject not found in data/raw folder")
            raise ValueError("read_jitai1_df(): Subject not found in data/raw folder")

        # check if the day is in the subject folder
        if day not in os.listdir(self.DATA_PATH + subject_full + '/logs-watch/'):
            logger.error("read_jitai1_df(): Day not found in subject logs-watch folder")
            raise ValueError("read_jitai1_df(): Day not found in subject logs-watch folder")

        # unzipping the logs-watch folder
        try:
            self.unzip.unzip_logs_watch_folder(subject, day)
        except Exception as e:
            logger.error("read_jitai1_df(): Error unzipping logs-watch folder")
            logger.error(traceback.format_exc())
            print(traceback.format_exc())
            raise ValueError(f'Error unzipping {subject} on {day}')
        logger.info(f"read_jitai1_df(): Unzipping logs-watch folder for {subject} on {day} done")

        # search for the Common folder inside the logs-watch/day folder
        common_folder_path = self.DATA_PATH + subject_full + '/logs-watch/' + day + '/Common/'
        # check if the Common folder exists
        if not os.path.exists(common_folder_path):
            logger.error("read_jitai1_df(): Common folder not found in subject logs-watch folder")
            raise ValueError("read_jitai1_df(): Common folder not found in subject logs-watch folder")

        # search for the Watch-FirstMessageJITAI.log.csv file inside the Common folder
        jitai1_file_path = common_folder_path + 'Watch-FirstMessageJITAI.log.csv'
        # check if the Watch-FirstMessageJITAI.log.csv file exists
        if not os.path.exists(jitai1_file_path):
            logger.error("read_jitai1_df(): FirstMessageJITAI file not found in Common folder")
            return None

        # read the Watch-FirstMessageJITAI.log.csv file
        jitai1_df = pd.read_csv(jitai1_file_path, names=['timestamp', 'type', "message", 'unknown', 'unknown2'], header=None)
        # check if the jitai1_df is empty
        if jitai1_df.empty:
            logger.error("read_jitai1_df(): FirstMessageJITAI file is empty")
            return None

        # if there is a message contains the string prompt_answered:wakeEMA then set answered to true
        answered = jitai1_df['message'].str.contains('fortuneCookie').any()
        # filter out the rows that has substring prompt_appear:jitai1~ in the message column
        jitai1_df = jitai1_df[jitai1_df['message'].str.contains('prompt_appear:jitai1~')]
        # create a epoch column with the value of spltting the message column by ~ and taking the second element
        jitai1_df['epoch'] = jitai1_df['message'].str.split('~').str[1]
        # delete the message column
        del jitai1_df['message']
        del jitai1_df['type']
        # create a message_type column and set it to jitai1
        jitai1_df['message_type'] = 'jitai1'

        # create a status column, set all value to false except for the last row to answered
        jitai1_df['status'] = False
        jitai1_df['status'].iloc[-1] = answered
        return jitai1_df

    def read_jitai2_df(self, subject, day):
        subject_full = subject + '@scijitai_com'
        # check if the day format is YYYY-MM-DD by converting it to datetime
        try:
            datetime.datetime.strptime(day, '%Y-%m-%d')
        except ValueError:
            logger.error("read_jitai2_df(): Incorrect data format, should be YYYY-MM-DD")
            raise ValueError("read_jitai2_df(): Incorrect data format, should be YYYY-MM-DD")

        # check if the subject is in the data/raw folder
        if subject_full not in os.listdir(self.DATA_PATH):
            logger.error("read_jitai2_df(): Subject not found in data/raw folder")
            raise ValueError("read_jitai2_df(): Subject not found in data/raw folder")

        # check if the day is in the subject folder
        if day not in os.listdir(self.DATA_PATH + subject_full + '/logs-watch/'):
            logger.error("read_jitai2_df(): Day not found in subject logs-watch folder")
            raise ValueError("read_jitai2_df(): Day not found in subject logs-watch folder")

        # unzipping the logs-watch folder
        try:
            self.unzip.unzip_logs_watch_folder(subject, day)
        except Exception as e:
            logger.error("read_jitai2_df(): Error unzipping logs-watch folder")
            logger.error(traceback.format_exc())
            print(traceback.format_exc())
            raise ValueError(f'Error unzipping {subject} on {day}')
        logger.info(f"read_jitai2_df(): Unzipping logs-watch folder for {subject} on {day} done")

        # search for the Common folder inside the logs-watch/day folder
        common_folder_path = self.DATA_PATH + subject_full + '/logs-watch/' + day + '/Common/'
        # check if the Common folder exists
        if not os.path.exists(common_folder_path):
            logger.error("read_jitai2_df(): Common folder not found in subject logs-watch folder")
            raise ValueError("read_jitai2_df(): Common folder not found in subject logs-watch folder")

        # search for the Watch-SecondMessageJITAI.log.csv file inside the Common folder
        jitai2_file_path = common_folder_path + 'Watch-SecondMessageJITAI.log.csv'
        # check if the Watch-SecondMessageJITAI.log.csv file exists
        if not os.path.exists(jitai2_file_path):
            logger.error("read_jitai2_df(): SecondMessageJITAI file not found in Common folder")
            return None

        # read the Watch-SecondMessageJITAI.log.csv file
        jitai2_df = pd.read_csv(jitai2_file_path, names=['timestamp', 'type', "message", 'unknown', 'unknown2'], header=None)
        # check if the jitai2_df is empty
        if jitai2_df.empty:
            logger.error("read_jitai2_df(): SecondMessageJITAI file is empty")
            return None
        
        # if there is a message contains the string prompt_answered:wakeEMA then set answered to true
        answered = jitai2_df['message'].str.contains('fortuneCookie').any()
        # filter out the rows that has substring prompt_appear:jitai2~ in the message column
        jitai2_df = jitai2_df[jitai2_df['message'].str.contains('prompt_appear:jitai2~')]
        # create a epoch column with the value of spltting the message column by ~ and taking the second element
        jitai2_df['epoch'] = jitai2_df['message'].str.split('~').str[1]
        # delete the message column
        del jitai2_df['message']
        del jitai2_df['type']
        # create a message_type column and set it to jitai2
        jitai2_df['message_type'] = 'jitai2'
        
        # create a status column, set all value to false except for the last row to answered
        jitai2_df['status'] = False
        jitai2_df['status'].iloc[-1] = answered
        return jitai2_df

    def read_eod_df(self, subject, day):
        subject_full = subject + '@scijitai_com'
        # check if the day format is YYYY-MM-DD by converting it to datetime
        try:
            datetime.datetime.strptime(day, '%Y-%m-%d')
        except ValueError:
            logger.error("read_eod_df(): Incorrect data format, should be YYYY-MM-DD")
            raise ValueError("read_eod_df(): Incorrect data format, should be YYYY-MM-DD")

        # check if the subject is in the data/raw folder
        if subject_full not in os.listdir(self.DATA_PATH):
            logger.error("read_eod_df(): Subject not found in data/raw folder")
            raise ValueError("read_eod_df(): Subject not found in data/raw folder")

        # check if the day is in the subject folder
        if day not in os.listdir(self.DATA_PATH + subject_full + '/logs-watch/'):
            logger.error("read_eod_df(): Day not found in subject logs-watch folder")
            raise ValueError("read_eod_df(): Day not found in subject logs-watch folder")

        # unzipping the logs-watch folder
        try:
            self.unzip.unzip_logs_watch_folder(subject, day)
        except Exception as e:
            logger.error("read_eod_df(): Error unzipping logs-watch folder")
            logger.error(traceback.format_exc())
            print(traceback.format_exc())
            raise ValueError(f'Error unzipping {subject} on {day}')
        logger.info(f"read_eod_df(): Unzipping logs-watch folder for {subject} on {day} done")

        # search for the Common folder inside the logs-watch/day folder
        common_folder_path = self.DATA_PATH + subject_full + '/logs-watch/' + day + '/Common/'
        # check if the Common folder exists
        if not os.path.exists(common_folder_path):
            logger.error("read_eod_df(): Common folder not found in subject logs-watch folder")
            raise ValueError("read_eod_df(): Common folder not found in subject logs-watch folder")

        # search for the Watch-EODEMA.log.csv file inside the Common folder
        eod_file_path = common_folder_path + 'Watch-EODEMA.log.csv'
        # check if the Watch-EODEMA.log.csv file exists
        if not os.path.exists(eod_file_path):
            logger.error("read_eod_df(): EODEMA file not found in Common folder")
            return None

        # read the Watch-EODEMA.log.csv file
        eod_df = pd.read_csv(eod_file_path, names=['timestamp', 'type', "message", 'unknown', 'unknown2'], header=None)
        # check if the eod_df is empty
        if eod_df.empty:
            logger.error("read_eod_df(): EODEMA file is empty")
            return None

        # if there is a message contains the string prompt_answered:wakeEMA then set answered to true
        answered = eod_df['message'].str.contains('prompt_answered:wakeEMA').any()
        # filter out the rows that has substring prompt_appear:eod~ in the message column
        eod_df = eod_df[eod_df['message'].str.contains('prompt_appear:eod~')]
        # create a epoch column with the value of spltting the message column by ~ and taking the second element
        eod_df['epoch'] = eod_df['message'].str.split('~').str[1]
        # delete the message column
        del eod_df['message']
        del eod_df['type']
        # create a message_type column and set it to eod
        eod_df['message_type'] = 'eod'

        # create a status column, set all value to false except for the last row to answered
        eod_df['status'] = False
        eod_df['status'].iloc[-1] = answered
        return eod_df
    
    def read_WI_message(self, subject, day):
        subject_full = subject + '@scijitai_com'
        # check if the day format is YYYY-MM-DD by converting it to datetime
        try:
            datetime.datetime.strptime(day, '%Y-%m-%d')
        except ValueError:
            logger.error("read_WI_message(): Incorrect data format, should be YYYY-MM-DD")
            raise ValueError("read_WI_message(): Incorrect data format, should be YYYY-MM-DD")

        # check if the subject is in the data/raw folder
        if subject_full not in os.listdir(self.DATA_PATH):
            logger.error("read_WI_message(): Subject not found in data/raw folder")
            raise ValueError("read_WI_message(): Subject not found in data/raw folder")

        # check if the day is in the subject folder
        if day not in os.listdir(self.DATA_PATH + subject_full + '/logs-watch/'):
            logger.error("read_WI_message(): Day not found in subject logs-watch folder")
            raise ValueError("read_WI_message(): Day not found in subject logs-watch folder")

        # unzipping the logs-watch folder
        try:
            self.unzip.unzip_logs_watch_folder(subject, day)
        except Exception as e:
            logger.error("read_WI_message(): Error unzipping logs-watch folder")
            logger.error(traceback.format_exc())
            print(traceback.format_exc())
            raise ValueError(f'Error unzipping {subject} on {day}')
        logger.info(f"read_WI_message(): Unzipping logs-watch folder for {subject} on {day} done")

        # search for the Common folder inside the logs-watch/day folder
        common_folder_path = self.DATA_PATH + subject_full + '/logs-watch/' + day + '/Common/'
        # check if the Common folder exists
        if not os.path.exists(common_folder_path):
            logger.error("read_WI_message(): Common folder not found in subject logs-watch folder")
            raise ValueError("read_WI_message(): Common folder not found in subject logs-watch folder")

        # search for the Watch-EODEMA.log.csv file inside the Common folder
        wi_file_path = common_folder_path + 'Watch-WI-Notification.log.csv'
        # check if the Watch-wiEMA.log.csv file exists
        if not os.path.exists(wi_file_path):
            logger.error("read_WI_message(): WI file not found in Common folder")
            return None
        
        # read the Watch-wiEMA.log.csv file
        wi_df = pd.read_csv(wi_file_path, names=['timestamp', 'type', "message"], header=None)
        # print(wi_df)
        # check if the wi_df is empty
        if wi_df.empty:
            logger.error("read_wi_df(): wiEMA file is empty")
            return None

        # filter out the rows that has substring prompt_appear:wi~ in the message column
        wi_df = wi_df[wi_df['message'].str.contains('prompted~')]
        # create a epoch column with the value of spltting the message column by ~ and taking the second element
        wi_df['epoch'] = wi_df['message'].str.split('~').str[1]
        # delete the message column
        del wi_df['message']
        del wi_df['type']
        # create a message_type column and set it to wi
        wi_df['message_type'] = 'wi'

        #only take the first row
        wi_df = wi_df.tail(1)
        return wi_df

    def read_all_message_df(self, subject, day):
        goal_settings_df = self.read_goal_settings_df(subject, day)
        jitai1_df = self.read_jitai1_df(subject, day)
        jitai2_df = self.read_jitai2_df(subject, day)
        eod_df = self.read_eod_df(subject, day)
        wi_df = self.read_WI_message(subject, day)

        all_message_df = pd.concat([goal_settings_df, jitai1_df, jitai2_df, eod_df, wi_df])
        all_message_df = all_message_df.sort_values(by=['timestamp'])
        #remove all columns with names starting with 'unknown'
        all_message_df = all_message_df.loc[:, ~all_message_df.columns.str.startswith('unknown')]

        # check if the day folder exists in the shcedule folder
        if day not in os.listdir(self.SCHEDULE_PATH):
            # create the day folder in the schedule folder
            os.mkdir(self.SCHEDULE_PATH + day)
            logger.info(f"read_all_message_df(): Created {day} folder in schedule folder")

        # save the all_message_df to a csv file
        all_message_df.to_csv(self.SCHEDULE_PATH + day + '/' + subject + '.csv', index=False)

        return all_message_df

    def process_all_user(self, day, user_list = ['user01', 'user02', 'user03']):
        for user in user_list:
            try:
                logger.info(f"process_all_user(): Processing {user} on {day}")
                self.read_all_message_df(user, day)
            except Exception as e:
                logger.error(f"process_all_user(): Error processing {user} on {day}")
                logger.error(traceback.format_exc())
                # print(traceback.format_exc())
                continue


# test = Prompts()
# print(
#     test.read_all_message_df('user01', '2023-02-27')
# )
