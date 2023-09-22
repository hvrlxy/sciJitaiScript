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
log_file = logs_path + '/schedule.log'
# set the log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# set the log file handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

# put the file handler in the logger
logger.addHandler(file_handler)

class Schedule:
    def __init__(self):
        # initialize the project root
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'

        # initialize the data path
        self.DATA_PATH = '/opt/sci_jitai/'

        # initialize the schedule path
        self.SCHEDULE_PATH = self.ROOT_DIR + '/reports/schedule/'

        # initialize the unzip class
        self.unzip = UnZip()

    def generate_day_schedule(self, subject: str, day: str):
        subject_full = subject + '@scijitai_com'
        # check if the day format is YYYY-MM-DD by converting it to datetime
        try:
            datetime.datetime.strptime(day, '%Y-%m-%d')
        except ValueError:
            logger.error("generate_day_schedule(): Incorrect data format, should be YYYY-MM-DD")
            print("generate_day_schedule(): Incorrect data format, should be YYYY-MM-DD")

        # check if the subject is in the data/raw folder
        if subject_full not in os.listdir(self.DATA_PATH):
            logger.error("generate_day_schedule(): Subject not found in data/raw folder")
            print("generate_day_schedule(): Subject not found in data/raw folder")

        # check if the day is in the subject folder
        if day not in os.listdir(self.DATA_PATH + subject_full + '/logs-watch/'):
            logger.error("generate_day_schedule(): Day not found in subject logs-watch folder")
            print("generate_day_schedule(): Day not found in subject logs-watch folder")

        # unzipping the logs-watch folder
        try:
            self.unzip.unzip_logs_watch_folder(subject, day)
        except Exception as e:
            logger.error("generate_day_schedule(): Error unzipping logs-watch folder")
            logger.error(traceback.format_exc())
            # print(traceback.format_exc())
            print(f'Error unzipping {subject} on {day}')
        logger.info(f"generate_day_schedule(): Unzipping logs-watch folder for {subject} on {day} done")

        # search for the Common folder inside the logs-watch/day folder
        common_folder_path = self.DATA_PATH + subject_full + '/logs-watch/' + day + '/Common/'
        # check if the Common folder exists
        if not os.path.exists(common_folder_path):
            logger.error("generate_day_schedule(): Common folder not found in subject logs-watch folder")
            print("generate_day_schedule(): Common folder not found in subject logs-watch folder")

        # search for the Watch-EMAManager.log.csv file inside the Common folder
        watch_sensor_manager_service_path = common_folder_path + 'Watch-EMAManager.log.csv'
        # check if the Watch-EMAManager.log.csv file exists
        if not os.path.exists(watch_sensor_manager_service_path):
            logger.error("generate_day_schedule(): Watch-EMAManager.log.csv file not found in subject logs-watch folder")
            print("generate_day_schedule(): Watch-EMAManager.log.csv file not found in subject logs-watch folder")
            return None
        
        # read the Watch-EMAManager.log.csv file
        logger.info(f"generate_day_schedule(): Reading Watch-EMAManager.log.csv file for {subject} on {day}")
        # read the Watch-EMAManager.log.csv file with columns: timestamp, type and message
        watch_sensor_manager_service_df = pd.read_csv(watch_sensor_manager_service_path, names=['timestamp', 'type', 'message', "unknown", "unknown2", "unknown3"])
        logger.info(f"generate_day_schedule(): Reading Watch-EMAManager.log.csv file for {subject} on {day} done")

        # filter out the rows start with schedule_generation in the message column
        logger.info(f"generate_day_schedule(): Filtering out the rows start with schedule_generation in the message column for {subject} on {day}")
        schedule_generation_df = watch_sensor_manager_service_df[watch_sensor_manager_service_df['message'].str.startswith('schedule_generation')]
        logger.info(f"generate_day_schedule(): Filtering out the rows start with schedule_generation in the message column for {subject} on {day} done")
        # reindex the schedule_generation_df
        schedule_generation_df = schedule_generation_df.reset_index(drop=True)

        # get the index of the row with prefix schedule_generation:week
        schedule_generation_index = schedule_generation_df[schedule_generation_df['message'].str.contains('schedule_generation:week')].index[0]
        
        #remove all rows before the schedule_generation:week row
        schedule_generation_df = schedule_generation_df[schedule_generation_index:]

        return schedule_generation_df

    def generate_schedule_retrieval_df(self, subject:str, date:str):
        # get the path to the sensor manager service file
        sensor_manager_service_path = self.DATA_PATH + subject + '@scijitai_com/logs-watch/' + date + '/Common/Watch-EMAManager.log.csv'
        #check if the sensor manager service file exists
        if not os.path.exists(sensor_manager_service_path):
            logger.error("generate_schedule_retrieval_df(): Watch-EMAManager.log.csv file not found in subject logs-watch folder")
        
        # read the Watch-EMAManager.log.csv file
        logger.info(f"generate_schedule_retrieval_df(): Reading Watch-EMAManager.log.csv file for {subject} on {date}")
        # read the Watch-EMAManager.log.csv file with columns: timestamp, type and message
        sensor_manager_service_df = pd.read_csv(sensor_manager_service_path, names=['timestamp', 'type', 'message', "unknown", "unknown2", "unknown3"])
        logger.info(f"generate_schedule_retrieval_df(): Reading Watch-EMAManager.log.csv file for {subject} on {date} done")

        # get the index of the row with prefix schedule_generation:week
        schedule_generation_index = sensor_manager_service_df[sensor_manager_service_df['message'].str.contains('schedule_generation:week')].index[0]
        #remove all rows before the schedule_generation:week row
        sensor_manager_service_df = sensor_manager_service_df[schedule_generation_index:]
        # filter out the rows start with schedule_retrieval in the message column
        logger.info(f"generate_schedule_retrieval_df(): Filtering out the rows start with schedule_retrieval in the message column for {subject} on {date}")
        schedule_retrieval_df = sensor_manager_service_df[sensor_manager_service_df['message'].str.startswith('schedule_retrieval')]
        logger.info(f"generate_schedule_retrieval_df(): Filtering out the rows start with schedule_retrieval in the message column for {subject} on {date} done")

        # reindex the schedule_retrieval_df
        schedule_retrieval_df = schedule_retrieval_df.reset_index(drop=True)

        return schedule_retrieval_df

    def extract_info_from_schedule_df(self, schedule_df, key_word):
        logger.info(f"extract_info_from_schedule_df(): Extracting {key_word} from schedule_df")
        try:
            # from the message column, get the first row with substring week 
            value = schedule_df[schedule_df['message'].str.contains(key_word)].iloc[0]['message']
            # split by ~ and get the second element
            value = value.split('~')[1]
            logger.info(f"extract_info_from_schedule_df(): {key_word} is {value}")
        except Exception as e:
            logger.error(f"extract_info_from_schedule_df(): Error getting {key_word}")
            logger.error(traceback.format_exc())
            # return "N/A"
            return np.NaN

        return value
    
    def process_schedule_generation(self, subject: str, day: str):
        schedule_generation_df = self.generate_day_schedule(subject, day)
        try:
            #replace any AST in timestamp with EST
            schedule_generation_df['timestamp'] = schedule_generation_df['timestamp'].str.replace('AST', 'EST')
            #replace any AST in timestamp with EST
            schedule_generation_df['timestamp'] = schedule_generation_df['timestamp'].str.replace('CDT', 'EST')
            # get the first item in the timestamp column
            timestamp = schedule_generation_df.iloc[0]['timestamp']
        except Exception as e:
            logger.error(f"process_schedule_generation(): Error getting timestamp")
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=['date', 'wake_time', 'week', 'day', 'message_type', 'message_note', 'start_prompt'])
        # convert the timestamp to datetime (string to datetime)
        timestamp = datetime.datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Z %Y')
        # convert timestamp to string format YYYY-MM-DD
        timestamp = timestamp.strftime('%Y-%m-%d')

        # get the week
        week = self.extract_info_from_schedule_df(schedule_generation_df, 'week')
        dayIndex = self.extract_info_from_schedule_df(schedule_generation_df, 'day')
        wake_time = self.extract_info_from_schedule_df(schedule_generation_df, 'wake_time')
        goal_settings = self.extract_info_from_schedule_df(schedule_generation_df, 'goal_settings')
        first_jitai = self.extract_info_from_schedule_df(schedule_generation_df, 'first_jitai')
        second_jitai = self.extract_info_from_schedule_df(schedule_generation_df, 'second_jitai')
        eod_prompt = str(int(wake_time) + 13 * 60 * 60 * 1000)
        # get goalType, jitai1, jitai2
        goalType, jitai1, jitai2 = self.process_schedule_retrieval(subject, timestamp)
        # create a schedule_df with columns: date, wake_time, week, day, message_type, start_prompt
        logger.info(f"process_schedule_generation(): Creating schedule_df for {subject} on {day}")
        schedule_df = pd.DataFrame(columns=['date', 'wake_time', 'week', 'day', 'message_type', 'message_note', 'start_prompt'])
        # add the goal_settings to the schedule_df
        schedule_df = schedule_df.append({'date': timestamp, 'wake_time': wake_time, 'week': week, 'day': dayIndex, 'message_type': 'goal_settings', 'message_note': goalType, 'start_prompt': goal_settings}, ignore_index=True)

        # add the first_jitai to the schedule_df
        schedule_df = schedule_df.append({'date': timestamp, 'wake_time': wake_time, 'week': week, 'day': dayIndex, 'message_type': 'first_jitai', 'message_note': jitai1, 'start_prompt': first_jitai}, ignore_index=True)
        # add the second_jitai to the schedule_df
        schedule_df = schedule_df.append({'date': timestamp, 'wake_time': wake_time, 'week': week, 'day': dayIndex, 'message_type': 'second_jitai', 'message_note': jitai2, 'start_prompt': second_jitai}, ignore_index=True)
        # add the eod_message to the schedule_df
        schedule_df = schedule_df.append({'date': timestamp, 'wake_time': wake_time, 'week': week, 'day': dayIndex, 'message_type': 'eod_message', 'message_note': None, 'start_prompt': eod_prompt}, ignore_index=True)
        logger.info(f"process_schedule_generation(): Creating schedule_df for {subject} on {day} done")

        # add an user_id column to the beginning of the schedule_df
        schedule_df.insert(0, 'user_id', subject)
        # convert the wake_time and the start_prompt to datetime string with format HH:MM from epoch time in milliseconds
        logger.info(f"process_schedule_generation(): Converting the wake_time and the start_prompt to datetime string with format HH:MM from epoch time for {subject} on {day}")
        schedule_df['wake_time'] = schedule_df['wake_time'].apply(lambda x: datetime.datetime.fromtimestamp(int(x)/1000).strftime('%H:%M'))
        schedule_df['start_prompt_epoch'] = schedule_df['start_prompt']
        schedule_df['start_prompt'] = schedule_df['start_prompt'].apply(lambda x: datetime.datetime.fromtimestamp(int(x)/1000).strftime('%H:%M'))
        logger.info(f"process_schedule_generation(): Converting the wake_time and the start_prompt to datetime string with format HH:MM from epoch time for {subject} on {day} done")
        return schedule_df

    def process_schedule_retrieval(self, subject: str, day: str):
        schedule_retrieval_df = self.generate_schedule_retrieval_df(subject, day)
        logger.info(f"process_schedule_retrieval(): Retrieving goalType, jitai1, jitai2 for {subject} on {day}")
        # get the goalType
        goalType = self.extract_info_from_schedule_df(schedule_retrieval_df, 'goalType')
        # get jitai1
        jitai1 = self.extract_info_from_schedule_df(schedule_retrieval_df, 'jitai1')
        # get jitai2
        jitai2 = self.extract_info_from_schedule_df(schedule_retrieval_df, 'jitai2')
        logger.info(f"process_schedule_retrieval(): Retrieving goalType, jitai1, jitai2 for {subject} on {day} done")
        return goalType, jitai1, jitai2

    def process_all_user(self, day: str, user_list):
        # create a schedule_df with columns: date, wake_time, week, day, message_type, start_prompt
        schedule_df = pd.DataFrame(columns=['user_id', 'date', 'wake_time', 'week', 'day', 'message_type', 'message_note', 'start_prompt'])
        # loop through the user_list
        for user in user_list:
            try:
                logger.info(f"process_all_user(): Generating schedule_df for {user} on {day}")
                # get the schedule_df for the user
                user_schedule_df = self.process_schedule_generation(user, day)
                # check if the date folder exists in the schedule folder
                if not os.path.exists(f"{self.SCHEDULE_PATH}/{day}"):
                    # create the date folder
                    os.mkdir(f"{self.SCHEDULE_PATH}/{day}")

                # save the user_schedule_df to a csv file
                user_schedule_df.to_csv(f"{self.SCHEDULE_PATH}/{day}/{user}.csv", index=False)
                # append the user_schedule_df to the schedule_df
                schedule_df = schedule_df.append(user_schedule_df, ignore_index=True)
                logger.info(f"process_all_user(): Generating schedule_df for {user} on {day} done")
            except Exception as e:
                logger.error(f"process_all_user(): Error while generating schedule_df for {user} on {day}: {e}")
                logger.error(traceback.format_exc())
                continue

        # save the schedule_df to a csv file
        logger.info(f"process_all_user(): Saving schedule_df to a csv file")
    
        # save the schedule_df to a csv file
        schedule_df.to_csv(f"{self.SCHEDULE_PATH}/{day}/schedule.csv", index=False)
        logger.info(f"process_all_user(): Saving schedule_df to a csv file done")

        return schedule_df
