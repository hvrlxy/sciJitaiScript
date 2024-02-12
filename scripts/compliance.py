import numpy as np
import pandas as pd
import datetime
import os
from logger import Logger
from schedule import Schedule
from prompts import Prompts
import traceback
import warnings

warnings.filterwarnings("ignore")

class Compliance:
    def __init__(self):
        self.logger = Logger().getLog("compliance")

        # initialize the project root
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'

        # initialize the schedule path
        self.SCHEDULE_PATH = self.ROOT_DIR + '/reports/schedule/'

        # initialize the schedule class
        self.schedule = Schedule()

        # initialize the prompts class
        self.prompts = Prompts()

    def generate_user_compliance_report(self, user, date):
        schedule_df = self.schedule.process_schedule_generation(user, date)
        if schedule_df is None:
            return None
        # get the user's prompts data
        prompts_df = self.prompts.read_all_message_df(user, date)
        if prompts_df is None:
            # create an empty dataframe
            prompts_df = pd.DataFrame(columns=['timestamp', 'epoch', 'message_type', 'status'])
        #convert message_type "jitai1" to "first_message"
        prompts_df['message_type'] = prompts_df['message_type'].replace('jitai1', 'first_jitai')
        #convert message_type "jitai2" to "second_message"
        prompts_df['message_type'] = prompts_df['message_type'].replace('jitai2', 'second_jitai')
        #convert message_type "eod" to "eod_message"
        prompts_df['message_type'] = prompts_df['message_type'].replace('eod', 'eod_message')
        
        # create a new df called compliance_df, copy over content from schedule_df
        compliance_df = schedule_df.copy()
        # add three columns, called them "prompt_epoch", "reprompt1_epoch", "reprompt2_epoch"
        compliance_df['prompt_epoch'] = np.nan
        compliance_df['reprompt1_epoch'] = np.nan
        compliance_df['reprompt2_epoch'] = np.nan
        compliance_df['status'] = "NOT_PROMPTED"
        # loop through each row in compliance_df
        for index, row in compliance_df.iterrows():
            prompt_time = []
            # get the message_type
            message_type = row['message_type']
            # filter prompts_df by message_type
            filtered_df = prompts_df[prompts_df['message_type'] == message_type]
            # print(prompts_df)
            #check if filtered_df is empty
            if not filtered_df.empty:
                # get the status on the last row of filtered_df
                compliance_df.loc[index, 'status'] = filtered_df['status'].iloc[-1]
            # get start_prompt_epoch as list and put it in prompt_time
            prompt_time += (filtered_df['epoch'].tolist())
            # add nan to prompt_time if the length of prompt_time is less than 3
            while len(prompt_time) < 3:
                prompt_time.append(-1)
            # put the values in prompt_time into compliance_df as prompt_epoch, reprompt1_epoch, reprompt2_epoch
            compliance_df.loc[index, 'prompt_epoch'] = prompt_time[0]
            compliance_df.loc[index, 'reprompt1_epoch'] = prompt_time[1]
            compliance_df.loc[index, 'reprompt2_epoch'] = prompt_time[2]
        #change the name of the column start_prompt to scheduled_prompt
        compliance_df = compliance_df.rename(columns={'start_prompt': 'scheduled_prompt',
                                                    'start_prompt_epoch': 'scheduled_prompt_epoch'})
        # filter out the wi message type in prompts_df
        wi_df = prompts_df[prompts_df['message_type'] == 'wi']
        if (len(wi_df) > 0):
            # add a new row to compliance_df, copy over the last row
            compliance_df.loc[len(compliance_df)] = compliance_df.iloc[-1]
            # set the message_type of the new row to "wi"
            compliance_df.loc[len(compliance_df)-1, 'message_type'] = 'wi'
            # set the prompt_epoch of the new row to the start_prompt_epoch in the wi_df
            compliance_df.loc[len(compliance_df)-1, 'prompt_epoch'] = wi_df['epoch'].iloc[0]
            # set the reprompt1_epoch of the new row to nan
            compliance_df.loc[len(compliance_df)-1, 'reprompt1_epoch'] = np.nan
            # set the reprompt2_epoch of the new row to nan
            compliance_df.loc[len(compliance_df)-1, 'reprompt2_epoch'] = np.nan
            # set the scheduled_prompt epoch of the new row to noon of the date
            compliance_df.loc[len(compliance_df)-1, 'scheduled_prompt_epoch'] = datetime.datetime.strptime(date, '%Y-%m-%d').replace(hour=12, minute=0, second=0).timestamp() * 1000
            # set the scheduled_prompt of the new row to the scheduled_prompt_epoch converted to HH:MM
            compliance_df.loc[len(compliance_df)-1, 'scheduled_prompt'] = datetime.datetime.fromtimestamp(compliance_df.loc[len(compliance_df)-1, 'scheduled_prompt_epoch']/1000).strftime('%H:%M')
        compliance_df['status'] = compliance_df['status'].replace(True, 'ANS')
        compliance_df['status'] = compliance_df['status'].replace(False, 'NOT_ANS')
        
        return compliance_df
    
    def add_late(self, user, date):
        compliance_df = self.generate_user_compliance_report(user, date)
        # convert the scheduled_prompt_epoch to int with NaN as -1
        compliance_df['scheduled_prompt_epoch'] = compliance_df['scheduled_prompt_epoch'].fillna(-1).astype(int)
        # convert the prompt_epoch to int with NaN as -1
        compliance_df['prompt_epoch'] = compliance_df['prompt_epoch'].fillna(-1).astype(int)
        # add a new column called "late", marked as LATE if scheduled_prompt_epoch 5 minutes away from prompt_epoch, get the absolute value of the difference
        compliance_df['late'] = compliance_df.apply(lambda x: 'LATE' if abs(x['scheduled_prompt_epoch'] - x['prompt_epoch']) > 5 * 60 * 1000 else 'ON_TIME', axis=1)
        # for the rows with status = NOT_PROMPTED, set the late to "N/A"
        compliance_df.loc[compliance_df['status'] == 'NOT_PROMPTED', 'late'] = 'N/A'
        # # but if status = NOT_PROMPTED and message_note not start with NO_, set the late to "LATE"
        # compliance_df.loc[(compliance_df['message_note'].notnull()) & (compliance_df['message_note'].str.startswith('NO') == False), 'late'] = 'LATE'
        # replace all nan with -1
        compliance_df = compliance_df.replace(np.nan, -1)
        # add the prompt column before prompt_epoch column that convert the epoch to datetime with format HH:MM
        compliance_df.insert(9, 'prompt', compliance_df['prompt_epoch'].apply(lambda x: datetime.datetime.fromtimestamp(int(x)/1000).strftime('%H:%M') if (x != -1) else 'N/A'))
        # add the reprompt1 column before reprompt1_epoch column that convert the epoch to datetime with format HH:MM
        compliance_df.insert(11, 'reprompt1', compliance_df['reprompt1_epoch'].apply(lambda x: datetime.datetime.fromtimestamp(int(x)/1000).strftime('%H:%M') if (x != np.nan and int(x) != -1) else 'N/A'))
        # add the reprompt2 column before reprompt2_epoch column that convert the epoch to datetime with format HH:MM
        compliance_df.insert(13, 'reprompt2', compliance_df['reprompt2_epoch'].apply(lambda x: datetime.datetime.fromtimestamp(int(x)/1000).strftime('%H:%M') if (int(x) != -1) else 'N/A'))
        # convert all -1 to "N/A"
        compliance_df = compliance_df.replace(-1, 'N/A')
        return compliance_df

    def add_battery_check(self, user, date, scheduled_prompt_epoch):
        # get the battery log file in the Common folder
        battery_log_file = '/home/hle5/sciJitaiScript/logs-watch/Common/Watch-BatteryLogger.log.csv'
        try:
            # read the battery log file
            battery_df = pd.read_csv(battery_log_file, names=['datetime', 'info', 'battery_level', 'charging'])
            # replace AST with EDT in datetime
            battery_df['datetime'] = battery_df['datetime'].apply(lambda x: str(x).replace('AST', 'EDT'))
            battery_df['datetime'] = battery_df['datetime'].apply(lambda x: str(x).replace('CDT', 'EDT'))
            battery_df['datetime'] = battery_df['datetime'].apply(lambda x: str(x).replace('GMT+08:00', 'EDT'))
            battery_df['datetime'] = battery_df['datetime'].apply(lambda x: str(x).replace('PDT', 'EDT'))
            battery_df['datetime'] = battery_df['datetime'].apply(lambda x: str(x).replace('PST', 'EDT'))
            battery_df['datetime'] = battery_df['datetime'].apply(lambda x: str(x).replace('CST', 'EDT'))
            #convert datetime from format %a %b %d %H:%M:%S %Z %Y to epoch milliseconds
            battery_df['datetime'] = battery_df['datetime'].apply(lambda x: datetime.datetime.strptime(x, '%a %b %d %H:%M:%S %Z %Y').timestamp() * 1000)
            # get the row with the closest datetime to the scheduled_prompt_epoch
            battery_df = battery_df.iloc[(battery_df['datetime'] - scheduled_prompt_epoch).abs().argsort()[:1]]
            # get the battery_level
            battery_level = battery_df['battery_level'].values[0]
            # if battery_level is less than 20, return True, else return False
            return True if battery_level < 20 else False
        except Exception as e:
            print(traceback.format_exc())
            return False
        
    def add_dnd_check(self, user, date, scheduled_prompt_epoch):
        # get the hour string from the scheduled_prompt_epoch
        hour = datetime.datetime.fromtimestamp(int(scheduled_prompt_epoch)/1000).strftime('%H')
        # format it to have 2 digits
        hour = f'0{hour}' if len(hour) == 1 else hour
        # list all the folders in date folder
        folders = os.listdir('/home/hle5/sciJitaiScript/logs-watch/')
        # filter out the folders that start with hour
        folders = list(filter(lambda x: x.startswith(hour), folders))
        # remove the one ends with .zip
        folders = list(filter(lambda x: x.endswith('.zip') == False, folders))
        if len(folders) == 0:
            return False
        # get the folder name
        folder = folders[0]
        # get the battery log file in the Common folder
        dnd_path = f'/home/hle5/sciJitaiScript/logs-watch/logs-watch//{folder}/Watch-PromptScheduler.log.csv'
        try:
            # read the battery log file
            dnd_df = pd.read_csv(dnd_path, names=['datetime', 'info', 'message', 'unknown'])
            # check if there is any line with message contains Dnd status: 2
            return True if dnd_df[dnd_df['message'].str.contains('Dnd status: 2')].shape[0] > 0 else False
        except Exception as e:
            return False
            
    def add_why_late(self, user, date):
        compliance_df = self.add_late(user, date)
        # add a new column called "why_late"
        compliance_df['why_late'] = "N/A"
        # for rows without substring LATE in late, set the why_late to "NOT_LATE"
        compliance_df.loc[~compliance_df['late'].str.contains('LATE'), 'why_late'] = 'NOT_LATE'
        # loop through the rows with substring LATE in late
        for index, row in compliance_df[compliance_df['late'].str.contains('LATE')].iterrows():
            # get the scheduled_prompt_epoch    
            scheduled_prompt_epoch = row['scheduled_prompt_epoch']
            if self.add_battery_check(user, date, scheduled_prompt_epoch):
                compliance_df.loc[index, 'why_late'] = 'LOW_BATTERY'
            elif self.add_dnd_check(user, date, scheduled_prompt_epoch):
                compliance_df.loc[index, 'why_late'] = 'DND'
            else:
                compliance_df.loc[index, 'why_late'] = 'UNKNOWN'

        return compliance_df
    
    def add_why_not_prompted(self, user, date):
        compliance_df = self.add_why_late(user, date)

        # add why_not_prompted column
        compliance_df['why_not_prompted'] = "N/A"
        # loop through the rows with substring NOT_PROMPTED in status
        for index, row in compliance_df[compliance_df['status'].str.contains('NOT_PROMPTED')].iterrows():
            # get the scheduled_prompt_epoch    
            scheduled_prompt_epoch = row['scheduled_prompt_epoch']
            # if message_note start with NO, set why_not_prompted to SCHEDULE
            if row['message_note'].startswith('NO') or (row['message_note'].startswith('DAY_')) or (row['message_note'] == 'N/A' and 'jitai' in row['message_type']):
                compliance_df.loc[index, 'why_not_prompted'] = 'SCHEDULE'
            elif self.add_battery_check(user, date, scheduled_prompt_epoch):
                compliance_df.loc[index, 'why_not_prompted'] = 'LOW_BATTERY'
            elif self.add_dnd_check(user, date, scheduled_prompt_epoch):
                compliance_df.loc[index, 'why_not_prompted'] = 'DND'
            else:
                compliance_df.loc[index, 'why_not_prompted'] = 'UNKNOWN'
        return compliance_df
    
    def save_compliance_report(self, user, date):
        compliance_df = self.add_why_not_prompted(user, date)
        # check if the compliance/ folder exists, if not, create it
        if not os.path.exists(self.ROOT_DIR + '/reports/compliance/'):
            os.makedirs(self.ROOT_DIR + '/reports/compliance/')
        #check if the user folder exists, if not, create it
        if not os.path.exists(self.ROOT_DIR + f'/reports/compliance/{user}/'):
            os.makedirs(self.ROOT_DIR + f'/reports/compliance/{user}/')
            
        # save the compliance report to csv
        compliance_df.to_csv(self.ROOT_DIR + f'/reports/compliance/{user}/{date}.csv')
        return compliance_df