import numpy as np
import pandas as pd
import datetime
from unzip_all import UnZip
import os
from logger import Logger
from compliance import Compliance
import traceback
import warnings

warnings.filterwarnings("ignore")

class Proximal:
    def __init__(self):
        self.logger = Logger().getLog("proximal")

        # initialize the project root
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'

        # initialize the data path
        self.DATA_PATH = self.ROOT_DIR + '/data/raw/'

        # initialize the schedule path
        self.SCHEDULE_PATH = self.ROOT_DIR + '/reports/schedule/'

        # intialize the report path
        self.REPORT_PATH = self.ROOT_DIR + '/reports/'

        # initialize the unzip class
        self.unzip = UnZip()

        # initialize the compliance class
        self.compliance = Compliance()

    def get_answered_time(self, row):
        # check if row is None
        if row is None:
            return 0
        # first check if "reprompt2" column is not N/A, if it is not, return the value
        if row['reprompt2_epoch'] != 'N/A':
            return row['reprompt2_epoch']
        # if it is N/A, check if "reprompt1" column is not N/A, if it is not, return the value
        elif row['reprompt1_epoch'] != 'N/A':
            return row['reprompt1_epoch']
        # if it is N/A, check if "prompt" column is not N/A, if it is not, return the value
        elif row['prompt_epoch'] != 'N/A':
            return row['prompt_epoch']
        else:
            return 0

    def get_jitai_info(self, userID, date, compliance_df=None):
        '''
        This function will get the jitai info for the given user and date
        It will return two variables: time the first jitai is answered
        and time the second jitai is answered.
        If the jitai is not shown or answered, it will return 0
        Timestamp return is in epoch millis
        '''
        # filter the compliance dataframe by message_type and only get the first_jitai and second_jitai
        jitai_df = compliance_df[compliance_df['message_type'].isin(['first_jitai', 'second_jitai'])]
        # filter the jitai_df by the status and only get ANS
        jitai_df = jitai_df[jitai_df['status'] == 'ANS']
        if len(jitai_df[jitai_df['message_type'] == 'first_jitai']) == 0:
            first_jitai = self.get_answered_time(None)
        else:
            first_jitai = int(self.get_answered_time(jitai_df[jitai_df['message_type'] == 'first_jitai'].iloc[0]))
        
        if len(jitai_df[jitai_df['message_type'] == 'second_jitai']) == 0:
            second_jitai = self.get_answered_time(None)
        else:
            second_jitai = int(self.get_answered_time(jitai_df[jitai_df['message_type'] == 'second_jitai'].iloc[0]))
        
        return first_jitai, second_jitai
    
    def process_proximal_with_timestamp(self, userID, date, epoch):
        '''
        This function will process the proximal data for the given user, date and epoch
        It will return the proximal value for the given epoch and the timestamp of the proximal value
        '''
        if epoch == 0:
            return None, None
        # turn the epoch into datetime and get the hour in HH format
        hour = datetime.datetime.fromtimestamp(epoch/1000).strftime('%H')
        # check if the hour is smaller than 10
        if int(hour) < 10:
            # if it is, add a 0 in front of it
            hour = '0' + hour
        # list all the folders in date folder
        folders = os.listdir(self.DATA_PATH + f'{userID}@scijitai_com/logs-watch/{date}')
        # filter out the folders that start with hour
        folders = [folder for folder in folders if folder.startswith(hour)]
        # remove the one ends with .zip
        folders = list(filter(lambda x: x.endswith('.zip') == False, folders))
        if len(folders) == 0:
            return None, None
        # get the folder name
        folder = folders[0]
        # get the battery log file in the Common folder
        pa_path = self.DATA_PATH + f'{userID}@scijitai_com/logs-watch/{date}/{folder}/Watch-PAMinutes.log.csv'
        # read the battery log file
        try:
            pa_df = pd.read_csv(pa_path, names=["timestamp", "info", "epoch", "value"])
            # turn the epoch column into int
            pa_df['epoch'] = pa_df['epoch'].astype(int)
            # get the row closest to the target hour
            pa_df = pa_df.iloc[(pa_df['epoch']-epoch).abs().argsort()[:1]]
            # get the value
            pa_value = pa_df['value'].iloc[0]
            return pa_value, pa_df['epoch'].iloc[0]
        except:
            print(traceback.format_exc())
            return None, None
        

    def get_proximal(self, userID, date, compliance_df):
        first_jitai, second_jitai = self.get_jitai_info(userID, date, compliance_df)
        # process the first jitai
        first_jitai_value, first_jitai_timestamp = self.process_proximal_with_timestamp(userID, date, first_jitai)
        # process the second jitai
        second_jitai_value, second_jitai_timestamp = self.process_proximal_with_timestamp(userID, date, second_jitai)
        if first_jitai != 0:
            # get the value 2 hours after the first jitai
            first_jitai_value_2h,_ = self.process_proximal_with_timestamp(userID, date, first_jitai + 2*60*60*1000)
        else:
            first_jitai_value_2h = None
        if second_jitai != 0:
            # get the value 2 hours after the second jitai
            second_jitai_value_2h,_ = self.process_proximal_with_timestamp(userID, date, second_jitai + 2*60*60*1000)
        else:
            second_jitai_value_2h = None
        # return the first jitai value, first jitai timestamp, second jitai value, second jitai timestamp
        return first_jitai_value, first_jitai_timestamp, second_jitai_value, second_jitai_timestamp, first_jitai_value_2h, second_jitai_value_2h
    
    def get_weekly_proximal_data(self, userID, last_date):
        # convert the date to datetime
        last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d')
        # initialize a dataframe
        result_df = pd.DataFrame(columns=['date', 
                                          'jitai1_timestamp', 
                                          'jitai1_pa', 
                                            'jitai1_pa_2h',
                                          'jitai2_timestamp', 
                                          'jitai2_pa',
                                            'jitai2_pa_2h'])
        # loop from 6 days ago to 0 days ago
        for i in range(7, 0, -1):
            # get the date in string format
            date = (last_date - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            try:
                compliance_df = self.compliance.save_compliance_report(userID, date)
                # get the proximal data
                first_jitai_value, first_jitai_timestamp, second_jitai_value, second_jitai_timestamp, first_jitai_value_2h, second_jitai_value_2h = self.get_proximal(userID, date, compliance_df)
            except Exception as e:
                # set all value to None if there is an error
                first_jitai_value, first_jitai_timestamp, second_jitai_value, second_jitai_timestamp, first_jitai_value_2h, second_jitai_value_2h = "NO_DATA", "NO_DATA", "NO_DATA", "NO_DATA", "NO_DATA", "NO_DATA"
            # if first_jitai_timestamp is None, replace it with compliance_df['message_type'] == 'first_jitai']['status']
            if first_jitai_timestamp == "NO_DATA":
                first_jitai_timestamp = compliance_df[compliance_df['message_type'] == 'first_jitai']['status'].tolist()[0]
            # if second_jitai_timestamp is None, replace it with compliance_df['second_message']['status']
            if second_jitai_timestamp == "NO_DATA":
                second_jitai_timestamp = compliance_df[compliance_df['message_type'] == 'second_jitai']['status'].tolist()[0]
            # append the data to the dataframe
            result_df = result_df.append({'date': date, 
                                          'jitai1_timestamp': first_jitai_timestamp, 
                                          'jitai1_pa': first_jitai_value, 
                                          'jitai2_timestamp': second_jitai_timestamp, 
                                          'jitai2_pa': second_jitai_value,
                                          'jitai1_pa_2h': first_jitai_value_2h,
                                          'jitai2_pa_2h': second_jitai_value_2h}, 
                                          ignore_index=True)
        
        
        # check if the proximal folder is create in the report folder
        if not os.path.exists(self.ROOT_DIR + f'/reports/proximal/{userID}/{date}'):
            # if not, create the folder
            os.makedirs(self.ROOT_DIR + f'/reports/proximal/{userID}/{date}')
        # save the dataframe to csv
        result_df.to_csv(self.ROOT_DIR + f'/reports/proximal/{userID}/{date}/proximal.csv', index=False)
        return result_df
    
object = Proximal()
df = object.get_weekly_proximal_data('user06', '2023-04-12')
print(df)