import numpy as np
import pandas as pd
import datetime
from unzip_all import UnZip
import os
from logger import Logger
from compliance import Compliance
import traceback
import warnings
from pa_algorithm import PAbouts

warnings.filterwarnings("ignore")

class Proximal:
    def __init__(self):
        self.logger = Logger().getLog("proximal")

        # initialize the project root
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'

        # initialize the data path
        self.DATA_PATH = '/opt/sci_jitai/'

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
        if row['reprompt2_epoch'] != "N/A":
            return row['reprompt2_epoch']
        # if it is N/A, check if "reprompt1" column is not N/A, if it is not, return the value
        elif row['reprompt1_epoch'] != "N/A":
            return row['reprompt1_epoch']
        # if it is N/A, check if "prompt" column is not N/A, if it is not, return the value
        elif row['prompt_epoch'] != "N/A":
            return row['prompt_epoch']
        elif row['scheduled_prompt_epoch'] != "N/A":
            return row['scheduled_prompt_epoch']
        else:
            return 0

    def get_jitai_info(self, userID, date, compliance_df):
        '''
        This function will get the jitai info for the given user and date
        It will return two variables: time the first jitai is answered
        and time the second jitai is answered.
        If the jitai is not shown or answered, it will return 0
        Timestamp return is in epoch millis
        '''
        # filter the compliance dataframe by message_type and only get the first_jitai and second_jitai
        jitai_df = compliance_df[compliance_df['message_type'].isin(['first_jitai', 'second_jitai'])]
        if len(jitai_df[jitai_df['message_type'] == 'first_jitai']) == 0:
            first_jitai = self.get_answered_time(None)
        else:
            first_jitai = int(self.get_answered_time(jitai_df[jitai_df['message_type'] == 'first_jitai'].iloc[0]))
        
        if len(jitai_df[jitai_df['message_type'] == 'second_jitai']) == 0:
            second_jitai = self.get_answered_time(None)
        else:
            second_jitai = int(self.get_answered_time(jitai_df[jitai_df['message_type'] == 'second_jitai'].iloc[0]))
        
        return first_jitai, second_jitai
    
    def process_proximal_with_timestamp(self, pa_df, epoch):
        '''
        This function will process the proximal data for the given user, date and epoch
        It will return the proximal value for the given epoch and the timestamp of the proximal value
        '''
        if epoch == 0:
            return None, None
        if pa_df is None:
            return None, None
        # turn the values in pa_df to int
        pa_df['epoch'] = pa_df['epoch'].astype(int)
        #remove all rows with epoch lower than the epoch 
        pa_df = pa_df[pa_df['epoch'] >= epoch]
        # get the row with epoch higer but closest to the given epoch
        pa_row = pa_df.iloc[(pa_df['epoch']-epoch).abs().argsort()[:1]]
        try:
            # return the PA value and the epoch
            return pa_row['PA'].iloc[0], pa_row['epoch'].iloc[0]
        except Exception:
            return None, None
    
    def get_total_pa_at_date(self, userID, date):
        # get the battery log file in the Common folder
        pa_path = self.DATA_PATH + f'{userID}@scijitai_com/logs-watch/{date}/Common/Watch-Watch-PABoutInfo.log.csv'
        # if there is no such file, return 0
        if os.path.isfile(pa_path) == False:
            return 0
        # read the battery log file
        try:
            pa_df = pd.read_csv(pa_path, names=["timestamp", "info", "value"])
            #get the "value" column at the last row
            pa_value = pa_df['value'].iloc[-1]
            # parse by : and get the second value
            pa_value = pa_value.split(': ')[1]
            return pa_value
        except:
            return None

    def get_proximal(self, userID, date, compliance_df, baseline = 2000):
        obj = PAbouts(userID, date)
        # generate the epoch list, starting with the first epoch of the date to the last epoch of the day, with 1.5 minutes increment
        # get the first epoch by converting the date to epoch in milliseconds
        first_epoch = int(datetime.datetime.strptime(date, '%Y-%m-%d').timestamp())*1000
        # create a list of epochs by incrementing the first epoch by 1.5 minutes until the last epoch of the day
        epoch_list = list(range(first_epoch, first_epoch + 24*60*60*1000, 90*1000))
        # get the pa_df
        pa_df, total_pa = obj.calculate_PA(epoch_list, baseline)
        print(total_pa)
        if compliance_df is None:
            print("reach if statement")
            return "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", total_pa
        first_jitai, second_jitai = self.get_jitai_info(userID, date, compliance_df)
        # process the first jitai
        first_jitai_value, first_jitai_timestamp = self.process_proximal_with_timestamp(pa_df, first_jitai)
        print(first_jitai_value, first_jitai_timestamp)
        # process the second jitai
        second_jitai_value, second_jitai_timestamp = self.process_proximal_with_timestamp(pa_df, second_jitai)
        print(second_jitai_value, second_jitai_timestamp)
        if first_jitai != 0:
            # get the value 2 hours after the first jitai
            first_jitai_value_2h,_ = self.process_proximal_with_timestamp(pa_df, first_jitai + 2*60*60*1000)
        else:
            first_jitai_value_2h = None
            
        if second_jitai != 0:
            # get the value 2 hours after the second jitai
            second_jitai_value_2h,_ = self.process_proximal_with_timestamp(pa_df, second_jitai + 2*60*60*1000)
        else:
            second_jitai_value_2h = None
        # return the first jitai value, first jitai timestamp, second jitai value, second jitai timestamp
        return first_jitai_value, first_jitai_timestamp, second_jitai_value, second_jitai_timestamp, first_jitai_value_2h, second_jitai_value_2h, total_pa
    
    def get_compliance_report_csv(self, userID, date):
        path = f"/home/hle5/sciJitaiScript/reports/compliance/{userID}/{date}.csv"
        try:
            # read the csv file into a pandas dataframe
            compliance_df = pd.read_csv(path)
            #remove the Unnamed column
            compliance_df = compliance_df.loc[:, ~compliance_df.columns.str.contains('^Unnamed')]
            # turn all NaN to N/A
            compliance_df = compliance_df.fillna('N/A')
            return compliance_df
        except Exception as e:
            return None
        
    def get_weekly_proximal_data(self, userID, last_date, start_date, baseline = 2000):
        # convert the date to datetime
        last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d')
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        # initialize a dataframe
        result_df = pd.DataFrame(columns=['date', 
                                          'goal_type',
                                          'jit1_timestamp', 
                                          'jit1_epoch',
                                          'jit1_type',
                                          'jit1_status',
                                          'jit1_pa', 
                                            'jit1_pa_2h',
                                          'jit2_timestamp', 
                                            'jit2_epoch',
                                            'jit2_type',
                                            'jit2_status',
                                          'jit2_pa',
                                            'jit2_pa_2h',
                                            "total_pa"])
        
        # generate all the dates from start_date to last_date in format YYYY-MM-DD
        date_list = [start_date + datetime.timedelta(days=x) for x in range((last_date-start_date).days + 1)]
        # loop from 6 days ago to 0 days ago
        for date in date_list:
            try:
                # convert date to string
                date = date.strftime('%Y-%m-%d')
                compliance_df = self.get_compliance_report_csv(userID, date)
                # get the proximal data
                first_jitai_value, first_jitai_epoch, second_jitai_value, second_jitai_epoch, first_jitai_value_2h, second_jitai_value_2h, total_pa = self.get_proximal(userID, date, compliance_df, baseline)
                if first_jitai_epoch != "N/A" and first_jitai_epoch is not None:
                    first_jitai_timestamp = datetime.datetime.fromtimestamp(first_jitai_epoch/1000).strftime('%H:%M:%S')
                else:
                    first_jitai_timestamp = "N/A"
                if second_jitai_epoch != "N/A" and second_jitai_epoch is not None:
                    second_jitai_timestamp = datetime.datetime.fromtimestamp(second_jitai_epoch/1000).strftime('%H:%M:%S')
                else:
                    second_jitai_timestamp = "N/A"
            except Exception as e:
                print("Error processing proximal data for date: " + date + "with error: " + str(e))
                print(traceback.format_exc())
                # set all value to None if there is an error
                first_jitai_value, first_jitai_timestamp, second_jitai_value, second_jitai_timestamp, first_jitai_value_2h, second_jitai_value_2h, total_pa = "NO_DATA", "NO_DATA", "NO_DATA", "NO_DATA", "NO_DATA", "NO_DATA", "NO_DATA"
                first_jitai_epoch, second_jitai_epoch = "NO_DATA", "NO_DATA"
                compliance_df = None
            
            if compliance_df is not None:
                jitai1_status = compliance_df[compliance_df['message_type'] == 'first_jitai']['status'].tolist()[0]
                jitai2_status = compliance_df[compliance_df['message_type'] == 'second_jitai']['status'].tolist()[0]
                goal_type = compliance_df[compliance_df['message_type'] == 'goal_settings']['message_note'].tolist()[0]
                jit1_type = compliance_df[compliance_df['message_type'] == 'first_jitai']['message_note'].tolist()[0]
                jit2_type = compliance_df[compliance_df['message_type'] == 'second_jitai']['message_note'].tolist()[0]
            else:
                jitai1_status, jitai2_status, goal_type, jit1_type, jit2_type = "NO_DATA", "NO_DATA", "NO_DATA", "NO_DATA", "NO_DATA"

            # append the data to the dataframe
            result_df = result_df.append({'date': date, 
                                            'goal_type': goal_type,
                                          'jit1_timestamp': first_jitai_timestamp, 
                                            'jit1_epoch': first_jitai_epoch,
                                            'jit1_type': jit1_type,
                                            'jit1_status': jitai1_status,
                                          'jit1_pa': first_jitai_value, 
                                          'jit2_timestamp': second_jitai_timestamp, 
                                            'jit2_epoch': second_jitai_epoch,
                                            'jit2_type': jit2_type,
                                            'jit2_status': jitai2_status,
                                          'jit2_pa': second_jitai_value,
                                          'jit1_pa_2h': first_jitai_value_2h,
                                          'jit2_pa_2h': second_jitai_value_2h,
                                          "total_pa": total_pa}, 
                                          ignore_index=True)
            compliance_df = None
        
        # check if the proximal folder is create in the report folder
        if not os.path.exists(self.ROOT_DIR + f'/reports/proximal/'):
            # if not, create the folder
            os.makedirs(self.ROOT_DIR + f'/reports/proximal/')
        # save the dataframe to csv
        result_df.to_csv(self.ROOT_DIR + f'/reports/proximal/{userID}.csv', index=False)
        return result_df

# print(Proximal().get_weekly_proximal_data("scijitai_06", "2023-08-15", "2023-08-07", 2000))