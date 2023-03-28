import numpy as np
import pandas as pd
import datetime
from unzip_all import UnZip
import os
from logger import Logger
from schedule import Schedule
from prompts import Prompts
import traceback
import warnings

warnings.filterwarnings("ignore")

class Compliance:
    def __init__(self):
        self.logger = Logger().get_logger("compliance")

        # initialize the project root
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'

        # initialize the data path
        self.DATA_PATH = self.ROOT_DIR + '/data/raw/'

        # initialize the schedule path
        self.SCHEDULE_PATH = self.ROOT_DIR + '/reports/schedule/'

        # initialize the unzip class
        self.unzip = UnZip()

        # initialize the schedule class
        self.schedule = Schedule()

        # initialize the prompts class
        self.prompts = Prompts()

    def generate_user_compliance_report(self, user, date):
        schedule_df = self.schedule.process_schedule_generation(user, date)
        if schedule_df is None:
            return None
        # get the user's prompts data
        prompts_df = self.prompts.process_prompts_generation(user, date)
        if prompts_df is None:
            # create an empty dataframe
            prompts_df = pd.DataFrame(columns=['timestamp', 'epoch', 'message_type', 'status'])
        
        #convert message_type "jitai1" to "first_message"
        prompts_df['message_type'] = prompts_df['message_type'].replace('jitai1', 'first_message')
        #convert message_type "jitai2" to "second_message"
        prompts_df['message_type'] = prompts_df['message_type'].replace('jitai2', 'second_message')
        #convert message_type "eod" to "eod_message"
        prompts_df['message_type'] = prompts_df['message_type'].replace('eod', 'eod_message')

        # merge the schedule and prompts dataframes, keeping all rows from both
        merged_df = pd.merge(schedule_df, prompts_df, how='outer', on=['timestamp', 'epoch', 'message_type'])
        
        return merged_df

    
