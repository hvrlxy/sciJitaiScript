import json
import logging
import os
import datetime
import warnings
import pandas as pd

warnings.filterwarnings("ignore")

# get today's date as format YYYY-MM-DD
today = datetime.datetime.today().strftime('%Y-%m-%d')

logs_path = os.path.dirname(os.path.abspath(__file__)) + '/../..' + '/logs/' + today

# create a logs folder of today's date if it doesn't exist
if not os.path.exists(logs_path):
    os.makedirs(logs_path)

# set up a logger for the schedule class
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# set the log file
log_file = logs_path + '/jsonReader.log'
# set the log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# set the log file handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

# put the file handler in the logger
logger.addHandler(file_handler)

class JSONReader:
    def __init__(self, DATA_PATH, OUTPUT_PATH):
        self.DATA_PATH = DATA_PATH
        self.OUTPUT_PATH = OUTPUT_PATH

    def read(self, user_id):
        filename = user_id + '.json'
        logger.info('Reading ' + filename)

        # initialize the json object as None
        json_object = None
        with open(self.DATA_PATH + filename, 'r') as f:
            json_object = json.load(f)

        # check if the json object is empty
        if json_object is None:
            logger.error('JSON object is empty')
            return None
        
        #initialize the pandas dataframe
        df = pd.DataFrame(columns=["weekIndex", "dayIndex", "goalType", "firstMessage", "secondMessage"])
        # loop through json object inside the list
        for item in json_object:
            # get the weekIndex
            weekIndex = item['weekIndex']
            # get the daySchedules object
            daySchedules = item['daySchedules']
            # loop through the daySchedules object
            for daySchedule in daySchedules:
                dayIndex = daySchedule['dayIndex']
                goalType = daySchedule['goalType']
                firstMessage = daySchedule['firstMessage']
                secondMessage = daySchedule['secondMessage']

                #append the data to the dataframe
                df = df.append({"weekIndex": weekIndex, "dayIndex": dayIndex, "goalType": goalType, "firstMessage": firstMessage, "secondMessage": secondMessage}, ignore_index=True)
        
        # create the output folder if it doesn't exist
        if not os.path.exists(self.OUTPUT_PATH + "/xlsx"):
            os.makedirs(self.OUTPUT_PATH + "/xlsx")

        if not os.path.exists(self.OUTPUT_PATH + "/csv"):
            os.makedirs(self.OUTPUT_PATH + "/csv")
        #save the dataframe to an excel file
        df.to_excel(self.OUTPUT_PATH + "/xlsx/" + filename + '.xlsx', index=False)
        #save the dataframe to a csv file
        df.to_csv(self.OUTPUT_PATH + "/csv/" + filename + '.csv', index=False)
        # return the dataframe
        return df
        
    def read_all(self, user_ids):
        for user_id in user_ids:
            self.read(user_id)

    def read_into_one_xlsx(self, user_ids):
        #initilaize a list of dfs
        dfs = []
        for user_id in user_ids:
            df = self.read(user_id)
            dfs.append(df)
        
        # create a xlsx file
        writer = pd.ExcelWriter(self.OUTPUT_PATH + "/xlsx/" + 'all.xlsx', engine='xlsxwriter')
        # write each dataframe to a different sheet, and name the sheet with the user_id
        for i in range(len(dfs)):
            dfs[i].to_excel(writer, sheet_name=user_ids[i], index=False)

        # get the summary of the data
        summary = self.summary(user_ids)
        # write the summary to a different sheet
        summary.to_excel(writer, sheet_name='summary', index=False)

        # get weekly summary of the data
        weekly_summary = self.weekly_summary(user_ids)
        # write the weekly summary to a different sheet
        weekly_summary.to_excel(writer, sheet_name='weekly_summary', index=False)
        # save the file
        writer.save()

    def summary(self, user_ids):
        # initialize a dataframe
        final_df = pd.DataFrame(columns=["user_id", "NO_MESSAGE (exclude_baseline)", "ACHIEVED_MESSAGE", "TO_ACHIEVE_MESSAGE"])
        for user_id in user_ids:
            # read the df
            df = self.read(user_id)
            # get the number of NO_MESSAGE for the firstMessage excluding week 0,1,2
            noMessage = df[df['firstMessage'] == 'NO_MESSAGE'].shape[0] - 3*6
            # get the number of ACHIEVED_MESSAGE for the firstMessage
            achieved = df[df['firstMessage'] == 'ACHIEVED_MESSAGE'].shape[0]
            # get the number of TO_ACHIEVE_MESSAGE for the firstMessage
            toAchieve = df[df['firstMessage'] == 'TO_ACHIEVE_MESSAGE'].shape[0]
            # add the number of NO_MESSAGE for the secondMessage to the NO_MESSAGE
            noMessage += df[df['secondMessage'] == 'NO_MESSAGE'].shape[0] - 3*6
            # add the number of ACHIEVED_MESSAGE for the secondMessage to the ACHIEVED_MESSAGE
            achieved += df[df['secondMessage'] == 'ACHIEVED_MESSAGE'].shape[0]
            # add the number of TO_ACHIEVE_MESSAGE for the secondMessage to the TO_ACHIEVE_MESSAGE
            toAchieve += df[df['secondMessage'] == 'TO_ACHIEVE_MESSAGE'].shape[0]
            # append the data to the dataframe
            final_df = final_df.append({"user_id": user_id, "NO_MESSAGE (exclude_baseline)": noMessage, "ACHIEVED_MESSAGE": achieved, "TO_ACHIEVE_MESSAGE": toAchieve}, ignore_index=True)
        #return the dataframe
        return final_df
    
    def weekly_summary(self, user_ids):
        # create a pandas dataframe
        final_df = pd.DataFrame(columns=["user_id", "NO_MESSAGE", "ACHIEVED_MESSAGE", "TO_ACHIEVE_MESSAGE"])
        for user_id in user_ids:
            # read the df
            df = self.read(user_id)
            # loop through weekIndex from 3 to 24
            totalNoMessage = 0
            totalAchieved = 0
            totalToAchieve = 0

            for weekIndex in range(3, 25):
                # filter the dataframe by weekIndex
                week_df = df[df['weekIndex'] == weekIndex]
                # get the number of NO_MESSAGE for the firstMessage
                noMessage = week_df[week_df['firstMessage'] == 'NO_MESSAGE'].shape[0]
                # get the number of ACHIEVED_MESSAGE for the firstMessage
                achieved = week_df[week_df['firstMessage'] == 'ACHIEVED_MESSAGE'].shape[0]
                # get the number of TO_ACHIEVE_MESSAGE for the firstMessage
                toAchieve = week_df[week_df['firstMessage'] == 'TO_ACHIEVE_MESSAGE'].shape[0]
                # add the number of NO_MESSAGE for the secondMessage to the NO_MESSAGE
                noMessage += week_df[week_df['secondMessage'] == 'NO_MESSAGE'].shape[0]
                # add the number of ACHIEVED_MESSAGE for the secondMessage to the ACHIEVED_MESSAGE
                achieved += week_df[week_df['secondMessage'] == 'ACHIEVED_MESSAGE'].shape[0]
                # add the number of TO_ACHIEVE_MESSAGE for the secondMessage to the TO_ACHIEVE_MESSAGE
                toAchieve += week_df[week_df['secondMessage'] == 'TO_ACHIEVE_MESSAGE'].shape[0]
                # add three variables to the counter
                totalNoMessage += noMessage
                totalAchieved += achieved
                totalToAchieve += toAchieve

            # get the average of the three variables
            avgNoMessage = totalNoMessage / 22
            avgAchieved = totalAchieved / 22
            avgToAchieve = totalToAchieve / 22
            # append the data to the dataframe
            final_df = final_df.append({"user_id": user_id, "NO_MESSAGE": avgNoMessage, "ACHIEVED_MESSAGE": avgAchieved, "TO_ACHIEVE_MESSAGE": avgToAchieve}, ignore_index=True)

        #return the dataframe
        return final_df


# testing
DATA_PATH = os.path.dirname(os.path.abspath(__file__)) + '/../../data/schedules/'
OUTPUT_PATH = os.path.dirname(os.path.abspath(__file__)) + '/../../data/clean/'
jsonReader = JSONReader(DATA_PATH, OUTPUT_PATH)
jsonReader.read_into_one_xlsx(['user01', 'user03', 'user04', 'user06', 'user09'])