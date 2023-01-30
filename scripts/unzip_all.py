import numpy as np
import pandas as pd
import datetime
import sys, os
import warnings
import pyzipper
import re
import logging
import traceback

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
log_file = logs_path + '/unzip.log'
# set the log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# set the log file handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

# put the file handler in the logger
logger.addHandler(file_handler)

# Initialize project root
#  TODO: change this to the root directory of the project in your local machine/server
ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'
DATA_PATH = ROOT_DIR + '/data/raw/'

def unzip_raw_auc_file(path, password = 'TIMEisthenewMICROTStudy-NUUSC'):
    '''
    unzip the raw auc file
    :param path: str
    :return: None
    '''
    # get all the files in the raw auc path
    raw_auc_files = os.listdir(path)
    # only grab the zip file
    zip_files = [file for file in raw_auc_files if pyzipper.is_zipfile(path + file)]
    # unzip all the zipfiles in the olist
    for file in zip_files:
        with pyzipper.AESZipFile(path + file, 'r', compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES) as zip_ref:
            # make a new directory to put the unzipped file
            new_dir = path + file[:-4]
            try:
                os.mkdir(new_dir)
            except:
                #  the directory already exists, so we can just pass
                pass
            
            # go to the new directory
            os.chdir(new_dir)
            # get the path of the folder to put the unzipped file
            new_path = new_dir + '/'
            # unzip the file
            try:
                zip_ref.extractall(pwd=str.encode(password))
            except:
                # if there is an error, then we can just pass
                logger.error(traceback.format_exc())
                logger.error(f'Error unzipping {path}{file}')
                pass
            zip_ref.close()
            # move back to the raw auc path
            os.chdir(path)
    logger.info(f"unzip file at {path} done")

def unzip_logs_folder(subject:str, date:str):
    '''
    unzip all the phone logs folder
    :param subject: str
    :param date: str
    :return: None
    '''

    # check if there is any zip file in the folder
    path_logs = f'{DATA_PATH}{subject}@scijitai_com/logs/{date}/'
    path_logs_watch = f'{DATA_PATH}{subject}@scijitai_com/logs-watch/{date}/'

    path_data = f'{DATA_PATH}{subject}@scijitai_com/data/{date}/'
    path_data_watch = f'{DATA_PATH}{subject}@scijitai_com/data-watch/{date}/'
    #add files in path_logs
    files = os.listdir(path_logs)
    #add files in path_logs_watch
    files += os.listdir(path_logs_watch)
    #add files in path_data
    files += os.listdir(path_data)
    #add files in path_data_watch
    files += os.listdir(path_data_watch)

    # check if there is any files in the folder ending with zip
    if not any([file.endswith('.zip') for file in files]):
        print(f'No zip file found for {subject} on {date}')
        return
    try:
        print("unzip file in logs folder")
        unzip_raw_auc_file(path_logs)
    except Exception as e:
        print(traceback.format_exc())
        return ValueError(f'Error reading file: {path_logs} - Check if the file exists')

    try:
        print("unzip file in logs-watch folder")
        unzip_raw_auc_file(path_logs_watch)
    except Exception as e:
        print(traceback.format_exc())
        return ValueError(f'Error reading file: {path_logs_watch} - Check if the file exists')

    try:
        print("unzip file in data folder")
        unzip_raw_auc_file(path_data)
    except Exception as e:
        print(traceback.format_exc())
        return ValueError(f'Error reading file: {path_data} - Check if the file exists')

    try:
        print("unzip file in data-watch folder")
        unzip_raw_auc_file(path_data_watch)
    except Exception as e:
        print(traceback.format_exc())
        return ValueError(f'Error reading file: {path_data_watch} - Check if the file exists')

    print("unzip file done")

def unzip_logs_watch_folder(subject:str, date:str):
    '''
    unzip all the phone logs folder
    :param subject: str
    :param date: str
    :return: None
    '''

    # check if there is any zip file in the folder
    path_logs_watch = f'{DATA_PATH}{subject}@scijitai_com/logs-watch/{date}/'
    #add files in path_logs_watch
    files = os.listdir(path_logs_watch)

    # check if there is any files in the folder ending with zip
    if not any([file.endswith('.zip') for file in files]):
        logger.info(f'No zip file found for {subject} on {date}')
        return
    try:
        logger.info(f'Unzipping logs-watch folder for {subject} on {date}')
        unzip_raw_auc_file(path_logs_watch)
    except Exception as e:
        logger.error(traceback.format_exc())
        print(traceback.format_exc())
        logger.error(f'Error reading file: {path_logs_watch} - Check if the file exists')
        return ValueError(f'Error reading file: {path_logs_watch} - Check if the file exists')


def unzip_all():
    subject_list = ['user01', 'user02', 'user03', 'user04', 'user05', 'user06', 'user07', 'user08', 'user09', 'user10']

    # get a list of all the dates from 10 days ago to today
    date_list = [datetime.datetime.now() - datetime.timedelta(days=x) for x in range(0, 10)]
    date_list = [date.strftime('%Y-%m-%d') for date in date_list]
    print(date_list)

    for subject in subject_list:
        for date in date_list:
            try:
                unzip_logs_folder(subject, date)
            except Exception as e:
                print(traceback.format_exc())
                print(f'Error unzipping {subject} on {date}')
                pass

unzip_all()