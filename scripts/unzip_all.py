import numpy as np
import pandas as pd
import datetime
import sys, os
import warnings
import pyzipper
import shutil
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
DATA_PATH = '/opt/sci_jitai/'

# remove directory with read-only files
def rm_dir_readonly(func, path, _):
    "Clear the readonly bit and reattempt the removal"
    os.chmod(path, stat.S_IWRITE)
    func(path)
    
    
class UnZip:
    def unzip_raw_auc_file(self, path, password = 'TIMEisthenewMICROTStudy-NUUSC'):
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

    def unzip_logs_folder(self, subject:str, date:str):
        '''
        unzip all the phone logs folder
        :param subject: str
        :param date: str
        :return: None
        '''

        # check if there is any zip file in the folder
        # path_logs = f'{DATA_PATH}{subject}@scijitai_com/logs/{date}/'
        path_logs_watch = f'{DATA_PATH}{subject}@scijitai_com/logs-watch/{date}/'
        # #add files in path_logs
        # files = os.listdir(path_logs)
        #add files in path_logs_watch
        files = os.listdir(path_logs_watch)

        # check if there is any files in the folder ending with zip
        if not any([file.endswith('.zip') for file in files]):
            print(f'No zip file found for {subject} on {date}')
            return
        # try:
        #     print("unzip file in logs folder")
        #     self.unzip_raw_auc_file(path_logs)
        # except Exception as e:
        #     print(traceback.format_exc())
        #     return ValueError(f'Error reading file: {path_logs} - Check if the file exists')

        try:
            print("unzip file in logs-watch folder")
            self.unzip_raw_auc_file(path_logs_watch)
        except Exception as e:
            print(traceback.format_exc())
            return ValueError(f'Error reading file: {path_logs_watch} - Check if the file exists')

        print("unzip file done")

    def unzip_logs_watch_folder(self, subject:str, date:str):
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
            self.unzip_raw_auc_file(path_logs_watch)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(f'Error reading file: {path_logs_watch} - Check if the file exists')
            return ValueError(f'Error reading file: {path_logs_watch} - Check if the file exists')

    def unzip_all(self, days = 3, subject_list = []):

        # get a list of all the dates from 10 days ago to today
        date_list = [datetime.datetime.now() - datetime.timedelta(days=x) for x in range(0, days)]
        date_list = [date.strftime('%Y-%m-%d') for date in date_list]

        for subject in subject_list:
            for date in date_list:
                try:
                    self.unzip_logs_folder(subject, date)
                except Exception as e:
                    print(f'Error unzipping {subject} on {date}')
                    pass

    def delete_all_unzip_files(self, subject, date):
        folders_type = ['logs', 'data', 'logs-watch', 'data-watch']
        for folder_type in folders_type:
            try: 
                path = f'{DATA_PATH}{subject}@scijitai_com/{folder_type}/{date}/'
                folders = os.listdir(path)
                # loop though all folders 
                for folder in folders:
                    # list all the files in the folder
                    try:
                        if not folder.endswith(".zip"):
                            shutil.rmtree(os.path.join(path, folder), onerror=rm_dir_readonly)
                    except Exception as e:
                        print(f'Error {e}, could not delete {folder} for {subject} on {date}')
                        continue
            except Exception as e:
                print(e)
                pass
            
