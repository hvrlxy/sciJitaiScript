import os, sys 
import logging
import datetime

class Logger:
    def getLog(self, TAG):
        # get today's date
        today = datetime.datetime.today().strftime('%Y-%m-%d') 
        logs_path = os.path.dirname(os.path.abspath(__file__)) + '/../..' + '/logs/' + today

        # create a logs folder of today's date if it doesn't exist
        if not os.path.exists(logs_path):
            os.makedirs(logs_path)

        # set up a logger for the main class
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        # set the log file
        log_file = logs_path + f'/{TAG}.log'
        # set the log format
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # set the log file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        # put the file handler in the logger
        logger.addHandler(file_handler)

        # return the logger
        return logger