from schedule import Schedule
from unzip_all import UnZip
from prompts import Prompts
from auc import PlotSubject
from proximal import Proximal
import warnings
import os
import logging
import datetime
from compliance import Compliance
from globals import *
from PAstats import *

warnings.filterwarnings("ignore")

ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'
nums_day = 20

# get today's date as format YYYY-MM-DD
today = datetime.datetime.today().strftime('%Y-%m-%d')

logs_path = os.path.dirname(os.path.abspath(__file__)) + '/../logs/' + today

# create a logs folder of today's date if it doesn't exist
if not os.path.exists(logs_path):
    os.makedirs(logs_path)

# set up a logger for the main class
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# set the log file
log_file = logs_path + '/main.log'
# set the log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# set the log file handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

# put the file handler in the logger
logger.addHandler(file_handler)

# get yesterday's date as format YYYY-MM-DD
yesterday = datetime.datetime.today()
# get a list of the last 10 days from yesterday with format YYYY-MM-DD
last_10_days = [yesterday - datetime.timedelta(days=x) for x in range(0, nums_day)]
last_10_days = [day.strftime('%Y-%m-%d') for day in last_10_days]

# initialize the schedule class
schedule = Schedule()

# initialize the unzip class
unzip = UnZip()

# initialize the prompts class
prompts = Prompts()

# initialize the plot subject class
plot_subject = PlotSubject()

# initialize the compliance class
compliance = Compliance()

# initiliaze the proximal class
proximal = Proximal()
    
# unzip all the files
unzip.unzip_all(days=nums_day, subject_list=subjects)

# get the schedule for the last 10 days
for day in last_10_days:
    try:
        schedule.process_all_user(day, subjects)
    except Exception as e:
        logger.error('Error processing schedule for day: ' + day)
        continue

# get the prompts for the last 10 days
for day in last_10_days:
    try:
        prompts.process_all_user(day, subjects)
    except Exception as e:
        logger.error('Error processing prompts for day: ' + day)
        continue

last_x_days = [yesterday - datetime.timedelta(days=x) for x in range(0, nums_day + 7)]
last_x_days = [day.strftime('%Y-%m-%d') for day in last_x_days]
# plot the auc for the last 10 days
for day in last_10_days:
    for subject in subjects:
        try:
            plot_subject.plot_subject(subject, day, int(auc_dict[subject]))
        except Exception as e:
            logger.error('Error plotting subject: ' + subject + ' for day: ' + day)
            delete_unzipped_files(subject)
            continue
        
        try:
            compliance.save_compliance_report(subject, day)
        except Exception as e:
            logger.error('Error generating compliance report for subject: ' + subject + ' for day: ' + day)
            delete_unzipped_files(subject)
            continue
        
        delete_unzipped_files(subject)
        
# generate proximal report
for subject in subjects:
    try:
        print("compute proximal data for subject: " + subject)
        proximal.get_weekly_proximal_data(subject, today, start_dates_dict[subject], threshold_dict[subject])
    except Exception as e:
        logger.error('Error generating proximal report for subject: ' + subject + ' for day: ' + today)
        delete_unzipped_files(subject)
        continue
    
    delete_unzipped_files(subject)
 # get the baseline stats summary for all participants   
get_baseline_stats_for_all()
 
