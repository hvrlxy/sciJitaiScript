from schedule import Schedule
from unzip_all import UnZip
from prompts import Prompts
from auc import PlotSubject
from auto_scp import AutoSCP
from battery_connectivity import BatteryConnectivity
from proximal import Proximal
import warnings
import os
import logging
import datetime
import traceback
from compliance import Compliance

warnings.filterwarnings("ignore")

ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'
khoury_id = 'hle5'
ppk_password = 'lemyha00'
ppk_path = ROOT_DIR + '/ssh/id_ed25519.ppk'
nums_day = 4

# get today's date as format YYYY-MM-DD
today = datetime.datetime.today().strftime('%Y-%m-%d')

logs_path = os.path.dirname(os.path.abspath(__file__)) + '/..' + '/logs/' + today

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
# TODO: list of subjects
subjects = ['user01', 'user02', 'user03', 'user04', 'user05', 'user06', 'user07', 'user10', 'user09', 'user08', 'use003', 'use004']
# initialize the auto scp class
auto_scp = AutoSCP(khoury_id, ppk_password, ppk_path)

# initialize the schedule class
schedule = Schedule()

# initialize the unzip class
unzip = UnZip()

# initialize the prompts class
prompts = Prompts()

# initialize the plot subject class
plot_subject = PlotSubject()

# initialize the battery class
battery = BatteryConnectivity()

# initialize the compliance class
compliance = Compliance()

# initiliaze the proximal class
proximal = Proximal()

# get the data from the server for the last 10 days
for day in last_10_days:
    for subject in subjects:
        logger.info('Getting data for subject: ' + subject + ' for day: ' + day)
        # get logs-watch 
        try:
            auto_scp.get_logs_watch(subject, day)
        except Exception as e:
            logger.error('Error getting logs-watcher logs for day: ' + day)
            logger.error(traceback.format_exc())
            print('Error getting logs-watcher logs for day: ' + day)
        logger.info('Finished getting data for subject: ' + subject + ' for day: ' + day)
        try:
            auto_scp.get_data(subject, day)
        except Exception as e:
            logger.error('Error getting data for day: ' + day)
            logger.error(traceback.format_exc())
            print('Error getting logs-watcher logs for day: ' + day)
        try:
            auto_scp.get_logs(subject, day)
        except Exception as e:
            logger.error('Error getting logs for day: ' + day)
            logger.error(traceback.format_exc())
            print('Error getting logs-watcher logs for day: ' + day)
        # get data-watcher logs
        try:
            auto_scp.get_data_watch(subject, day)
        except Exception as e:
            logger.error('Error getting data-watcher logs for day: ' + day)
            logger.error(traceback.format_exc())
        print('Finished getting data for subject: ' + subject + ' for day: ' + day)
    # time.sleep(5)

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

# plot the auc for the last 10 days
for day in last_10_days:
    for subject in subjects:
        try:
            plot_subject.plot_subject(subject, day)
        except Exception as e:
            logger.error('Error plotting subject: ' + subject + ' for day: ' + day)
            continue

# generate compliance report for the last 10 days
for day in last_10_days:
    for subject in subjects:
        try:
            compliance.save_compliance_report(subject, day)
        except Exception as e:
            logger.error('Error generating compliance report for subject: ' + subject + ' for day: ' + day)
            continue

# generate proximal report for today
for subject in subjects:
    try:
        proximal.get_weekly_proximal_data(subject, today)
    except Exception as e:
        logger.error('Error generating proximal report for subject: ' + subject + ' for day: ' + today)
        continue

# upload the reports to the server
auto_scp.upload_reports()