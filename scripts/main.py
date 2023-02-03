from schedule import Schedule
from unzip_all import UnZip
from prompts import Prompts
from auc import PlotSubject
from auto_scp import AutoSCP
import warnings
import os
import logging
import datetime

warnings.filterwarnings("ignore")

khoury_id = 'hle5'
ppk_password = 'lemyha00'
ppk_path = '/Users/hale/.ssh/id_ed25519.ppk'

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
last_10_days = [yesterday - datetime.timedelta(days=x) for x in range(0, 3)]
last_10_days = [day.strftime('%Y-%m-%d') for day in last_10_days]
print(last_10_days)
# TODO: list of subjects
subjects = ['user01', 'user02', 'user03']

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

# get the data from the server for the last 10 days
for day in last_10_days:
    for subject in subjects:
        logger.info('Getting data for subject: ' + subject + ' for day: ' + day)
        # get logs-watch 
        try:
            auto_scp.get_logs_watch(subject, day)
        except Exception as e:
            logger.error('Error getting logs-watcher logs for day: ' + day)
            continue
        logger.info('Finished getting data for subject: ' + subject + ' for day: ' + day)
        try:
            auto_scp.get_data(subject, day)
        except Exception as e:
            logger.error('Error getting data for day: ' + day)
            continue
        try:
            auto_scp.get_logs(subject, day)
        except Exception as e:
            logger.error('Error getting logs for day: ' + day)
            continue
        # get data-watcher logs
        try:
            auto_scp.get_data_watch(subject, day)
        except Exception as e:
            logger.error('Error getting data-watcher logs for day: ' + day)
            continue
        print('Finished getting data for subject: ' + subject + ' for day: ' + day)

# unzip all the files from the last 10 days
unzip.unzip_all()

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

