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
import traceback
from rsync import *

warnings.filterwarnings("ignore")

ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'
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

def process_data_pid_at_date(pid, date, recomputed = False):
    # check if computed
    if (not recomputed) and check_if_computed(pid, date):
        logger.info('Data for pid: ' + pid + ' at date: ' + date + ' has already been computed')
        return
    delete_logs_watch_folder()
    try:
        #rsync the watch logs for the pid at date
        get_watch_logs_for_pid(pid, date)
        computed = True
        try:
            # first compute the schedule for the pid at date
            computed = schedule.process_schedule_pid_at_date(pid, date) and computed
        except Exception as e:
            logger.error('main.py::Error processing schedule for pid: ' + pid + ' at date: ' + date)
            
        try:
            # next compute the prompts for the pid at date
            computed = prompts.process_user_at_date(pid, date) and computed
        except Exception as e:
            logger.error('main.py::Error processing prompts for pid: ' + pid + ' at date: ' + date)
            
        try:
            # next compute the compliance for the pid at date
            computed = (compliance.save_compliance_report(pid, date) is not None) and computed
        except Exception as e:
            logger.error('main.py::Error processing compliance for pid: ' + pid + ' at date: ' + date)
        # next compute the plot for the pid at date
        try:
            plot_subject.plot_subject(pid, date, int(auc_dict[pid]))
        except Exception as e:
            logger.error('main.py::Error processing plot for pid: ' + pid + ' at date: ' + date)
            
        # check if the Common folder exists in the logs folder
        if os.path.exists('/home/hle5/sciJitaiScript/logs-watch/Common'):
            # add the pid and date to the computed list
            add_date_to_computed(pid, date)
            # print the computed message
            print('Data for pid: ' + pid + ' at date: ' + date + ' has been added to computed json')
    except Exception as e:
        print('main.py::Error processing data for pid: ' + pid + ' at date: ' + date)
    # delete_logs_watch_folder()

# recomputed_pid = []
# # subjects = ['scijitai_31']
# for subject in subjects:
#     #get the start date for the subject
#     start_date = get_subject_start_date(subject)
#     #get the list of date from start date to yesterday
#     date_list = pd.date_range(start_date, yesterday).tolist()
#     # convert the date list to string
#     date_list = [date.strftime('%Y-%m-%d') for date in date_list]
#     #reverse the date list
#     # date_list.reverse()
#     for date in date_list:
#         try:
#             process_data_pid_at_date(subject, date, recomputed=subject in recomputed_pid)
#         except Exception as e:
#             #print the traceback
#             print('main.py::Error processing data for pid: ' + subject + ' at date: ' + date)
#             print(traceback.format_exc())
    
for subject in subjects:
    #get the start date for the subject
    start_date = get_subject_start_date(subject)
    # convert yesterday to string
    end_date = yesterday.strftime('%Y-%m-%d')
    Proximal().get_weekly_proximal_data(subject, end_date, start_date, threshold_dict[subject])

get_baseline_stats_for_all()
