import json
import os
import datetime
import pandas as pd

PATH_TO_INFO = '/home/hle5/mhealth-sci-dashboard/static/json/participant.json'
PATH_TO_COMPUTED = '/home/hle5/sciJitaiScript/reports/computed.json'
#read the json file
json_object = json.load(open(PATH_TO_INFO))

# get all the keys in the json file as participant ids
subjects = list(json_object.keys())
# reverse the list
subjects.reverse()

# create a dictionary of participant ids and their corresponding info
auc_dict = {}
for user_id in subjects:
    auc_dict[user_id] = json_object[user_id]['0']['AUC']
    
start_dates_dict = {}
for user_id in subjects:
    start_dates_dict[user_id] = json_object[user_id]['0']['start_date']
    
threshold_dict = {}
for user_id in subjects:
    threshold_dict[user_id] = int(json_object[user_id]['0']['AUC'])
    
    
intervention_dict = {}
for user_id in subjects:
    intervention_dict[user_id] = json_object[user_id]['0']['phase_2']

# print(intervention_dict)
baseline_dict = {}
for user_id in subjects:
    baseline_dict[user_id] = json_object[user_id]['0']['phase_1']

sustainability_dict = {}
for user_id in subjects:
    sustainability_dict[user_id] = json_object[user_id]['0']['phase_3']
    
def add_date_to_computed(pid, date):
    # read the json file
    json_object = json.load(open(PATH_TO_COMPUTED))
    # check if the date is in the pid's list of computed dates
    if date not in json_object[pid]:
        # if not, add it
        json_object[pid].append(date)
        # write to the json file
        with open(PATH_TO_COMPUTED, 'w') as outfile:
            json.dump(json_object, outfile)
            # close the json file
            outfile.close()
    else:
        # if yes, do nothing
        pass
    
def check_if_computed(pid, date):
    # read the json file
    try:
        json_object = json.load(open(PATH_TO_COMPUTED))
    except Exception as e:
        # create empty json object
        json_object = {}
    # check if pid is in the json object
    if pid not in json_object:
        # if not, add it
        json_object[pid] = []
        # write to the json file
        with open(PATH_TO_COMPUTED, 'w') as outfile:
            json.dump(json_object, outfile)
            # close the json file
            outfile.close()
            
    # check if the date is in the pid's list of computed dates
    if date in json_object[pid]:
        # if yes, return True
        return True
    else:
        # if not, return False
        return False
    
def get_subject_start_date(pid):
    return start_dates_dict[pid]

def get_subject_phase_1_end_date(pid):
    if baseline_dict[pid] != "N/A":
        return baseline_dict[pid]
    else:
        # return today's date
        return datetime.datetime.today().strftime('%Y-%m-%d')
    
def get_baseline_date_list(pid):
    start_date = get_subject_start_date(pid)
    phase_2 = get_subject_phase_1_end_date(pid)
    # turn the start date into a datetime object
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    #turn the end date into a datetime object
    phase_2 = datetime.datetime.strptime(phase_2, '%Y-%m-%d')
    # return the list of dates from start date to phase 2
    date_list = pd.date_range(start_date, phase_2).tolist()
    # turn the date list into a list of strings
    date_list = [date.strftime('%Y-%m-%d') for date in date_list]
    return date_list
    
# print(get_baseline_date_list("scijitai_24"))