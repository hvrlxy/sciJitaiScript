import json
import os

PATH_TO_INFO = '/home/hle5/mhealth-sci-dashboard/static/json/participant.json'
#read the json file
json_object = json.load(open(PATH_TO_INFO))

# get all the keys in the json file as participant ids
subjects = list(json_object.keys())

# create a dictionary of participant ids and their corresponding info
auc_dict = {}
for user_id in subjects:
    auc_dict[user_id] = json_object[user_id][0]['AUC']
    
start_dates_dict = {}
for user_id in subjects:
    start_dates_dict[user_id] = json_object[user_id][0]['start_date']
    
threshold_dict = {}
for user_id in subjects:
    threshold_dict[user_id] = int(json_object[user_id][0]['AUC'])
    
    
intervention_dict = {}
for user_id in subjects:
    intervention_dict[user_id] = json_object[user_id][0]['phase_2']
    
baseline_dict = {}
for user_id in subjects:
    baseline_dict[user_id] = json_object[user_id][0]['phase_1']
    
sustainability_dict = {}
for user_id in subjects:
    sustainability_dict[user_id] = json_object[user_id][0]['phase_3']
    
    
def delete_unzipped_files(userid):
    path = '/opt/sci_jitai/' + f"{userid}@scijitai_com/logs-watch/"
    # check if the path exists
    if not os.path.exists(path):
        return
    # list out all the folders
    folders = os.listdir(path)
    
    # for each folder, remove all non_zip files and folders inside it
    for folder in folders:
        # list all folders inside the folder
        datefolders = os.listdir(path + folder)
        for f in datefolders:
            # if the folder is not a zip file, remove it
            if not f.endswith('.zip'):
                try:
                    # need to sudo remove the folder
                    os.system(f"sudo rm -rf {path}{folder}/{f}")
                    print(f"Removed {path}{folder}/{f}")
                except Exception as e:
                    print(e)
                    print(f"Error removing {path}{folder}/{f}")