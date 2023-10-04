import os
from unzip_all import UnZip
def get_watch_logs_for_pid(subject_id, date, unzip = True):
    try:
        os.system(f"/home/backups/google-cloud-sdk/bin/gsutil -m rsync -r gs://sci-jitai.appspot.com/sci_jitai/{subject_id}@scijitai_com/logs-watch/{date} /home/hle5/sciJitaiScript/logs-watch/")
        if unzip:
            unzip = UnZip()
            unzip.unzip_raw_auc_file(f"/home/hle5/sciJitaiScript/logs-watch/")
    except Exception as e:
        print(e)
        print(f"Error syncing watch logs for subject {subject_id} on {date}")        

def delete_logs_watch_folder():
    # delete all the files and folders in the logs-watch folder
    try:
        os.system(f"rm -rf /home/hle5/sciJitaiScript/logs-watch/*")
    except Exception as e:
        print(e)
        print(f"Error deleting files in logs-watch folder")
        
# delete_logs_watch_folder()
# get_watch_logs_for_pid("scijitai_13", "2023-08-29")