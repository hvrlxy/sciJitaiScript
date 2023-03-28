from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os, sys 

#chdir to the directory of the script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/../../'

gauth = GoogleAuth()           

gauth.LoadCredentialsFile(ROOT_DIR + "scripts/drive/credentials.txt")
if gauth.credentials is None:
    # Authenticate if they're not there
    gauth.LocalWebserverAuth()
elif gauth.access_token_expired:
    # Refresh them if expired
    gauth.Refresh()
else:
    # Initialize the saved creds
    gauth.Authorize()
# Save the current credentials to a file
gauth.SaveCredentialsFile(ROOT_DIR + "scripts/drive/credentials.txt")
drive = GoogleDrive(gauth)  

# create a list of all folders in local machine that we want to upload
folders = [ROOT_DIR + 'figures', ROOT_DIR + 'reports']

def create_folder(folder_name, parent_folder):
    #check if the folder_name already exists in the parent folder
    folder_list = drive.ListFile({'q': f"'{parent_folder['id']}' in parents and trashed=false"}).GetList()
    for folder in folder_list:
        if folder['title'] == folder_name:
            return folder
    #create the folder with the same name inside the parent folder
    new_folder = drive.CreateFile({'title' : folder_name, "parents": [{"kind": "drive#fileLink", "id": parent_folder['id']}], "mimeType": "application/vnd.google-apps.folder"})
    new_folder.Upload()
    print(f'Folder created: {new_folder["title"]}')
    return new_folder

def upload_recursively(folder, parent_folder):
    if ".DS_Store" in folder or "README.md" in folder or '.html' in folder:
        return
    # get the name of the folder
    folder_name = folder.split('/')[-1]
    #check if folder is a folder
    if os.path.isdir(folder):
        # get all files in the folder
        files = os.listdir(folder)
        #create the folder with the same name inside the parent folder
        new_folder = create_folder(folder_name, parent_folder)
        # recursively call the function for all files in the folder
        for file in files:
            upload_recursively(f'{folder}/{file}', new_folder)
    elif os.path.isfile(folder):
        # if the file is not a folder, upload it to the parent folder
        file = drive.CreateFile({'title' : folder_name, "parents": [{"kind": "drive#fileLink", "id": parent_folder['id']}]})
        file.SetContentFile(folder)
        file.Upload()

#print all folder from the root
file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
# check if the "SCI JITAI" folder exists in the drive
for file in file_list:
    is_exist = False
    if file['title'] == 'SCI JITAI':
        is_exist = True
        SCI_JITAI_FOLDER = file
        break
if not is_exist:
    #create the folder with the same name inside the parent folder
    new_folder = drive.CreateFile({'title' : 'SCI JITAI', "parents": [{"kind": "drive#fileLink", "id": 'root'}], "mimeType": "application/vnd.google-apps.folder"})
    new_folder.Upload()
    SCI_JITAI_FOLDER = new_folder

# upload all folders in the list
for folder in folders:
    upload_recursively(folder, SCI_JITAI_FOLDER)

# chdir back to the root
os.chdir(ROOT_DIR)