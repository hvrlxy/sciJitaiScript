import paramiko
from scp import SCPClient
import os

class AutoSCP:
    def __init__(self, khoury_id, ppk_password, ppk_path):
        self.khoury_id = khoury_id
        self.ppk_password = ppk_password
        self.ppk_path = ppk_path
        self.data_path = os.path.dirname(os.path.abspath(__file__)) + '/../data/raw/'

    def get_logs_watch(self, subject_id, date):
        ssh = paramiko.SSHClient()

        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect('mhealth-sci.khoury.northeastern.edu', 
                    username=self.khoury_id, 
                    password=self.ppk_password, 
                    key_filename=self.ppk_path)

        scp = SCPClient(ssh.get_transport())

        # check if the destination folder exists
        if not os.path.exists(f"{self.data_path}{subject_id}@scijitai_com/logs-watch/"):
            os.makedirs(f"{self.data_path}{subject_id}@scijitai_com/logs-watch/")

        scp.get(f"/opt/sci_jitai/{subject_id}@scijitai_com/logs-watch/{date}", 
                f"{self.data_path}{subject_id}@scijitai_com/logs-watch", 
                recursive=True)

        scp.close()
        ssh.close()

    def get_logs(self, subject_id, date):
        ssh = paramiko.SSHClient()

        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect('mhealth-sci.khoury.northeastern.edu', 
                    username=self.khoury_id, 
                    password=self.ppk_password, 
                    key_filename=self.ppk_path)

        scp = SCPClient(ssh.get_transport())

        # check if the destination folder exists
        if not os.path.exists(f"{self.data_path}{subject_id}@scijitai_com/logs/"):
            os.makedirs(f"{self.data_path}{subject_id}@scijitai_com/logs/")

        scp.get(f"/opt/sci_jitai/{subject_id}@scijitai_com/logs/{date}", 
                f"{self.data_path}{subject_id}@scijitai_com/logs", 
                recursive=True)
        scp.close()
        ssh.close()
    def get_data(self, subject_id, date):
        ssh = paramiko.SSHClient()

        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect('mhealth-sci.khoury.northeastern.edu', 
                    username=self.khoury_id, 
                    password=self.ppk_password, 
                    key_filename=self.ppk_path)

        scp = SCPClient(ssh.get_transport())

        # check if the destination folder exists
        if not os.path.exists(f"{self.data_path}{subject_id}@scijitai_com/data/"):
            os.makedirs(f"{self.data_path}{subject_id}@scijitai_com/data/")

        scp.get(f"/opt/sci_jitai/{subject_id}@scijitai_com/data/{date}", 
                f"{self.data_path}{subject_id}@scijitai_com/data", 
                recursive=True)
        scp.close()
        ssh.close()
    def get_data_watch(self, subject_id, date):
        ssh = paramiko.SSHClient()

        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect('mhealth-sci.khoury.northeastern.edu', 
                    username=self.khoury_id, 
                    password=self.ppk_password, 
                    key_filename=self.ppk_path)

        scp = SCPClient(ssh.get_transport())

        # check if the destination folder exists
        if not os.path.exists(f"{self.data_path}{subject_id}@scijitai_com/data-watch/"):
            os.makedirs(f"{self.data_path}{subject_id}@scijitai_com/data-watch/")

        scp.get(f"/opt/sci_jitai/{subject_id}@scijitai_com/data-watch/{date}", 
                f"{self.data_path}{subject_id}@scijitai_com/data-watch", 
                recursive=True)
        scp.close()
        ssh.close()

# test
# test = AutoSCP('hle5', 'lemyha00', '/Users/hale/.ssh/id_ed25519.ppk')
# test.get_logs_watch('user01', '2023-02-03')
# test.get_logs_watch('user02', '2023-02-03')
# test.get_logs_watch('user03', '2023-02-03')