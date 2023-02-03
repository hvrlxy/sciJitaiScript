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

        scp.get(f"/opt/sci_jitai/{subject_id}@scijitai_com/logs-watch/{date}", 
                f"{self.data_path}{subject_id}@scijitai_com/logs-watch", 
                recursive=True)

        scp.close()

    def get_logs(self, subject_id, date):
        ssh = paramiko.SSHClient()

        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect('mhealth-sci.khoury.northeastern.edu', 
                    username=self.khoury_id, 
                    password=self.ppk_password, 
                    key_filename=self.ppk_path)

        scp = SCPClient(ssh.get_transport())

        scp.get(f"/opt/sci_jitai/{subject_id}@scijitai_com/logs/{date}", 
                f"{self.data_path}{subject_id}@scijitai_com/logs", 
                recursive=True)
        scp.close()
    def get_data(self, subject_id, date):
        ssh = paramiko.SSHClient()

        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect('mhealth-sci.khoury.northeastern.edu', 
                    username=self.khoury_id, 
                    password=self.ppk_password, 
                    key_filename=self.ppk_path)

        scp = SCPClient(ssh.get_transport())

        scp.get(f"/opt/sci_jitai/{subject_id}@scijitai_com/data/{date}", 
                f"{self.data_path}{subject_id}@scijitai_com/data", 
                recursive=True)
        scp.close()
    def get_data_watch(self, subject_id, date):
        ssh = paramiko.SSHClient()

        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect('mhealth-sci.khoury.northeastern.edu', 
                    username=self.khoury_id, 
                    password=self.ppk_password, 
                    key_filename=self.ppk_path)

        scp = SCPClient(ssh.get_transport())

        scp.get(f"/opt/sci_jitai/{subject_id}@scijitai_com/data-watch/{date}", 
                f"{self.data_path}{subject_id}@scijitai_com/data-watch", 
                recursive=True)
        scp.close()