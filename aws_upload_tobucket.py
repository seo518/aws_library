import boto3
import botocore
from boto3.session import Session
import mimetypes
from tkinter import filedialog
from tkinter import *
from botocore.exceptions import NoCredentialsError
import os


def choose_dir():
    root = Tk()
    dirname = filedialog.askdirectory(parent=root,initialdir="/",title='Please select a directory')
    root.destroy()
    return(dirname)

def initiate_session(access_id, secret_key):
    session = Session(aws_access_key_id = access_id,
                      aws_secret_access_key = secret_key)
    return (session)

     
def upload_to_aws(path, s3, bucket_name):
    for subdir, dirs, files in os.walk(path):
        for file in files:
            print(file)
            full_path = os.path.join(subdir, file).replace("\\", "/")
            try:
                s3.upload_file(full_path, bucket_name, Key=os.path.basename(full_path))
                print("Upload Successful")
            except FileNotFoundError:
                print("The file was not found")
            except NoCredentialsError:
                print("Credentials not available")
        
                
Accesskey = 'AKIA5AY573NGDLRFQLXY'
Secretkey = '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'
bucket = 'fordsalesdata'
s3 = boto3.client('s3', aws_access_key_id=Accesskey, aws_secret_access_key=Secretkey)
upload_to_aws(path = choose_dir(), s3 = s3, bucket_name = bucket )



