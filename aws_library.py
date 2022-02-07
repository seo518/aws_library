import os
import boto3
import botocore
import pandas as pd
from boto3.session import Session
import mimetypes

email_key = "C:/Users/Administrator/Documents/Access Key/4c4172306742283679696f657c566c23.txt"

def get_aws_Key(name, directory):
    print(name)
    d = os.listdir(directory)
    for line in d:
        print(line)
        if name in line:
            s3Key = pd.read_csv(directory + line)
            return s3Key
        
def initiate_session(access_id, secret_key):
    session = Session(aws_access_key_id = access_id,
                      aws_secret_access_key = secret_key)
    return session

def initiate_client(access_id, secret_key, resource_string):
    client = boto3.client(
        resource_string,
        aws_access_key_id=access_id,
        aws_secret_access_key=secret_key
    )    
    return(client)

def list_s3_files(bucket, session , filter_string = None):
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket) 
    if filter_string is None:
        objs = list(bucket.objects.all())
    else:
        objs = list(bucket.objects.filter(Prefix=filter_string))
    return(objs)

# inputs
# bucket = string
# session = object
# key = string
# file name = string/save as filename e,g 'C:/filename'
def download_s3_files(bucket, session, key, file_name):
    s3 = session.resource('s3')
    try:
        s3.Bucket(bucket).download_file(key, file_name)
        
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise    
        

# Need client initialization, resource_string = "s3"
def upload_files_dir(path, client, bucket_name):
    for subdir, dirs, files in os.walk(path):
        for file in files:
            print(file)
            full_path = os.path.join(subdir, file)
            file_mime = mimetypes.guess_type(file)[0] or 'binary/octet-stream'
            with open(full_path, 'rb') as data:
                client.put_object(Body=data, Bucket = bucket_name, Key=full_path[len(path):])


# Need client initialization, resource_string = "s3"
def upload_file(name, full_file_path, bucket_name, client):
    with open(full_file_path, 'rb') as data:
        client.put_object(Body=data, Bucket=bucket_name, Key=full_file_path[len(path):])        
    
# uses session to return bucket
def return_bucket(session, bucket_name):
    return(s3.Bucket(bucket_name))




def get_file_from_link(msg):
    import requests
    import re
    url= re.search("(?P<url>https?://[^\s]+)", str(msg).replace("\n","")).group('url').replace("=","").replace("3D","=")
    r= requests.get(url)
    d = r.headers['content-disposition']
    fname = re.findall("filename=(.+)", d)[0]
    return(r.content,fname)
    
    
def s3_load_email(s):
    import os
    import email
    import mimetypes   
    
    if s.key:
        #print("key present'")
        body = s.get()['Body'].read()
        msg = email.message_from_bytes(body)        
        
    else:
        with open(s, 'rb') as fp:
            msg = email.message_from_binary_file(fp)

    return(msg)
    


#file_temp = local file/s3 object body
#file_temp = s3 .get() object from key streaming bytes, the body s.get()['Body'].read()
#tmpfile = file name
def search_email_body(s,search_key):
    import re
    msg = s3_load_email(s)
    search_return=[]
    for part in msg.walk():
        if part.get_content_type() == 'text/plain':
            reg = r"(?<=" + search_key.lower() +":"+r").*"
            string= part.get_payload().lower().replace(' ','')
            search_return = re.findall(reg, string)
            if search_return:
                search_return= re.sub('\\r','',search_return[0])
                return(search_return)
            else:
                return(None)
        
        

def s3emails_get_attachment(s,fileName, email_part='Subject', download_ext='.csv'): #= "C:/Users/Administrator/Documents/s3Output/"):
    import re
    import email
    #import mimetypes
       
    msg = s3_load_email(s)
    date = format_date(s) 
    #print(msg)
    if fileName is None:
        filename = re.sub(r"[^A-Za-z0-9]+", '_', msg[email_part])
        #date=msg['date'][5:-15].replace(",",'').replace(" " ,"")
    else:
        filename = fileName
     
    if 'download' in str(msg).lower():
        x = get_file_from_link(msg)
        ext=download_ext
        filename=date+'_'+filename+ext
    else:
        for part in msg.walk():            
            if part.get_filename() is not None:
                ext=part.get_filename().split(".")[-1]
                print(ext)
                ext="."+ext         
                filename=date+'_'+filename+ext
            x = part.get_payload(decode=True)
            #print(x)
    #print(ext)
    if filename is None:
        print(0)  
    return(filename,x)



#TO DO, have this read the msg from s3_load_email() WIP
#msg = msg return from s3_load_email
def scan_email(s, access_key = email_key):
    msg = s3_load_email(s)
    with open(access_key, 'r') as key:
        key = key.read()
    return(key in str(msg))

#def rename_files(rename):
def format_date(file):
    import datetime
    date = file.last_modified.replace(tzinfo=None)
    date = str(datetime.datetime(date.year,date.month,date.day))
    date = date.replace("00:00:00","").replace("-","").replace(" ","")
    return(date)

def get_from_emailID(s, email_part, email_key):
    if scan_email(s, access_key=email_key)==True:
        msg = s3_load_email(s)
        part = msg[email_part]
        return(part)
    else:
        return(print("invalid key"))
