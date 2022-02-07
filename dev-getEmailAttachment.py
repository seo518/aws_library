# Get date from email and format it
# @param: file = S3 bucket object
def format_date(file):
    import datetime
    date = file.last_modified.replace(tzinfo=None)
    date = str(datetime.datetime(date.year,date.month,date.day))
    date = date.replace("00:00:00","").replace("-","").replace(" ","")
    return(date)


# get attachment from email and save in destination folder
# @params:
# s = S3 bucket object
# fileName = if the file name of attachement needs to be changed, specify fileName, else default = None
# Output:
#    filename = name of the attchemnt file
#    

def s3emails_get_attachment(s, access_key): 
       
    msg = s3_load_email(s, access_key)
    date = format_date(s) 
    
    if 'download' in str(msg).lower():
        attachement,fname = get_file_from_link(msg)
        filename = fname
        filename=date+'_'+filename+"."+fname.split(".")[-1]
    else:
        for part in msg.walk():            
            if part.get_filename() is not None:
                name=part.get_filename().replace("\r",'').replace("\n",'').replace(' ','')
                attachement = part.get_payload(decode=True)
                filename=date+'_'+name
    if filename is None:
        print(0)  
    return(filename,attachement)

def get_file_from_link(msg):
    import re
    url= re.search("(?P<url>https?://[^\s]+)", str(msg).replace("\n","")).group('url').replace("=","").replace("3D","=")
    #r= requests.get(url)
    http = urllib3.PoolManager()
    r = http.request("GET", url)
    d = r.headers['content-disposition']
    fname = re.findall("filename=(.+)", d)[0]
    return(r.data,fname)

