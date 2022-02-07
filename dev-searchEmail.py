# Search email for specific key words
# @params: 
#    s = S3 bucket object
#    search_key = key to be searched
# Output : search_return = return strin/line following search key, if present.
def search_email_body(s, access_key, search_key):
    import re
    msg = s3_load_email(s, access_key)
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