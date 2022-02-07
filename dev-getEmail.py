
# get email body
# @param:
#    s = S3 bucket object
#    access_key = email access key
# Output:
#    msg = email body in bytes

def s3_load_email(s , access_key):
    import email     
    
    if s.key:
        body = s.get()['Body'].read()
        msg = email.message_from_bytes(body)        
        
    else:
        with open(s, 'rb') as fp:
            msg = email.message_from_binary_file(fp)
    
    if access_key in str(msg):
        return(msg)
    else:
        print("No key available")
        return(None)