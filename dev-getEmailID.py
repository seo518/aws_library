# get email Id from email object
# @params:

def get_from_emailID(s, email_part, access_key):
    msg = s3_load_email(s, access_key)
    part = msg[email_part]
    return(part)