import json
import smtplib
import ast
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import base64

# Inputs:
# required params:
#   to = str/list
#   subject = str
#   filename = str = "NA"
#   key = str
# post 
#   attachment = Base64 string
#   username = str
#   password = str
#   {
#        "attachment": "BASE64 STRING"
#        "username": "YOUR USERNAME"
#        "password": "YOUR PASSWORD"
#   }

def sendEmail(username, password, to, subject, attachment = None, filename = None, key = None):
    mail_content = """
        This is an automated email. Please do not reply.
        """
    message = MIMEMultipart()
    message['From'] = username
    message['To'] = to
    message['Subject'] = subject
    
    message.attach(MIMEText(mail_content, 'plain'))
    
    attach_file_name = filename
    attach_file = base64.standard_b64decode(attachment)
    print("base64 decode"+str(attach_file))
    
    print("attachfile type")
    print(type(attach_file))
    
    payload = MIMEBase('application', 'octate-stream')
    payload.set_payload(attach_file)
    print("attach_file.read()")
    print(attach_file)
    
    encoders.encode_base64(payload) #encode the attachment
    
    payload.add_header('Content-Disposition', 'attachment', filename=attach_file_name)
    print("payload::::")
    print(str(payload))
    message.attach(payload)
    
    text = message.as_string()
    print("message is:")
    print(text)
    
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(username, password)
        server.sendmail(username, to, text)
        server.close()
        return {
            'statusCode': 200,
            'body': json.dumps('Email sent.')
        }
    except:
            return {
            'statusCode': 502,
            'body': json.dumps('Error.')
        }

#-----WIP
# Delete API GATEWAY Methods
def lambda_handler(event, context):
    
    
    postbody = event['body']
    
    print(postbody)
    
    if postbody['filename'] is "NA":
        r = sendEmail(username = postbody['username'], 
        password = postbody['password'], 
        to = postbody['to'], 
        subject = postbody['subject'], 
        key = postbody['key'])
    else:
        
        r = sendEmail(username = postbody['username'], 
        password = postbody['password'], 
        to = postbody['to'], 
        subject = postbody['subject'], 
        attachment = postbody['attachment'], 
        filename = postbody['filename'], 
        key = postbody['key'])
    
    return {
        'statusCode': 200,
        'body': json.dumps(r)
    } 
    

