# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 10:03:50 2020

@author: Shweta.Anjan
"""
import os
import pandas as pd
import boto3
import botocore
from boto3.session import Session
import io
import gzip
import codecs
import datatables as dt


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


def list_s3_files(bucket, session , filter_string = None):
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket) 
    if filter_string is None:
        objs = list(bucket.objects.all())
    else:
        objs = list(bucket.objects.filter(Prefix=filter_string))
    return(objs)    


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


def read_prefix_to_df(your_bucket, bucket, session, cols, file_name, j, length,sep):
    prefix_df = pd.DataFrame()
    s3 = Session.resource('s3')
    #prefix_df.columns = cols
    for obj in your_bucket:
        body = obj.get()['Body'].read()
        
        if 'gz' in obj.key:
            gzipfile = io.BytesIO(body)
            gzipfile = gzip.GzipFile(fileobj=gzipfile)
            content = gzipfile.read()
        else:
            content = body
        df = pd.read_csv(io.StringIO(str(content,'utf-8')), sep=sep, names = cols, engine='python')
        prefix_df = prefix_df.append(df)
    
    csv_buffer = io.StringIO()
    prefix_df.to_csv(csv_buffer, index=False)
    s3.Object(bucket, file_name).put(Body=csv_buffer.getvalue())
    j=j+100
    length = length - 100
    return(j,length)

     

from datetime import datetime
from datetime import timedelta 

datetime_object = datetime.strptime('2020-01-04', "%Y-%m-%d").date()

file_name = 'masterkey-apnreferencefields'
creds_directory = os.path.expanduser("~/OneDrive - insidemedia.net/AWS/aws_library/creds/").replace("\\", "/")
s3Key = get_aws_Key(file_name+'.csv', creds_directory)
Session = initiate_session(access_id = s3Key.iloc[0]["Access key ID"], secret_key = s3Key.iloc[0]["Secret access key"])
bucket ='ttd-bclcloglevel'
datetime_object.strftime("%Y-%m-%d")
cols_clicks = ['LogEntryTime', 'ClickId', 'IPAddress', 'ReferrerURL', 'Redirect URL', 'CampaignId', 'ChannelId', 'AdvertiserId', 'DisplayImpressionId', 'Keyword', 'KeywordId', 'MatchType', 
        'DistributionNetwork', 'TDID', 'RawUrl', 'ProcessedTime', 'DeviceID']
cols_imps = ['LogEntryTime', 'ImpressionId','PartnerId','AdvertiserId','CampaignId','AdGroupId','PrivateContractID','AudienceID','CreativeId','AdFormat','Frequency','SupplyVendor',
             'SupplyVendorPublisherID','DealID','Site','ReferrerCategoriesList','FoldPosition','UserHourOfWeek','UserAgent','IPAddress','TDID','Country','Region','Metro','City',
             'DeviceType','OSFamily','OS','Browser','Recency','LanguageCode','MediaCost','FeeFeatureCost','DataUsageTotalCost','TTDCostInUSD','PartnerCostInUSD','AdvertiserCostInUSD',
             'Latitude','Longitude','DeviceID','ZipCode','ProcessedTime','DeviceMake','DeviceModel','RenderingContext','CarrierID','TemperatrueInCelsiusName','TemperatureBucketStartInCelsiusName'
             'TemperatureBucketEndInCelsiusName','PartnerCurrency','PartnerCurrencyExchangeRateFromUSD','AdvertiserCurrency','AdvertiserCurrencyExchangeRateFromUSD',
             'ImpressionPlacementId','AdsTxtSellerTypeId','AuctionType']

cols_conv = ['LogEntryTime','ConversionId','AdvertiserId','Conversion Type','TDID','IP Address','UserAgent']

import re 


for i in range(98):
    date = datetime_object + timedelta(days = i)
    str_date = date.strftime("%Y-%m-%d")
    pattern = 'conversions_xy45m7n_V5_1_'+str_date
    print('FILE_pattern=',pattern)
    bucket_list = list_s3_files(bucket = bucket, session = Session, filter_string = pattern)
    print ('start time=', datetime.now())
    print ('bucket size=',len(bucket_list))
    v=0
    length = len(bucket_list)
    j=0
    while length >0 :
        print('j=',j)
        v=v+1
        if length > 100:
            seg_bucket_list = bucket_list[j:j+100]
        else:
            seg_bucket_list = bucket_list[:length]
    #day_list = [x for x in bucket_list if date.strftime("%Y-%m-%d") 
        print ('list done')
        file_name = pattern+date.strftime("%Y-%m-%d")+"V_"+str(v)+'.csv'
        print(file_name)
        j,length = read_prefix_to_df(seg_bucket_list, bucket, Session, cols_conv, file_name ,j ,length)
        print(length)
        


import csv

cols = ["Product_Category_Name","Product_Name","Sales_Date","FSA","CDC_Date","City","Draw_Nbr","transaction_count","Paid_Draw_Amt_Sales_","Draw_Date_Key","Estimated_Jackpot_Amt","Supplemental_Draw_Count_Maxmillions", "Nbr_GP_Million_Draws_Guaranteed_Prize","Nbr_GP_Additional_Draws_Promotional_Guaranteed_Prize"]
temp = "C:/Users/shweta.anjan/Desktop/temp_sales/"
client = boto3.client('s3', aws_access_key_id=s3Key.iloc[0]["Access key ID"], aws_secret_access_key=s3Key.iloc[0]["Secret access key"])
bucket ="bclcsalesdata"

cols = cols.replace(" ","_")


j,length = read_prefix_to_df(bucket_list, bucket, Session, cols, file_name ,j ,length)


for obj in bucket_list:
    print(obj)
    body = obj.get()['Body'].read()
    contents = io.BytesIO(body)
    contents = io.StringIO(str(contents,'utf-8'))
    with open(temp+obj.key.replace("\\",'').replace(' ','_'), "w",newline='') as fw:
        cr = csv.reader(contents)
        cw = csv.writer(fw)
        cw.writerow(cols)
        cw.writerows(cr)
    with open(temp+obj.key.replace("\\",'').replace(' ','_')) as data:
        client.put_object(Body=data, Bucket=bucket, Key=obj.key.replace("\\",'').replace(' ','_'))
    print ("next round")
        



for subdir, dirs, files in os.walk(temp):
    for file in files:
        print(file)
        full_path = os.path.join(subdir, file)
        name = file.replace("_",'').replace(' ','_')
        print(name)
        with codecs.open(full_path, encoding = 'utf-8-sig') as fr,codecs.open(os.path.join(subdir,"upload/"+name),"w", encoding = 'utf-8') as fw:
            cr = csv.reader(fr)
            cw = csv.writer(fw)
            cw.writerow(cols)
            cw.writerows(cr)
            
            
r = pd.read_csv(temp+"upload/MediaCom_Raw_Data_Output_Lottery_Sales_2019.csv", nrows =200)
r.to_csv(temp+"upload/MediaCom_Raw_Data_Output_Lottery_Sales_2016_schema.csv", index=False)

upload_to_aws(path = choose_dir(), s3 = client, bucket_name = bucket )

