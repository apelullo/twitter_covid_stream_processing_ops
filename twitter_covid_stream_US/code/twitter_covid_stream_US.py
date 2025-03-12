import requests
import json
import time
from datetime import datetime
from threading import Thread

import os
import glob
import boto3
from botocore.exceptions import NoCredentialsError

# Twitter access credentials
CONSUMER_KEY = "REDACTED"
CONSUMER_SECRET = "REDACTED"  
RECORDS_PER_FILE = 500000
# AWS access credentials
CDH_ACCESS_KEY_ID = "REDACTED"
CDH_SECRET_ACCESS_KEY = "REDACTED"
CDH_REGION = "us-east-2"
# S3 Bucket names
CDH_BUCKET_COVID = 'twitter-covid-100'
# EC2 File Paths
PROJECT_PATH = '/home/ubuntu/covid/cent_stream/'
INPUT_PATH = PROJECT_PATH + 'stream_data/'
TRANSFER_PATH = PROJECT_PATH + 'transfers/'
CARMEN_PATH = PROJECT_PATH + 'carmen-python/'
# Constants
FILE_TYPE = 'csv'
STORAGE_CLASS = 'DEEP_ARCHIVE'
RATE_SLEEP = 900
THREAD_SLEEP = 120
NUM_PARTITIONS = 4

# Globals
count = 0
file_object = None
file_name = None
lock = threading.Lock()

# Upload to AWS S3 bucket
def upload_to_aws(local_file, bucket, s3_file, args_dict, overwrite=False):
    # Establish S3 Connection
    s3_cdh = boto3.client('s3', aws_access_key_id=CDH_ACCESS_KEY_ID, aws_secret_access_key=CDH_SECRET_ACCESS_KEY, region_name=CDH_REGION)
    
    # Check upload status
    if 'Contents' in s3_cdh.list_objects(Bucket=bucket).keys() and not overwrite:
        uploaded = [item['Key'] for item in s3_cdh.list_objects(Bucket=bucket)['Contents']]
    else:
        uploaded = []
    
    # Upload files
    if s3_file not in uploaded:
        try:
            print('Now uploading: ', s3_file)
            s3_cdh.upload_file(Filename=local_file, Bucket=bucket, Key=s3_file, ExtraArgs=args_dict)
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False

# Get files to upload to AWS S3
def process_files():
    try:
        # transfer files
        for file in glob.glob(TRANSFER_PATH + '*.' + FILE_TYPE):
            input_file_name = file
            output_file_name = input_file_name.replace('.'+FILE_TYPE,'-loc.'+FILE_TYPE)
            print('Extracting locations...')
            carmen_cmd = 'python -m carmen.cli ' + input_file_name + ' ' + output_file_name
            os.system(carmen_cmd)
            print('Locations extracted!')

            aws_file_name = output_file_name.split(TRANSFER_PATH)[-1]
            bucket_name = CDH_BUCKET_COVID
            args_dict=dict()
            args_dict['StorageClass']=STORAGE_CLASS

            uploaded = upload_to_aws(output_file_name, bucket_name, aws_file_name, args_dict)
            if uploaded:
                os.remove(input_file_name)
                os.remove(output_file_name)
            else:
                print('File ', file, ' not uploaded!')
                
    except:
        print('File save incomplete! Please retry.')
        return False
    
    return True

# Bearer token request
def get_bearer_token(key, secret):
    #print('Getting access token')
    response = requests.post(
        "https://api.twitter.com/oauth2/token",
        auth=(key, secret),
        data={'grant_type': 'client_credentials'},
        headers={"User-Agent": "TwitterDevCovid19StreamQuickStartPython"})

    if response.status_code is not 200:
        raise Exception(f"Cannot get a Bearer token (HTTP %d): %s" % (response.status_code, response.text))

    print('Access token acquired')
    body = response.json()
    return body['access_token']


# Helper method that saves the tweets to a file at the specified path
def save_data(item):
    item = json.dumps(item)
    with lock:
        global file_object, count, file_name
        if file_object is None:
            file_name = int(datetime.utcnow().timestamp() * 1e3)
            count += 1
            file_object = open(f'{INPUT_PATH}covid19-{file_name}.csv', 'a')
            print('File created:')
            print(f'{INPUT_PATH}covid19-{file_name}.csv')
            file_object.write("{}\n".format(item))
            return
        if count == RECORDS_PER_FILE:
            # Save file
            file_object.close()
            os.rename(f'{INPUT_PATH}covid19-{file_name}.csv', f'{TRANSFER_PATH}covid19-{file_name}.csv')
            Thread(target=process_files, args=()).start()
            # Start new file
            count = 1
            file_name = int(datetime.utcnow().timestamp() * 1e3)
            file_object = open(f'{INPUT_PATH}covid19-{file_name}.csv', 'a')
            file_object.write("{}\n".format(item))
        else:
            #print(count,' tweets pulled in ', file_name)
            count += 1
            file_object.write("{}\n".format(item))


def stream_connect(partition):
    while True:
        try:
            print('Connecting to partition',partition)
            response = requests.get("https://api.twitter.com/labs/1/tweets/stream/covid19?partition={}".format(partition),
                                    headers={"User-Agent": "TwitterDevCovid19StreamQuickStartPython",
                                             "Authorization": "Bearer {}".format(get_bearer_token(CONSUMER_KEY, CONSUMER_SECRET))},
                                    stream=True)
            for response_line in response.iter_lines():
                if response_line:
                    save_data(json.loads(response_line))
        except:
            print('Connection to partition', partition, 'interrupted! Sleeping for', THREAD_SLEEP, 'seconds...')
            time.sleep(THREAD_SLEEP)


def main():
    # Create worker threads
    print('Pulling from 100% Covid stream')
    for partition in range(1, NUM_PARTITIONS+1):
        Thread(target=stream_connect, args=(partition,)).start()
    # Keep main thread alive
    while True:
        time.sleep(RATE_SLEEP)
        print('15 Minute Heatbeat: Rate Limits Reset - Going back to sleep...')

if __name__ == "__main__":
    main()
