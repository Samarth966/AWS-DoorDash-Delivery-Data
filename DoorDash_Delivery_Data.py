import boto3
import pandas as pd
from io import StringIO

def lambda_handler(event, context):
    print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    
    # Use boto3 to get the CSV file from S3
    try:
        s3_client = boto3.client('s3')
        sns_client = boto3.client('sns')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"].read().decode('utf-8')
        # Read the content using pandas
        data = pd.read_json(StringIO(file_content))
        new_d=data[data['status']=="delivered"]
        newjson=new_d.to_json(orient ='records')

        s3_client.put_object(Bucket='doordash-target-zn-processed', Key='2024-03-09-processed_input.json',Body=newjson)
        sns_client.publish(TopicArn='arn:aws:sns:us-east-1:992382369307:newtesttopic',Message='Successfully processed the daily file file name = '+key)

        print("test cicd feature")
    
    except:
        print("failed")
        sns_client.publish(TopicArn='arn:aws:sns:us-east-1:992382369307:newtesttopic',Message='Daily Processed Failed for below file = '+key)