import os
import json
import boto3
import requests
from datetime import datetime

s3 = boto3.client('s3')
bucket_name = 'job-market-source-bucket-rushi'  

def lambda_handler(event, context):
    # Read credentials from environment variables
    adzuna_app_id = os.environ.get('adzuna_app_id')
    adzuna_app_key = os.environ.get('adzuna_app_key')
    
    adzuna_url = f"https://api.adzuna.com/v1/api/jobs/in/search/1?app_id={adzuna_app_id}&app_key={adzuna_app_key}&results_per_page=10&what=developer"
    arbeitnow_url = "https://www.arbeitnow.com/api/job-board-api"
    
    try:
        # Fetch data from Adzuna
        adzuna_resp = requests.get(adzuna_url)
        adzuna_data = adzuna_resp.json()
        
        # Fetch data from Arbeitnow
        arbeitnow_resp = requests.get(arbeitnow_url)
        arbeitnow_data = arbeitnow_resp.json()
        
        # Combine data
        combined_data = {
            "adzuna": adzuna_data,
            "arbeitnow": arbeitnow_data
        }
        
        # Convert to JSON string
        json_str = json.dumps(combined_data)
        
        # Create S3 object key with timestamp
        timestamp = datetime.utcnow().strftime('%Y-%m-%d')
        s3_key = f"job_data/job_data_{timestamp}.json"
        
        # Upload to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_str,
            ContentType='application/json'
        )
        
        return {
            'statusCode': 200,
            'body': f'Successfully saved job data to s3://{bucket_name}/{s3_key}'
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
