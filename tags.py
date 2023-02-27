import boto3
import csv
import os
import logging
import botocore.exceptions
logging.basicConfig(filename='tags.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
os.environ['AWS_PROFILE'] = "My profile"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1"
session = boto3.session.Session(profile_name='My profile')
client = session.client('s3', 'us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

with open('tags.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        bucket_name = row['Bucket Name']
        new_tags = []
        for key, value in row.items():
            if key != 'Bucket' and value:
                new_tags.append({'Key': key, 'Value': value})

        # Get existing tags for the bucket
        try:
            existing_tags = s3.get_bucket_tagging(Bucket=bucket_name)['TagSet']
            logging.info(f"Existing tags for bucket {bucket_name}: {existing_tags}")
            
        except botocore.exceptions.ClientError as e:
             if e.response['Error']['Code'] == 'NoSuchTagSet':
        # Bucket does not have tags yet
                 existing_tags = []
             else:
        # Log the error message and move on to the next bucket
                logging.error(f"Error getting tags for bucket {bucket_name}: {str(e)}")
                continue
            
        # Merge existing and new tags
        merged_tags = list(existing_tags)
        for tag in new_tags:
            existing_tag = next((t for t in merged_tags if t['Key'] == tag['Key']), None)
            if existing_tag:
        # Replace existing tag with new value
                existing_tag['Value'] = tag['Value']
            else:
        # Add new tag to list
                merged_tags.append(tag)
        # Update bucket tags
        try:
            s3.put_bucket_tagging(Bucket=bucket_name, Tagging={'TagSet': merged_tags})
        except s3.exceptions.InvalidTag:
            # InvalidTag error occurred, log the bucket name and move on to the next bucket
            logging.error(f"InvalidTag error for bucket {bucket_name}")
            continue
