import boto3
import csv
import os
os.environ['AWS_PROFILE'] = "My-profile"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1"
session = boto3.session.Session(profile_name='my-profile')
client = session.client('s3', 'us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')


# Read bucket names and tags from CSV file
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
        except:
            # Bucket does not have tags yet
            existing_tags = []

        # Merge existing and new tags
        merged_tags = list(existing_tags)
        for tag in new_tags:
            if tag not in merged_tags:
                merged_tags.append(tag)

        # Update bucket tags
        s3.put_bucket_tagging(Bucket=bucket_name, Tagging={'TagSet': merged_tags})
