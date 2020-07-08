import boto3
import configparser

config = configparser.ConfigParser()
config.read("../dl.cfg")

aws_s3_uri = config.get("AWS", "AWS_S3_URI")
aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")
aws_s3_bucket = config.get("AWS", "AWS_S3_BUCKET")



s3 = boto3.resource("s3",
                  endpoint_url=aws_s3_uri,
                  aws_access_key_id=aws_access_key,
                  aws_secret_access_key=aws_secret_key)

# Check if bucket already exists
s3_bucket_exists = False
#for bucket in s3.list_buckets():
#response = s3.list_buckets()
#for bucket in response['Buckets']:
for bucket in s3.buckets.all():
#   bucket = bucket["Name"]
    bucket = bucket.name
    if bucket == s3_bucket:
        s3_bucket_exists = True

#s3 = boto3.client('s3', region_name=aws_region)

# Create bucket if it does not exist
if not s3_bucket_exists:
    s3.create_bucket(
        Bucket=s3_bucket
        )

# upload etl.py to s3_bucket
#
#s3.upload_file(s3_install_requirements_file, s3_bucket, s3_install_key)

s3.Bucket(s3_bucket).upload_file("./nyc-crime", "nyc-crime.csv")
