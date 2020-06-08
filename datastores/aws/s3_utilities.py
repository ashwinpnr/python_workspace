import boto3
import uuid


def get_s3_resource(access_key_id, secret_access_key):
    s3_resource = boto3.resource('s3', aws_access_key_id=access_key_id,
                                 aws_secret_access_key=secret_access_key)
    return s3_resource


def get_unique_s3_name(bucket_prefix):
    bucket_name = bucket_prefix + str(uuid.uuid4())
    return bucket_name


def create_bucket(s3_resource, bucket_prefix, region):
    bucket_name = get_unique_s3_name(bucket_prefix)
    response = s3_resource.create_bucket(Bucket=bucket_name,
                                         CreateBucketConfiguration={'LocationConstraint': region})
    return bucket_name, response


def upload_to_bucket(s3_resource, bucket_name, filename):
    s3_resource.Bucket(bucket_name).upload_file(Filename=filename, Key=filename)
