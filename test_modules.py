import configparser
import sys
import uuid
import os

from datastores import utilities
from datastores.aws import s3_utilities


def create_temp_file(size, file_name, file_content="test"):
    if not os.path.exists('temp'):
        os.makedirs('temp')
    random_file_name = "temp/" + file_name + str(uuid.uuid4().hex[:6])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name


def test_mongodb_connection(config):
    host = config.get('mongo', 'host')
    port = config.getint('mongo', 'port')
    user = config.get('mongo', 'user')
    password = config.get('mongo', 'password')
    auth = config.getboolean('mongo', 'auth')

    mongo_connect = utilities.get_mongodb_connection(host, port, user, password, auth)
    # List MongoDB Databases
    dbs = mongo_connect.list_database_names()
    print("DBs : " + str(dbs))

    # Connect db "test" and list collections
    db_name = "test"
    db_obj = mongo_connect[db_name]
    collections = db_obj.list_collection_names()
    print("Collections in DB : " + db_name + " are : " + str(collections))

    # Insert into collection "sample_collection"
    collection_name = "sample_collection"
    collection_obj = db_obj[collection_name]
    post = {"name": "Test Data", "description": "Test Description"}
    collection_obj.insert_one(post)

    # Find entries in collection "sample_collection"
    cursor = collection_obj.find()
    for record in cursor:
        print(str(record))


def test_s3_module(config):
    access_key_id = config.get('aws', 'aws_access_key_id')
    secret_access_key = config.get('aws', 'aws_secret_access_key')
    region_name = config.get('aws', 'aws_region')
    s3_resource = s3_utilities.get_s3_resource(access_key_id, secret_access_key)

    # create bucket
    bucket_prefix = "test"
    bucket_name,response = s3_utilities.create_bucket(s3_resource,bucket_prefix,region_name)
    print(bucket_name,response)

    # Create temp file and upload to bucket
    temp_file_name = create_temp_file(300, 'temp_file')
    s3_utilities.upload_to_bucket(s3_resource,bucket_name,temp_file_name)



def main():
    config_file = "config.ini"
    if len(sys.argv) >= 2:
        config_file = sys.argv[1]

    config = configparser.ConfigParser()
    config.read(config_file)
    #test_mongodb_connection(config)
    test_s3_module(config)


if __name__ == '__main__':
    main()
