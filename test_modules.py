import configparser
import sys

from datastores import utilities


def test_mongodb_connection(config):
    host = config.get('mongo', 'host')
    port = config.getint('mongo', 'port')
    user = config.get('mongo', 'user')
    password = config.get('mongo', 'password')
    auth = config.get('mongo', 'auth')

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


def main():
    config_file = "config.ini"
    if len(sys.argv) >= 2:
        config_file = sys.argv[1]

    config = configparser.ConfigParser()
    config.read(config_file)
    test_mongodb_connection(config)


if __name__ == '__main__':
    main()
