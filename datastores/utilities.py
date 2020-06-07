from elasticsearch import Elasticsearch
from pymongo import MongoClient, errors
import ssl
import urllib3
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings()


def get_es_connection(es_host, es_port, es_user, es_password, es_ssl=True, es_timeout=60):
    if es_ssl:
        print("ESConnection with SSL")
        es = Elasticsearch(es_host,
                           port=int(es_port),
                           scheme="https",
                           ca_certs=False,
                           verify_certs=False,
                           http_auth=(es_user, es_password),
                           timeout=es_timeout)
    else:
        es = Elasticsearch(es_host,
                           port=int(es_port),
                           timeout=es_timeout)

    return es


def get_mongodb_connection(host, port, user, password, auth):
    try:
        if auth == "true":
            mongodb_connect_obj = MongoClient(host, port, ssl=True,
                                              ssl_cert_reqs=ssl.CERT_NONE)
            mongodb_connect_obj.the_database.authenticate(user, password,
                                                          mechanism='SCRAM-SHA-1',
                                                          source='admin')
        else:
            mongodb_connect_obj = MongoClient(host, port)

        return mongodb_connect_obj
    except errors.ConnectionFailure:
        print("Could not connect to MongoDB")
        sys.exit()
