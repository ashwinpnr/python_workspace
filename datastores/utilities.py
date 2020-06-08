from elasticsearch import Elasticsearch
from pymongo import MongoClient, errors
import ssl
import urllib3
import sys


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings()


def get_es_connection(es_host, es_port, es_user, es_password, es_ssl=True, es_timeout=60):
    """

    :param es_host:
    :param es_port:
    :param es_user:
    :param es_password:
    :param es_ssl:
    :param es_timeout:
    :return: ES Connection
    """
    if es_ssl:
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


def get_mongodb_connection(host, port, user, password, auth = False):
    """

    :param host: Host name/ip of mongodb
    :param port: Port of MongoDB
    :param user: Username
    :param password: Password
    :param auth: Auth enabled or not
    :return: MongoDB Connection
    """
    try:
        if auth:
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


