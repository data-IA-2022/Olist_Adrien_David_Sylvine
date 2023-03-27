import time
from os.path import dirname, join

import yaml
from sshtunnel import SSHTunnelForwarder


def relative_path(*path):

    return join(dirname(__file__), *path)


def calc_time(start_time):

    d = time.time() - start_time
    h = int(d / 3600)
    h = f"{h} h " if d > 3600 else ''
    m = int(d % 3600 / 60)
    m = f"{m:02} m " if d >= 3600 else f"{m} m " if d > 60 else ''
    s = int(d % 3600 % 60)
    s = f"{s:02} s" if d >= 60 else f"{s} s"
    return h + m + s


def connect_ssh_tunnel(config_file):

    # Read configuration information from file
    config = yaml.safe_load(open(config_file, 'r'))

    if config["ssh_tunnel"] not in config:
        return None

    ssh_config = config[config["ssh_tunnel"]]

    server = SSHTunnelForwarder(
        ssh_config['host'],
        ssh_username = ssh_config['user'],
        ssh_password = ssh_config['password'],
        remote_bind_address = (ssh_config['remote_adr'], ssh_config['remote_port']),
        local_bind_address = (ssh_config['local_adr'], ssh_config['local_port'])
    )

    server.start()

    return server


def connect_to_db(config_file):

    # Read configuration information from file
    config = yaml.safe_load(open(config_file, 'r'))

    if config["connection"] not in config:
        return None

    conn_config = config[config["connection"]]

    url_pswd = ":{password}".format(**conn_config) if conn_config["password"] != None else ""
    url_user = "{user}{url_pswd}@".format(**conn_config, url_pswd = url_pswd) if conn_config["user"] != None else ""
    url_port = ":{port}".format(**conn_config) if conn_config["type"] != "mongodb" else ""
    url_data = "/{db_name}".format(**conn_config) if conn_config["db_name"] != None else ""

    url = "{type}://{url_user}{host}{url_port}{url_data}".format(**conn_config, url_user=url_user, url_port=url_port, url_data=url_data)

    if conn_config["type"] == "mongodb":

        from pymongo import MongoClient
        from pymongoarrow.monkey import patch_all

        patch_all()

        return MongoClient(url, conn_config["port"])

    else:

        from sqlalchemy import create_engine

        return create_engine(url)