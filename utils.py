from os.path import join, dirname
import yaml
from sqlalchemy import create_engine

def relative_path(*path):

    return join(dirname(__file__), *path)

def get_config(cnx):
    config_file = relative_path("config_local_david.yaml")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    cfg=config[cnx]
    if cnx == 'PG':
        url = "{driver}://{user}:{password}@{host}:{port}/{database}".format(**cfg)
        return create_engine(url)
    
import time


def calc_time(start_time):

    d = time.time() - start_time
    h = int(d / 3600)
    h = f"{h} h " if d > 3600 else ''
    m = int(d % 3600 / 60)
    m = f"{m:02} m " if d >= 3600 else f"{m} m " if d > 60 else ''
    s = int(d % 3600 % 60)
    s = f"{s:02} s" if d >= 60 else f"{s} s"
    return h + m + s