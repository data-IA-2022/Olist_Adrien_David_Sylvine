from os.path import join, dirname
import yaml

def relative_path(*path):

    return join(dirname(__file__), *path)

def get_config(cnx):
    config_file = relative_path("config_local_david.yml")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    cfg=config[cnx]
    if cnx == 'PG':
        return "{driver}://{user}:{password}@{host}:{port}/{database}".format(**cfg)