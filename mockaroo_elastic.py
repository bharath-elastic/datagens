from configparser import ConfigParser
import json, requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from argparse import ArgumentParser

def create_actions(cfg, data):
    for _id, rec in enumerate(data):
        action =  { "_index": cfg['elastic']['index'], "_type": cfg['elastic']['type'], "_id": _id }
        action["_source"] = rec
        yield action


def get_config(config_file):
    cfg = ConfigParser()
    try:
        if cfg.read(config_file) == []:
            raise FileNotFoundError(config_file)
        elastic = cfg['elastic']
        mockaroo = cfg['mockaroo']
    except FileNotFoundError as fnfe:
        print("configuration file not found", fnfe)
    except KeyError as e:
        print("missing section", e, "in configuration file")
    else:
        return cfg

def get_data(mockaroo_url):
    resp = requests.get(mockaroo_url, verify=False)
    data = json.loads(resp.text)
    return data

parser = ArgumentParser()
parser.add_argument("config_file")
args = parser.parse_args()
cfg = get_config(args.config_file)
#print(cfg['mockaroo']['url'])
if cfg != None:
    data = get_data(cfg['mockaroo']['url'])
    actions = create_actions(cfg, data)
    es = Elasticsearch()
    sb = streaming_bulk(es, actions)
    for i, item in enumerate(sb):
       print(i, end='\r')
