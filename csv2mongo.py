from csv import DictReader
import json
from pymongo import MongoClient
import sys, os
import argparse

def get_data_dict(file):
    return DictReader(file)

def data_dict_to_json(dd):
    return json.dumps([line for line in dd])

def get_collection(client, db, collection):
    db = client[db]
    db.drop_collection(collection)
    return db[collection]

def load_collection(client, db, collection, f):
    as_json = data_dict_to_json(get_data_dict(f))
    f.close()
        
    collection = get_collection(client, db, collection)
    collection.insert(json.loads(as_json))
    print "%s records created" % collection.count()
    
def load_client():
    try:
        return MongoClient('mongodb://localhost:27017/')
    except Exception, e:
        print "Could not find MongoDB process. Is it running?"
        sys.exit(0)
        
def get_dbs(client):
    return client.database_names()

def get_collections(client, db):
    db = client[db]
    return db.collection_names(False)

def list_collections(client, db, provided_db = False):
    for name in get_collections(client, db):
        print "%s%s" %("\t" if provided_db else " |-", name)
        
def is_valid_file(parser, arg):
    if not os.path.exists(arg):
       parser.error("The file %s does not exist!"%arg)
    else:
       return open(arg,'r')

parser = argparse.ArgumentParser(description="Load csv data into mongodb")
action = parser.add_mutually_exclusive_group(required=True)
action.add_argument("-s", "--show", action="store_true", help="List DB and Collection names")
action.add_argument("-l","--load", action="store_true", help="Load csv data into Collection")
parser.add_argument("-d","--database", help="DB to list/load")
parser.add_argument("-c","--collection", help="Collection name to load")
parser.add_argument("-f","--filename", dest="filename", help="CSV to load", metavar="FILE", type=lambda x: is_valid_file(parser,x))
args = parser.parse_args()

client = load_client()
if args.show:
    if args.database:
        print "Collections in '%s':" % args.database
        list_collections(client, args.database, provided_db = True)
    else:
        for db in get_dbs(client):        
            print db
            list_collections(client, db)
if args.load:
    if not args.database or not args.collection or not args.filename:
        parser.error("Load requires a database, collection, and filename to load")
    load_collection(client, args.database, args.collection, args.filename)