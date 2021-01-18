#!/usr/bin/env python
# coding: utf-8

# In[2]:


import json
from argparse import ArgumentParser
import requests
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson import json_util, ObjectId
import couchdb

######## CouchDB connection #########

URL = 'http://admin:admin8675423*@localhost:5984'
print(URL)

try:
    response = requests.get(URL)
    if response.status_code == 200:
        print('CouchDB connection: Success')
    if response.status_code == 401:
        print('CouchDB connection: failed', response.json())
except requests.ConnectionError as e:
    raise e

server=couchdb.Server(URL)
HEADERS = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

DBS = server['onlinecourses']


####### MongoDB connection ######

CLIENT = pymongo.MongoClient("mongodb://localhost:27017")

try:
    CLIENT.admin.command('ismaster')
    print('MongoDB connection: Success')
except ConnectionFailure as cf:
    print('MongoDB connection: failed', cf)

try:
    mydb = CLIENT["mongotocouch1"]
    mycol = mydb["onlinecourses"]
except:
    dblist = CLIENT.list_database_names()
    if "mongotocouch1" in dblist:
        print("The database exists.")
        
SKIPPED = []


###### Runninng ######

for db in DBS:
    
    try:
        documents=DBS.get(db, None)
        print(db)
        documents["_id"] = str(documents["id"])

        print(documents)
        doc = mycol.insert_one(documents)

    except TypeError as t:
        print('current document raised error: {}'.format(t))
        SKIPPED.append(documents)  # creating list of skipped documents for later analysis
        continue    # continue to next document
    except Exception as e:
        raise e


# In[ ]:




