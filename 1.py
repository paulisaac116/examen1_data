#!/usr/bin/env python
# coding: utf-8

# In[1]:


import couchdb
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json


### CREDENCIALES TWITTER ########################
ckey = "ya1iJhS7OyQRdCHwGT5B3aoUg"
csecret = "H0pgELBgPnxFj41HSRv6nnAbc5MQRaq1u6PEuRwUChi9saxVnv"
atoken = "247529219-Xx28XuCPLa66LzUCe2JEoWz6TCDD9vdvVnoglSj4"
asecret = "cu90qqOIsYC5XvkRBliqLOIacmCuYsJeuMGoJJPuXHmRY"
#####################################

class listener(StreamListener):
    
    def on_data(self, data):
        dictTweet = json.loads(data)
        try:
            
            dictTweet["_id"] = str(dictTweet['id'])
            doc = db.save(dictTweet)
            print ("SAVED" + str(doc) +"=>" + str(data))
        except:
            print ("Already exists")
            pass
        return True
    
    def on_error(self, status):
        print (status)
        
auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
twitterStream = Stream(auth, listener())

'''======== conexion couchdb'=========='''
server = couchdb.Server('http://admin:admin8675423*@localhost:5984/')  #('http://115.146.93.184:5984/')
try:
    db = server.create('onlinecourses')
except:
    db = server['onlinecourses']
    
'''=============== TOPICS =============='''    

twitterStream.filter(track=['Udemy','udemy','Coursera','coursera'])


# In[ ]:




