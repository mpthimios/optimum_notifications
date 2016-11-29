#!/usr/bin/env python
import pika, os
import logging
import subprocess
import sys
import json
import geocoder
import pymongo
from bson.son import SON
import db_credentials

credentials = pika.PlainCredentials('pubsub','pubsub')
params = pika.ConnectionParameters(host='optimum.euprojects.net',port=8925,credentials=credentials)

#cep_issues
#events
cep_issues_channel = 'events'
push_notifications_channel = 'push_notifications'

connection = pika.BlockingConnection(params)

channel = connection.channel()
channel.exchange_declare(exchange='events',type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue


channel.queue_bind(exchange=cep_issues_channel,
                   queue=queue_name)
channel1 = connection.channel()
channel1.exchange_declare(exchange=push_notifications_channel,type='fanout')

result1 = channel1.queue_declare(exclusive=True)
queue_name1 = result1.method.queue

channel1.queue_bind(exchange=push_notifications_channel,
                   queue=queue_name1)

print(' [*] Waiting for logs. To exit press CTRL+C')

def on_message(ch, method, properties, body):
    print(" Received [x] %r" % body)
    event = json.loads(body)
    
    #Find address from latitude longitude
    #g = geocoder.google(event['location'], method='reverse')

    #Find users that affected by the event  

    #MongDB connection
    client = pymongo.MongoClient(db_credentials.mongo_server, db_credentials.mongo_port)
    client.Optimum.authenticate(db_credentials.user, db_credentials.password)    
    db = client['Optimum']    
    #db.UserTrip.create_index([("segments.geometryGeoJson.geometry.coordinates", pymongo.GEO2D)])
    #cursor = db.UserTrip.find({
    #    "_id": "c484f244-e758-4e1f-a0c2-442b6b9d38f0"
    #})    
    cursor = db.UserTrip.find({
       "segments.geometryGeoJson.geometry.coordinates": SON([('$near', [event['location'][0],event['location'][1]]), ('$maxDistance', 0.1/111.12), ('$uniqueDocs', 1)])
    }, {"userId": 1, 'body': 1})
    
    affected_users = []

    for document in cursor:
        print(document['_id'])
        #print(document['userId'])
        #print(document['body'])
        j = json.loads(document['body'])
        #print j['segments']        
        db.UserTrip.update({'_id':document['_id']}, {"$set": {'segments':j['segments']}}, upsert=False)
        if (document['userId'] not in affected_users):
          affected_users.append(document['userId'])

    #Send message to the users
    #CHANGE
    for userId in affected_users:
      print "affected user"
      print userId
      message = json.dumps({"type":"INFO","message":"Heavy traffic on your recently planned trip.","userId":userId,"tripId":"0b988bbd-8bf1-49fa-93dc-89a27266cc19","location_name":None,"location_coordinates":None})
    #channel1.basic_publish(exchange='push_notifications',routing_key='',body=message,
    #                    properties=pika.BasicProperties(reply_to = queue_name1),mandatory=True)
    print("Sent %r" % message)
    #message = json.dumps({"type":"INFO","message":"Heavy traffic on your recently planned trip.","userId":"6EEGP034JBLydaotzqZrCs65jRdpfR4d","tripId":"null","location_coordinates":"null"})
    #channel1.basic_publish(exchange='push_notifications',routing_key='',body=message,
    #                    properties=pika.BasicProperties(reply_to = queue_name1),mandatory=True)    

channel.basic_consume(on_message,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()



