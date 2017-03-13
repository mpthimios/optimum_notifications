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
import datetime

credentials = pika.PlainCredentials('pubsub','pubsub')
params = pika.ConnectionParameters(host='optimum.euprojects.net',port=8925,credentials=credentials)

#cep_issues
#events
cep_issues_channel = 'cep_issues'
push_notifications_channel = 'push_notifications'

connection = pika.BlockingConnection(params)

channel = connection.channel()
#channel.exchange_declare(exchange=cep_issues_channel,type='fanout')

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
    #db.UserTrip.create_index([("body.segments.geometryGeoJson.geometry.coordinates", pymongo.GEO2D)])
    
    #cursor = db.UserTrip.find({
    #    "_id": "9d133153-e1c2-4ddb-92ed-3fa3f8e45898"
    #})    
    #9d133153-e1c2-4ddb-92ed-3fa3f8e45898
    #c484f244-e758-4e1f-a0c2-442b6b9d38f0
    cursor = db.UserTrip.find({
       "body.segments.geometryGeoJson.geometry.coordinates": SON([('$near', 
          [event['location'][0],event['location'][1]]), ('$maxDistance', 0.1/111.12), ('$uniqueDocs', 1)]),
          "createdat": { '$gte' : datetime.datetime.utcnow()}}, {"userId": 1, 'body': 1})
    
    affected_users = []

    for document in cursor:
        print(document['_id'])
        #print(document['userId'])
        #print(document['body'])
        #j = json.loads(document['body'])
        #print j['segments']        
        #db.UserTrip.update({'_id':document['_id']}, {"$set": {'segments':j['segments']}}, upsert=False)
        if (document['userId'] not in affected_users):
          affected_users.append([document['userId'], document['_id']])

    #store the new event
    new_event = {
      "event": event,
      "affected_users": affected_users,
      "datetime": datetime.datetime.utcnow()
    }
    db.EventNotifications.insert_one(new_event)

    #Send message to the users
    #CHANGE
    for affected_user in affected_users:
      print "affected user"
      print affected_user[0]
      message = json.dumps({"type":"CHANGE","message":"Heavy traffic on your recently planned trip.","userId": affected_user[0],"tripId":affected_user[1],"location_name":'Vienna',"location_coordinates":None})
      channel1.basic_publish(exchange='push_notifications',routing_key='',body=message,
                         properties=pika.BasicProperties(reply_to = queue_name1),mandatory=True)
      print("Sent %r" % message)
    
channel.basic_consume(on_message,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()



