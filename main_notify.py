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
import dateutil.parser as DP
import pytz
from dateutil.relativedelta import relativedelta

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
    global send_event 
    event = json.loads(body)
    #print(" Received [x] %r" % body)
    #Find address from latitude longitude
    #g = geocoder.google(event['location'], method='reverse')

    #Find users that affected by the event  
    if event['veryCritical'] == True:
        print "very critical event"
        print(" Received [x] %r" % body)
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
        date = datetime.datetime.utcnow()
        date = pytz.utc.localize(date)
        date_from = date - relativedelta(days=-15)
        date_check_to = date + relativedelta(minutes=10)
        date_check_from = date + relativedelta(minutes=-5)
        
        print date_from
        print date_check_from
        print date_check_to
        #cursor = db.UserTrip.find({"createdat": { '$gte' : date_from}, "favourite": True, "body.segments.geometryGeoJson.geometry.coordinates": SON([('$near', 
        #      [event['latitude'],event['longitude']]), ('$maxDistance', 10), ('$uniqueDocs', True)])}, {"userId": 1, 'body': 1, 'favourite': 1, "createdat": 1, "_id": 1})
              
        cursor = db.UserTrip.find({"createdat": { '$gte' : date_from}, "body.segments.modeOfTransport.generalizedType": "CAR", "body.segments.geometryGeoJson.geometry.coordinates": SON([('$near', [event['latitude'],event['longitude']]), ('$maxDistance', 10), ('$uniqueDocs', True)])}, {"userId": 1, 'body': 1, 'favourite': 1, "createdat": 1, "_id": 1})
        
        print "found documents:"
        print cursor.count()
        affected_users = []

        for document in cursor:
            #print(document['_id'])
            request_date = DP.parse(document['body']['startTime'])
            if (date_check_from <= request_date) & (request_date <= date_check_to):
                print request_date
                if (document['userId'] not in affected_users):
                    print "affected_user"
                    print document['userId']
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



