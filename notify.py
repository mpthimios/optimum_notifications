#!/usr/bin/env python
import pika, os
import logging
import subprocess
import sys
import json
import geocoder

credentials = pika.PlainCredentials('pubsub','pubsub')
params = pika.ConnectionParameters(host='optimum.euprojects.net',port=8925,credentials=credentials)

connection = pika.BlockingConnection(params)

channel = connection.channel()
channel.exchange_declare(exchange='events',type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='events',
                   queue=queue_name)
channel1 = connection.channel()
channel1.exchange_declare(exchange='push_notifications',type='fanout')

result1 = channel1.queue_declare(exclusive=True)
queue_name1 = result1.method.queue

channel1.queue_bind(exchange='push_notifications',
                   queue=queue_name1)

print(' [*] Waiting for logs. To exit press CTRL+C')

def on_message(ch, method, properties, body):
    print(" Received [x] %r" % body)
    new = json.loads(body)
    #Find address from latitude longitude
    g = geocoder.google(new['location'], method='reverse')
    #Find users that affected by the event
    #to be done
    #Send message to the users
    message = json.dumps({"type":"CHANGE","message":"Heavy traffic on your recently planned trip.","userId":"tz7yo2UqSAew3vi8SdtsBwGovH4iyt8x","tripId":"0b988bbd-8bf1-49fa-93dc-89a27266cc19","location_name":None,"location_coordinates":None})
    channel1.basic_publish(exchange='push_notifications',routing_key='',body=message,
                        properties=pika.BasicProperties(reply_to = queue_name1),mandatory=True)
    print("Sent %r" % message)
    #message = json.dumps({"type":"INFO","message":"Heavy traffic on your recently planned trip.","userId":"6EEGP034JBLydaotzqZrCs65jRdpfR4d","tripId":"null","location_coordinates":"null"})
    #channel1.basic_publish(exchange='push_notifications',routing_key='',body=message,
    #                    properties=pika.BasicProperties(reply_to = queue_name1),mandatory=True)
    print("Sent %r" % message)

channel.basic_consume(on_message,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()



