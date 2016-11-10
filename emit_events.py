#!/usr/bin/env python
import pika
import sys
import json
		
credentials = pika.PlainCredentials('pubsub','pubsub')
params = pika.ConnectionParameters(host='optimum.euprojects.net',port=8925,credentials=credentials)


connection = pika.BlockingConnection(params)

channel = connection.channel()

channel.exchange_declare(exchange='events',
                         type='fanout')

message = json.dumps({"event_type" : "traffic_issue","location" : [12, 45],"time": "time_of_occurrence"})
channel.basic_publish(exchange='events',
                      routing_key='',
                      body=message)
print(" [x] Sent %r" % message)
connection.close()
