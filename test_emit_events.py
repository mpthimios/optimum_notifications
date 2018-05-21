#!/usr/bin/env python
import pika
import sys
import json
		
credentials = pika.PlainCredentials('pubsub','pubsub')
params = pika.ConnectionParameters(host='optimum.euprojects.net',port=8925,credentials=credentials)

#cep_issues
#events
cep_issues_channel = 'cep_issues'

connection = pika.BlockingConnection(params)

channel = connection.channel()

message = json.dumps({"confidence":{"general":0.36,"bad_weather":0.092,"decreasing_speed":0.23,"social_media":"NaN"},"project":"sensor","bearing":180.0,"veryCritical":True,"highway":"M42","timestamp":1526881431104,"latitude":16.349067,"longitude":48.202386,"event_type":"traffic_issue","issue_type":"slowdown"})
channel.basic_publish(exchange=cep_issues_channel,
                      routing_key='',
                      body=message)
print(" [x] Sent %r" % message)
connection.close()
