#!/usr/bin/env python
import pika
import sys
import json
		
credentials = pika.PlainCredentials('pubsub','pubsub')
params = pika.ConnectionParameters(host='optimum.euprojects.net',port=8925,credentials=credentials)

#cep_issues
#events
cep_issues_channel = 'events'

connection = pika.BlockingConnection(params)

channel = connection.channel()

#cep_issues
channel.exchange_declare(exchange=cep_issues_channel,
                         type='fanout')

message = json.dumps({"event_type" : "traffic_issue","location" : [16.3799375, 48.2052104],"time": "time_of_occurrence"})
channel.basic_publish(exchange=cep_issues_channel,
                      routing_key='',
                      body=message)
print(" [x] Sent %r" % message)
connection.close()
