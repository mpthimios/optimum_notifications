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

message = json.dumps({"event_type" : "traffic_issue","location" : [14.4874945, 46.0424301],"time": "time_of_occurrence"})
channel.basic_publish(exchange=cep_issues_channel,
                      routing_key='',
                      body=message)
print(" [x] Sent %r" % message)
connection.close()
