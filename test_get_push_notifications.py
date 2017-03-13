#!/usr/bin/env python
import pika, os
import logging
import subprocess
import sys
import json

credentials = pika.PlainCredentials('pubsub','pubsub')
params = pika.ConnectionParameters(host='optimum.euprojects.net',port=8925,credentials=credentials)

connection = pika.BlockingConnection(params)

channel = connection.channel()
channel.exchange_declare(exchange='notifications',type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='push_notifications',
                   queue=queue_name)

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    print(" [x] %r" % body)
    #new = json.loads(body)
    #new['notification'] = "notification_text"
    #mes=json.dumps(new)
    #channel.basic_publish(exchange='gps_data',routing_key='',body=mes)


channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()

