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
channel.exchange_declare(exchange='events',type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='events',
                   queue=queue_name)
channel1 = connection.channel()
channel1.exchange_declare(exchange='notifications',type='fanout')

result1 = channel1.queue_declare(exclusive=True)
queue_name1 = result1.method.queue

channel1.queue_bind(exchange='notifications',
                   queue=queue_name1)

print(' [*] Waiting for logs. To exit press CTRL+C')

def on_message(ch, method, properties, body):
    print(" [x] %r" % body)
    new = json.loads(body)
    new['notification'] = "notification_text"
    new['confidence_level'] = "75%"
    new['user_id']= "userID"
    mes=json.dumps(new)
    #channel.basic_ack(delivery_tag=method.delivery_tag)
    channel1.basic_publish(exchange='notifications',routing_key='',body=mes, 
			properties=pika.BasicProperties(reply_to = queue_name1),mandatory=True)


channel.basic_consume(on_message,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()
