#!/usr/bin/env python

import re
import os
import sys
import time
import bottle
import logging
import urllib2
import urlparse
import contextlib
from messages import Message, MessageDB, MessageEncoder, MessageQueue

from bottle import route, request,get, post, put, delete, response, template
import  pika


import json

STATIC_ROOT = os.path.join(os.path.dirname(__file__), 'static')

logging.basicConfig()
log = logging.getLogger('sender')
log.setLevel(logging.DEBUG)


log.debug("setting up message queue")

rabbit_url = os.environ['RABBITMQ_URL']
queue_name = os.environ['QUEUE_NAME']

print os.environ['RABBITMQ_URL']

#rdb = redis.Redis(host=url.hostname, port=url.port, password=url.password)

 

'''
view routes
'''
@post('/send') 
def send():
	number = request.json['number']
	if not number:
		return template('Please add a number to the end of url: /send/5')
	fib = F(int(number))
	#rabbit_url = os.environ['RABBITMQ_URL']
	parameters = pika.URLParameters(rabbit_url)
	connection = pika.BlockingConnection(parameters)
	channel = connection.channel() 
	channel.queue_declare(queue=queue_name)

	json_body = json.dumps({'sequence_id':number, 'sequence_value':fib})
	 
	channel.basic_publish(exchange='', routing_key='fibq', body=json_body)
	connection.close()
	return json_body

def F(n):
	if n == 0: return 0
	elif n == 1: return 1
	else: return F(n-1)+F(n-2)

@route('/')
def home():
    return bottle.template('home', sent=False, body=None)

@post('/fib') 
def fib():
	number = request.json['number']
	if not number:
		return template('Please add a number to the end of url: /fib/5')
	fib = F(int(number))
	json_body = json.dumps({'sequence_id':number, 'sequence_value':fib})
	return json_body


@route('/static/:filename')
def serve_static(filename):
    log.debug("serving static assets")
    return bottle.static_file(filename, root=STATIC_ROOT)

'''
service runner code
'''
log.debug("starting web server")
application = bottle.app()
application.catchall = False

"""
#UNCOMMENT BELOW FOR RUNNING ON LOCALHOST
if os.getenv('SELFHOST', False):

url = os.getenv('VCAP_APP_HOST')
port = int(os.getenv('VCAP_APP_PORT'))
bottle.run(application, host=url, port=port)

#UNCOMMENT BELOW FOR RUNNING ON HDP
"""

bottle.run(application, host='0.0.0.0', port=os.getenv('PORT', 8080))


# this is the last line
