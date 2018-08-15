#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request, session

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

@app.route('/')
def default():
    default_event = {
	    'event_type': 'default',
	    'remote_ip' : request.remote_addr,
	    'logged_in_user' : '',
	    'purchased_sword' : '',
	    'joined_guild':  '',
	    'more_purchase_info' : {}
    }  
    log_to_kafka('curlProjectEvents', default_event) 
    return "This is the default response !!"

@app.route('/login', methods=['POST'])
def login():
	data = request.get_json()
	if request.method == 'POST':
	    login_event = {
		    'event_type': 'user_login',
		    'remote_ip' : request.remote_addr,
		    'logged_in_user' : data['username'],
		    'purchased_sword' : '',
		    'joined_guild':  '',
		    'more_purchase_info' : {}
	    }  
	    log_to_kafka('curlProjectEvents', login_event) 
	    return "Hello !! " +  data['username'] + " You are successfully logged in"

@app.route('/logout', methods=['POST'])
def logout():
	data = request.get_json()
	if request.method == 'POST':
	    logout_event = {
		    'event_type': 'user_logout',
		    'remote_ip' : request.remote_addr,
		    'logged_in_user' : data['username'],
		    'purchased_sword' : '',
		    'joined_guild':  '',
		    'more_purchase_info' : {}
	    }  
	    log_to_kafka('curlProjectEvents', logout_event) 
	    return "Thank You !! " + data['username'] + " You are logged out successfully!"   


@app.route("/purchase_a_sword", methods=['POST'])
def purchase_a_sword():
	data = request.get_json()
	if request.method == 'POST':
	    purchase_sword_event={
		    'event_type': 'purchase_sword',
		    'remote_ip' : request.remote_addr,
		    'logged_in_user' : data['username'],
		    'purchased_sword' : data['itemtype'],
		    'joined_guild':  '',
		    'more_purchase_info' : data
	    }
	    log_to_kafka('curlProjectEvents', purchase_sword_event)
        return "Hello !! " + data['username'] + " You purchased a Sword successfully!"   

@app.route("/join_a_guild", methods=['POST'])
def join_a_guild():
	data = request.get_json()
	if request.method == 'POST':
	    join_guild_event = {
		    'event_type': 'join_a_guild',
		    'remote_ip' : request.remote_addr,
		    'logged_in_user' : data['username'],
		    'purchased_sword' : '',
		    'joined_guild':  data['itemtype'],
		    'more_purchase_info' : data
	    }
	    log_to_kafka('curlProjectEvents', join_guild_event)
	    return "Hey " + data['username'] + "!! You joined the guild " + data['guildname'] + " successfully!\n"

@app.route("/purchase_a_knife")
def purchase_a_knife():
    purchase_knife_event = {
	    'event_type': 'purchase_knife',
	    'remote_ip' : request.remote_addr,
	    'description': 'very sharp knife'
    }
    log_to_kafka('curlProjectEvents', purchase_knife_event)
    return "Knife Purchased!\n"
