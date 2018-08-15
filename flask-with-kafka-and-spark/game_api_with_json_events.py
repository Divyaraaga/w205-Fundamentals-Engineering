#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {
    'event_type': 'default',
    'remote_ip' : request.remote_addr
    }

    log_to_kafka('eventsForm', default_event)
    return "This is the default response!\n" + request.remote_addr


@app.route("/purchase_a_sword/<username>")
def purchase_a_sword(username):
    purchase_sword_event = {
    'event_type': 'purchase_sword',
    'remote ip' : request.remote_addr,
    'purchase_user' : username
    }
    log_to_kafka('eventsForm', purchase_sword_event)
    return "Hello " + format(username) + "!! You purchased a Sword successfully!\n"


@app.route("/join_a_guild/<username>/<guildname>")
def join_a_guild(username,guildname):
    join_guild_event = {
    'event_type': 'join_a_guild',
    'remote ip' : request.remote_addr,
    'user_name' : username,
    'joined_guild':  guildname
    }
    log_to_kafka('eventsForm', join_guild_event)
    return "Hey " + format(username) + "!! You joined the guild " + format(guildname) + " successfully!\n"
