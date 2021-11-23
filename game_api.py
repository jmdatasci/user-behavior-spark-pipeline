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
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"

@app.route("/purchase_a_sword")
def purchase_a_sword():
    sword_type = request.args.get('sword_type')
    purchase_sword_event = {'event_type' : 'purchase_sword', 
                           'sword_type' : sword_type}
    log_to_kafka('events', purchase_sword_event)
    return " Sword Purchased!\n"

@app.route("/join_guild")
def join_guild():
    guild_name = request.args.get('guild_name')
    join_guild_event = {'event_type' : 'join_guild',
                       'guild_name' : guild_name}
    log_to_kafka('events', join_guild_event)
    return "Joined Guild!\n"
