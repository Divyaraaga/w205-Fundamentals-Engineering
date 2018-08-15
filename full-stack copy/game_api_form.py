#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request, session, redirect, url_for, escape

app = Flask(__name__)
# set the secret key.  keep this really secret:
app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

@app.route('/')
def index():
    if 'username' in session: 
        return 'Hi there, Logged in as %s' % escape(session['username'])
    return 'You just logged out or You are not logged in'

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        session['username'] = request.form['username']
        login_event = {
		    'event_type': 'user_login',
		    'remote_ip' : request.remote_addr,
		    'logged_in_user' : session['username'],
		    'purchased_sword' : '',
			'joined_guild':  '',
		    'more_purchase_info' : {}
        }  
        log_to_kafka('formProjectEvents', login_event) 
        return redirect(url_for('index'))
    return '''
        <form method="post">
            <p><input type=text name=username placeholder="Enter Username">
            <p><input type=submit value=Login>
        </form>
    '''

@app.route('/logout')
def logout():
    # remove the username from the session if it's there
    logout_event = {
	    'event_type': 'user_logout',
	    'remote_ip' : request.remote_addr,
	    'logged_in_user' : session['username'],
	    'purchased_sword' : '',
		'joined_guild':  '',
	    'more_purchase_info' : {}
    }  
    log_to_kafka('formProjectEvents', logout_event) 
    session.pop('username', None)
    return redirect(url_for('index'))

@app.route('/purchased')
def purchased():
    return "Hello !! " + session['username'] + " You purchased the Sword successfully!"    


@app.route("/purchase_a_sword", methods=['GET', 'POST'])
def purchase_a_sword():
	data = request.form
	print data
	if request.method == 'POST':
	    purchase_sword_event={
		    'event_type': 'purchase_sword',
		    'remote_ip' : request.remote_addr,
		    'logged_in_user' : session['username'],
		    'purchased_sword' : request.form['itemtype'],
		    'more_purchase_info' : {
		      "username" :session['username'],	
			  "itemtype":request.form['itemtype'], 
			  "comments":request.form['comments']
		    },
		    'joined_guild':  ''
	    }
	    log_to_kafka('formProjectEvents', purchase_sword_event)
	    return redirect(url_for('purchased'))  
	return '''
        <form method="post">
	        <select name=itemtype>
	          <option value="">Select Sword Type</option>
			  <option value="supersword">Super Sword</option>
			  <option value="kataya">Kataya</option>
			</select>
	        <p><input type=textarea name=comments placeholder="Enter Comments">
	        <p><input type=submit value=Purchase>
	    </form>
	'''

@app.route('/joinedguild')
def joinedguild():
    return "Hello !! " + session['username'] + " You joined the Guild successfully!"

@app.route("/join_a_guild", methods=['GET','POST'])
def join_a_guild():
	if request.method == 'POST':
	    join_guild_event = {
		    'event_type': 'join_a_guild',
		    'remote_ip' : request.remote_addr,
		    'logged_in_user' : session['username'],
		    'purchased_sword' : '',
		    'joined_guild': request.form['itemtype'],
		    'more_purchase_info' : {
		      "username" :session['username'],	
			  "itemtype":request.form['itemtype'], 
			  "comments":request.form['comments']
		    }
	    }
	    log_to_kafka('formProjectEvents', join_guild_event)
	    return redirect(url_for('joinedguild'))
	return '''
        <form method="post">
	       <select name=itemtype>
	          <option value="">Select Guild to join </option>
			  <option value="masadons">Masadons</option>
			  <option value="ligers">Ligers</option>
			</select>
	        <p><input type=textarea name=comments placeholder="Enter Comments">
	        <p><input type=submit value="Join the guild">
	    </form>
	'''
