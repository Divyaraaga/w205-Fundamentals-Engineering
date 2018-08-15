# Project 3 Setup

- You're a data scientist at a game development company.  
- Your latest mobile game has two events you're interested in tracking: 
- `buy a sword` & `join guild`...
- Each has metadata

## Project 3 Task
- Your task: instrument your API server to catch and analyze these two
event types.
- This task will be spread out over the last four assignments (9-12).

## Project 3 Task Options 

- All: Game shopping cart data used for homework 
- Advanced option 1: Generate and filter more types of items.
- Advanced option 2: Enhance the API to accept parameters for purchases (sword/item type) and filter
- Advanced option 3: Shopping cart data & track state (e.g., user's inventory) and filter

---

# Assignment 12

## Summary
  * In this exercise. we are spun up a cluster with kafka, zookeeper and the mids container,
  * Created a web based gaming application with apis written using python
  * We are focusing on 2 events
       1. Default route event
       2. Purchase a sword event
       3. Join a guild event    
  * Publishing the events to kafka when respective events are triggered by the user 
  * Reading the publised events through spark
  * Filter the events based on event type
  * Write the event type based data into Hadoop
  * Running queries with Presto
 
  
## Detailed Steps in the process.

 * I am spinning up my kafka, zookeeper and mids cluster
  
       docker-compose up -d

 * I am spinning up the spark cluster 
 
      docker-compose exec spark \
	  env \
	    PYSPARK_DRIVER_PYTHON=jupyter \
	    PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port 8888 --ip 0.0.0.0 --allow-root' \
	  pyspark      
     
 * I am checking for kafka broker logs
     
       docker-compose logs -f kafka

 * I am checking for Hadoop logs

       docker-compose logs -f cloudera      
      
 * I am checking for kafka broker logs, Here I am creating 2 Topics, 1 for Apache bench post commands and another for form based events
    
    - This is Apache bench POST requests

	       docker-compose exec kafka \
	       kafka-topics \
	         --create \
	         --topic curlProjectEvents \
	         --partitions 1 \
	         --replication-factor 1 \
	         --if-not-exists --zookeeper zookeeper:32181

	        OUTPUT
	        Created Topic curlProjectEvents.

    - This is for form based events

	       docker-compose exec kafka \
	       kafka-topics \
	         --create \
	         --topic formProjectEvents \
	         --partitions 1 \
	         --replication-factor 1 \
	         --if-not-exists --zookeeper zookeeper:32181

	        OUTPUT
	        Created Topic formProjectEvents.
        
 * I am publishing a sequence of 1 - 42 numbers as Messages to the topic curlProjectEvents 
    to test if kafka is up and ready to consume messages
     
       docker-compose exec kafka \
       bash -c "seq 42 | kafka-console-producer \
        --request-required-acks 1 \
        --broker-list localhost:29092 \
        --topic curlProjectEvents && echo 'Produced 42 messages.'"
       
       OUTPUT
       >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Produced 42 messages.
    
 * I am starting a consumer to read from the topic curlProjectEvents
    
       docker-compose exec kafka \
       kafka-console-consumer \
         --bootstrap-server localhost:29092 \
         --topic curlProjectEvents \
         --from-beginning \
         --max-messages 42


 * Once the above messages are published I purge the topics and recreate the same topics to keep the schema clean
  
    - This is Apache bench POST requests

	       docker-compose exec kafka \
		   kafka-topics \
			 --delete \
			 --topic curlProjectEvents \
			 --zookeeper zookeeper:32181

    - This is for form based events			 

		   docker-compose exec kafka \
		   kafka-topics \
		     --delete \
		     --topic formProjectEvents \
		     --zookeeper zookeeper:32181      

 * I am checking at tmp/ directory in hdfs and see that what we want to write isn't there already

       docker-compose exec cloudera hadoop fs -ls /tmp/

 * Create the function for "log_to_kafka" event in game_api_curl.py(apache bench post) and game_api_curl.py(Form based).
   - This function publishes the events e.g : "purchase a sword" , "join a guild" to kafka 
   - The python flask library is used to write the API server.
   
	       def log_to_kafka(topic, event):
		       event.update(request.headers)
		       producer.send(topic, json.dumps(event).encode())


 * Create the default route event in game_api_curl.py(apache bench post).
   - The python flask library is used to write the API server, this event captures the ip address of the user's machine 
   - Adding additional fields with empty values to get the entire schema to extract events based on varied filter criteria

    - This is Apache bench POST requests

			@app.route('/')
			def default():
			   # Building the json to capture the event type and 
	           # the ip address of the user's machine
			    default_event = {
				    'event_type': 'default',
				    'remote_ip' : request.remote_addr,
				    'logged_in_user' : '',
				    'purchased_sword' : '',
				    'joined_guild':  '',
				    'more_purchase_info' : ""
			    }  
			    log_to_kafka('curlProjectEvents', default_event) 
			    return "This is the default response!\n" + request.remote_addr


     - This is for form based events	

	        @app.route('/')
			def index():
			    if 'username' in session: 
			        return 'Hi there, Logged in as %s' % escape(session['username'])
			    return 'You just logged out or You are not logged in'	    


 * Create a Login route with method POST in game_api_curl.py(apache bench post) and game_api_curl.py(Form based).

    - This is Apache bench POST requests

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
					    'more_purchase_info' : ""
				    }  
				    log_to_kafka('curlProjectEvents', login_event) 
				    return "Hello !! " +  data['username'] + " You are successfully logged in"


    - This is for form based events
    - A form is displayed to capture the user name
    - The username is saved in the session

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
					    'more_purchase_info' : ""
			        }  
			        log_to_kafka('formProjectEvents', login_event) 
			        return redirect(url_for('index'))
			    return '''
			        <form method="post">
			            <p><input type=text name=username placeholder="Enter Username">
			            <p><input type=submit value=Login>
			        </form>
			    '''

* Create a Logout route with method POST in game_api_curl.py(apache bench post) and game_api_curl.py(Form based).

    - This is Apache bench POST requests

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
					    'more_purchase_info' : ""
				    }  
				    log_to_kafka('curlProjectEvents', logout_event) 
				    return "Thank You !! " + data['username'] + " You are logged out successfully!" 


    - This is for form based events
    - The username is removed from the session

			@app.route('/logout')
			def logout():
			    # remove the username from the session if it's there
			    logout_event = {
				    'event_type': 'user_logout',
				    'remote_ip' : request.remote_addr,
				    'logged_in_user' : session['username'],
				    'purchased_sword' : '',
				    'joined_guild':  '',
				    'more_purchase_info' : ""
			    }  
			    log_to_kafka('formProjectEvents', logout_event) 
			    session.pop('username', None)
			    return redirect(url_for('index'))

 * Create the route with method POST for "purchase a sword" event in game_api_curl.py(apache bench post) and game_api_curl.py(Form based).
   The purchase event captures the ip address of the user's machine, user name, Sword type, comments and more info about the purchases 
   and logs it to kafka

    - This is Apache bench POST requests

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


    - This is for form based events
    - A form is displayed to capture the user name,Sword type and comments
    - Also a default route to navigate after successful purchase

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

 * Create the another route for "Join a guild" event in the game_api_curl.py,
   The join a guild event captures the ip address of the user's machine, user name, guild type, comments and more info about the event 
   and logs it to kafka

    - This is Apache bench POST requests

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
				    return "Hey " + data['username'] + "!! You joined the guild " + data['itemtype'] + " successfully!\n"

    - This is for form based events
    - A form is displayed to capture the user name,Guild type and comments
    - Also a default route to navigate after successful purchase

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
        

 * I am running the game_api_curl.py and game_api_form.py file as below
     
       docker-compose exec mids \
       env FLASK_APP=/w205/full-stack/game_api_curl.py \
       flask run --host 0.0.0.0

       docker-compose exec mids \
	   env FLASK_APP=/w205/full-stack/game_api_form.py \
	   flask run --host 0.0.0.0

 * Testing the api server for the events using Apache bench, 
   The app is running on port 5000 as per configuration in docker compose file

   - Simple curl commands to make api calls 

		    curl -i -H "Content-Type: application/json" -X POST -d "@logon.json" http://0.0.0.0:5000/login
			curl -i -H "Content-Type: application/json" -X POST -d "@purchase_sword.json" http://0.0.0.0:5000/purchase_a_sword
			curl -i -H "Content-Type: application/json" -X POST -d "@join_guild.json" http://0.0.0.0:5000/join_a_guild
			curl -i -H "Content-Type: application/json" -X POST -d "@logon.json" http://0.0.0.0:5000/logout

   - Form requests which redirects to a form to enter the necessary data

			http://0.0.0.0:5000/login
			http://0.0.0.0:5000/purchase_a_sword
			http://0.0.0.0:5000/join_a_guild
			http://0.0.0.0:5000/logout

   - The below is the login route through apache bench

			docker-compose exec mids \
			  ab \
			  -n 1 \
			  -H 'Host: user1.comcast.com' \
			  -p '/w205/full-stack/logon.json'  \
			  -T 'application/json'   \
			  http://0.0.0.0:5000/login

     which gives the response 
        ```Hello !! Divya You are successfully logged in"```

   - The below is the api call/curl command for testing "purchase a sword" event/api using api call through apache bench

		    docker-compose exec mids \
			  ab \
			  -n 5 \
			  -H 'Host: user1.comcast.com' \
			  -p '/w205/full-stack/purchase_sword.json'  \
			  -T 'application/json'   \
			  http://0.0.0.0:5000/purchase_a_sword
       
     which maps to single API call
     ```GET /purchase_a_sword```, which gives the response

       ```Hello divya!! You purchased a Sword successfully!```

   - The below is the api call/curl command for "join a guild" event/api through apache bench

		    docker-compose exec mids \
		    ab \
		    -n 5 \
		    -H 'Host: user1.comcast.com' \
		    -p '/w205/full-stack/join_guild.json'  \
		    -T 'application/json'   \
		    http://0.0.0.0:5000/join_a_guild
       
     which maps to single API call
     ```GET /join_a_guild```, which gives the response

       ```Hey divya!! You joined the guild masadons successfully!```


   - The below is the logout route through apache bench

			docker-compose exec mids \
			  ab \
			  -n 1 \
			  -H 'Host: user1.comcast.com' \
			  -p '/w205/full-stack/logon.json'  \
			  -T 'application/json'   \
			  http://0.0.0.0:5000/logout

     which gives the response 
        ```Thank You !! You are logged out successfully!```     

       
 * I am using kafkacat to consume events from the *curlProjectEvents* topic 
   The below lists the events of the api server in the chronological order

       docker-compose exec mids \
       kafkacat -C -b kafka:29092 -t curlProjectEvents -o beginning -e
       
       WHICH OUTPUTS

        {"logged_in_user": "divya", "Content-Length": "24", "event_type": "user_login", "joined_guild": "", "purchased_sword": "", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "more_purchase_info": "", "Accept": "*/*", "Content-Type": "application/json", "remote_ip": "127.0.0.1"}
		{"logged_in_user": "divya", "Content-Length": "22", "event_type": "user_login", "joined_guild": "", "purchased_sword": "", "User-Agent": "curl/7.55.1", "Host": "0.0.0.0:5000", "more_purchase_info": "", "Accept": "*/*", "Content-Type": "application/json", "remote_ip": "172.23.0.1"}
		{"logged_in_user": "divya", "Content-Length": "97", "event_type": "purchase_sword", "joined_guild": "", "purchased_sword": "supersword", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "more_purchase_info": {"username": "divya", "itemtype": "supersword", "comments": "I like the sword too much"}, "Accept": "*/*", "Content-Type": "application/json", "remote_ip": "127.0.0.1"}
		{"logged_in_user": "divya", "Content-Length": "24", "event_type": "user_logout", "joined_guild": "", "purchased_sword": "", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "more_purchase_info": "", "Accept": "*/*", "Content-Type": "application/json", "remote_ip": "127.0.0.1"}
		{"logged_in_user": "raaga", "Content-Length": "24", "event_type": "user_login", "joined_guild": "", "purchased_sword": "", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "more_purchase_info": "", "Accept": "*/*", "Content-Type": "application/json", "remote_ip": "127.0.0.1"}
		{"logged_in_user": "raaga", "Content-Length": "93", "event_type": "purchase_sword", "joined_guild": "", "purchased_sword": "kataga", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "more_purchase_info": {"username": "raaga", "itemtype": "kataga", "comments": "I like the sword too much"}, "Accept": "*/*", "Content-Type": "application/json", "remote_ip": "127.0.0.1"}
		{"logged_in_user": "raaga", "Content-Length": "93", "event_type": "purchase_sword", "joined_guild": "", "purchased_sword": "kataga", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "more_purchase_info": {"username": "raaga", "itemtype": "kataga", "comments": "I like the sword too much"}, "Accept": "*/*", "Content-Type": "application/json", "remote_ip": "127.0.0.1"}
		{"logged_in_user": "raaga", "Content-Length": "93", "event_type": "join_a_guild", "joined_guild": "ligers", "purchased_sword": "", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "more_purchase_info": {"username": "raaga", "itemtype": "ligers", "comments": "I like the guild too much"}, "Accept": "*/*", "Content-Type": "application/json", "remote_ip": "127.0.0.1"}
		{"logged_in_user": "raaga", "Content-Length": "24", "event_type": "user_logout", "joined_guild": "", "purchased_sword": "", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "more_purchase_info": "", "Accept": "*/*", "Content-Type": "application/json", "remote_ip": "127.0.0.1"}
		..........................
		% Reached end of topic curlProjectEvents [0] at offset 35: exiting



* I am using kafkacat to consume events from the *formProjectEvents* topic
   The below lists the events of the api server in the chronological order

       docker-compose exec mids \
       kafkacat -C -b kafka:29092 -t formProjectEvents -o beginning -e

       
       WHICH OUTPUTS

       {"logged_in_user": "divya", "Origin": "http://0.0.0.0:5000", "Content-Length": "14", "Accept-Language": "en-US,en;q=0.9", "event_type": "user_login", "Accept-Encoding": "gzip, deflate", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "joined_guild": "", "Host": "0.0.0.0:5000", "purchased_sword": "", "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.117 Safari/537.36", "Connection": "keep-alive", "more_purchase_info": "", "Cache-Control": "max-age=0", "Cookie": "session=eyJ1c2VybmFtZSI6ImRpdnlhIn0.Db3aZg.ZTviD6qhNfSQY6Qme0yJjYm8bw8", "Referer": "http://0.0.0.0:5000/login", "Upgrade-Insecure-Requests": "1", "Content-Type": "application/x-www-form-urlencoded", "remote_ip": "172.23.0.1"}
	   {"logged_in_user": "raaga", "Origin": "http://0.0.0.0:5000", "Content-Length": "14", "Accept-Language": "en-US,en;q=0.9", "event   type": "user_login", "Accept-Encoding": "gzip, deflate", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image   webp,image/apng,*/*;q=0.8", "joined_guild": "", "Host": "0.0.0.0:5000", "purchased_sword": "", "User-Agent": "Mozilla/5.0 (Macintosh;   Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.117 Safari/537.36", "Connection": "keep-alive", "more   purchase_info": {}, "Cache-Control": "max-age=0", "Cookie": "session=eyJ1c2VybmFtZSI6ImRpdnlhIn0.Db4DOg.ZZeTt1eFRVhTb7Km6O8LCz8b4f8",   "Referer": "http://0.0.0.0:5000/login", "Upgrade-Insecure-Requests": "1", "Content-Type": "application/x-www-form-urlencoded",   "remote_ip": "172.23.0.1"}
	   {"logged_in_user": "raaga", "Origin": "http://0.0.0.0:5000", "Content-Length": "34", "Accept-Language": "en-US,en;q=0.9", "event   type": "purchase_sword", "Accept-Encoding": "gzip, deflate", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image   webp,image/apng,*/*;q=0.8", "joined_guild": "", "Host": "0.0.0.0:5000", "purchased_sword": "supersword", "User-Agent": "Mozilla/5.0    Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.117 Safari/537.36", "Connection":   "keep-alive", "more_purchase_info": {"username": "raaga", "itemtype": "supersword", "comments": "divya"}, "Cache-Control":   "max-age=0", "Cookie": "session=eyJ1c2VybmFtZSI6InJhYWdhIn0.Db4DQg.0WcUFshj-J1PvlSHfjS09FhWRbc", "Referer": "http://0.0.0.0:5000   purchase_a_sword", "Upgrade-Insecure-Requests": "1", "Content-Type": "application/x-www-form-urlencoded", "remote_ip": "172.23.0.1"}
	   {"logged_in_user": "raaga", "Origin": "http://0.0.0.0:5000", "Content-Length": "30", "Accept-Language": "en-US,en;q=0.9", "event   type": "purchase_sword", "Accept-Encoding": "gzip, deflate", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image   webp,image/apng,*/*;q=0.8", "joined_guild": "", "Host": "0.0.0.0:5000", "purchased_sword": "kataya", "User-Agent": "Mozilla/5.0    Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.117 Safari/537.36", "Connection":   "keep-alive", "more_purchase_info": {"username": "raaga", "itemtype": "kataya", "comments": "raaga"}, "Cache-Control": "max-age=0",   "Cookie": "session=eyJ1c2VybmFtZSI6InJhYWdhIn0.Db4DQg.0WcUFshj-J1PvlSHfjS09FhWRbc", "Referer": "http://0.0.0.0:5000/purchase_a_sword",   "Upgrade-Insecure-Requests": "1", "Content-Type": "application/x-www-form-urlencoded", "remote_ip": "172.23.0.1"}
	   {"logged_in_user": "raaga", "Origin": "http://0.0.0.0:5000", "Content-Length": "32", "Accept-Language": "en-US,en;q=0.9", "event   type": "join_a_guild", "Accept-Encoding": "gzip, deflate", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image   webp,image/apng,*/*;q=0.8", "joined_guild": "masadons", "Host": "0.0.0.0:5000", "purchased_sword": "", "User-Agent": "Mozilla/5.0    Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.117 Safari/537.36", "Connection":   "keep-alive", "more_purchase_info": {"username": "raaga", "itemtype": "masadons", "comments": "divya"}, "Cache-Control": "max-age=0",   "Cookie": "session=eyJ1c2VybmFtZSI6InJhYWdhIn0.Db4DQg.0WcUFshj-J1PvlSHfjS09FhWRbc", "Referer": "http://0.0.0.0:5000/join_a_guild",   "Upgrade-Insecure-Requests": "1", "Content-Type": "application/x-www-form-urlencoded", "remote_ip": "172.23.0.1"}
	   {"logged_in_user": "raaga", "Origin": "http://0.0.0.0:5000", "Content-Length": "46", "Accept-Language": "en-US,en;q=0.9", "event   type": "join_a_guild", "Accept-Encoding": "gzip, deflate", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image   webp,image/apng,*/*;q=0.8", "joined_guild": "ligers", "Host": "0.0.0.0:5000", "purchased_sword": "", "User-Agent": "Mozilla/5.0    Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.117 Safari/537.36", "Connection":   "keep-alive", "more_purchase_info": {"username": "raaga", "itemtype": "ligers", "comments": "raaga took this sword"}, "Cache-Control":   "max-age=0", "Cookie": "session=eyJ1c2VybmFtZSI6InJhYWdhIn0.Db4DQg.0WcUFshj-J1PvlSHfjS09FhWRbc", "Referer": "http://0.0.0.0:5000/join_   _guild", 	"Upgrade-Insecure-Requests": "1", "Content-Type": "application/x-www-form-urlencoded", "remote_ip": "172.23.0.1"}
		% Reached end of topic formProjectEvents [0] at offset 6: exiting



 * I am creating/capturing the events in a py file called sepaarate_events_curl.py and running the file as below
  
       docker-compose exec spark \
       spark-submit \
       /w205/spark-from-files/sepaarate_events_curl.py
      
 * I have the below code in my sepaarate_events_curl.py file to extract the raw events

       raw_events = spark \
       .read \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "kafka:29092") \
       .option("subscribe","curlProjectEvents") \
       .option("startingOffsets", "earliest") \
       .option("endingOffsets", "latest") \
       .load()

 * I am creating the function to extract munged events

       @udf('string')
       def munge_event(event_as_json):
       event = json.loads(event_as_json)
       event['Host'] = "divya" # silly change to show it works
       event['Cache-Control'] = "no-cache"
       return json.dumps(event) 

 * I am extracting the munged events and creating a column to save them, casting events as strings and showing them

        munged_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))    
       

 * I am extracting events from munged column of munged events

       extracted_events = munged_events \
        .rdd \
        .map(lambda x: Row(timestamp=x.timestamp, **json.loads(x.raw))) \
        .toDF()
       extracted_events.show() 

       PRINTS
       
       +------+--------------+----------------+-----------------+---------------+--------------+------------+--------------+--------------------+---------------+---------+--------------------+
       |Accept|Content-Length|    Content-Type|             Host|     User-Agent|    event_type|joined_guild|logged_in_user|  more_purchase_info|purchased_sword|remote_ip|           timestamp|
       +------+--------------+----------------+-----------------+---------------+--------------+------------+--------------+--------------------+---------------+---------+--------------------+
       |   */*|            24|application/json|user1.comcast.com|ApacheBench/2.3|    user_login|            |         raaga|               Map()|               |127.0.0.1|2018-04-22 11:37:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         raaga|Map(comments -> I...|         kataga|127.0.0.1|2018-04-22 11:38:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         raaga|Map(comments -> I...|         kataga|127.0.0.1|2018-04-22 11:38:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         raaga|Map(comments -> I...|         kataga|127.0.0.1|2018-04-22 11:38:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         raaga|Map(comments -> I...|         kataga|127.0.0.1|2018-04-22 11:38:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         raaga|Map(comments -> I...|         kataga|127.0.0.1|2018-04-22 11:38:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|  join_a_guild|      ligers|         raaga|Map(comments -> I...|               |127.0.0.1|2018-04-22 11:38:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|  join_a_guild|      ligers|         raaga|Map(comments -> I...|               |127.0.0.1|2018-04-22 11:38:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|  join_a_guild|      ligers|         raaga|Map(comments -> I...|               |127.0.0.1|2018-04-22 11:38:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|  join_a_guild|      ligers|         raaga|Map(comments -> I...|               |127.0.0.1|2018-04-22 11:38:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|  join_a_guild|      ligers|         raaga|Map(comments -> I...|               |127.0.0.1|2018-04-22 11:38:...|
       |   */*|            24|application/json|user1.comcast.com|ApacheBench/2.3|   user_logout|            |         raaga|               Map()|               |127.0.0.1|2018-04-22 11:38:...|
       |   */*|            24|application/json|user1.comcast.com|ApacheBench/2.3|    user_login|            |         divya|               Map()|               |127.0.0.1|2018-04-22 11:38:...|
       |   */*|            97|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         divya|Map(comments -> I...|     supersword|127.0.0.1|2018-04-22 11:39:...|
       |   */*|            97|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         divya|Map(comments -> I...|     supersword|127.0.0.1|2018-04-22 11:39:...|
       |   */*|            97|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         divya|Map(comments -> I...|     supersword|127.0.0.1|2018-04-22 11:39:...|
       |   */*|            97|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         divya|Map(comments -> I...|     supersword|127.0.0.1|2018-04-22 11:39:...|
       |   */*|            97|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         divya|Map(comments -> I...|     supersword|127.0.0.1|2018-04-22 11:39:...|
       |   */*|            95|application/json|user1.comcast.com|ApacheBench/2.3|  join_a_guild|    masadons|         divya|Map(comments -> I...|               |127.0.0.1|2018-04-22 11:39:...|
       |   */*|            95|application/json|user1.comcast.com|ApacheBench/2.3|  join_a_guild|    masadons|         divya|Map(comments -> I...|               |127.0.0.1|2018-04-22 11:39:...|
       +------+--------------+----------------+-----------------+---------------+--------------+------------+--------------+--------------------+---------------+---------+--------------------+
      
 * I am extracting and filtering individual items 

    - Login events

		    login_events = extracted_events \
		        .filter(extracted_events.event_type == 'user_login')
		    login_events.show()

    - Sword Purchase events 

	        sword_purchases = extracted_events \
	            .filter(extracted_events.event_type == 'purchase_sword')
		    sword_purchases.show()

    - Joined the guild events

		    joined_guild = extracted_events \
		        .filter(extracted_events.event_type == 'join_a_guild')
		    joined_guild.show()

    - Extract events for a specific user irrespective of the event type

		    user_sword_purchases = extracted_events \
		        .filter(extracted_events.logged_in_user == 'divya')
		    user_sword_purchases.show()
    
    - Extract events for a specific guild irrespective of the event type

		    joined_guild_user = extracted_events \
		        .filter(extracted_events.joined_guild == 'masadons')
		    joined_guild_user.show()

    - Logout events
    
		    logout_events = extracted_events \
		        .filter(extracted_events.event_type == 'user_logout')
		    logout_events.show()


 * I am creating another file called just_filtering.py in which I would extract events based on event type
   as different event types have different schema

		@udf('boolean')
		def is_purchase(event_as_json):
		    event = json.loads(event_as_json)
		    if event['event_type'] == 'purchase_sword':
		        return True
		    return False

		purchase_events = raw_events \
	    .select(raw_events.value.cast('string').alias('raw'),
	            raw_events.timestamp.cast('string')) \
	    .filter(is_purchase('raw'))

	    extracted_purchase_events = purchase_events \
	        .rdd \
	        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
	        .toDF()
	    extracted_purchase_events.printSchema()
	    extracted_purchase_events.show()


 * I am creating 2 more files called filtered_writes_curl.py and filtered_writes_form.py in which I would extract events based on event type
   and write it to HDFS


	    def is_login(event_as_json):
	    event = json.loads(event_as_json)
	    #event
	    if event['event_type'] == 'user_login':
	        return True
	    return False

	    # Login events and write into HDFS
	    login_events = raw_events \
	        .select(raw_events.value.cast('string').alias('raw'),
	                raw_events.timestamp.cast('string')) \
	        .filter(is_login('raw'))

	    login_events.printSchema()  

	    extracted_login_events = login_events \
	        .rdd \
	        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
	        .toDF()
	    extracted_login_events.printSchema()
	    extracted_login_events.show()    
	    

       PRINTS 
       +------+--------------+----------------+-----------------+---------------+----------+------------+--------------+------------------+---------------+---------+--------------------+
       |Accept|Content-Length|    Content-Type|             Host|     User-Agent|event_type|joined_guild|logged_in_user|more_purchase_info|purchased_sword|remote_ip|           timestamp|
       +------+--------------+----------------+-----------------+---------------+----------+------------+--------------+------------------+---------------+---------+--------------------+
       |   */*|            24|application/json|user1.comcast.com|ApacheBench/2.3|user_login|            |         divya|                  |               |127.0.0.1|2018-04-22 12:32:...|
       |   */*|            24|application/json|user1.comcast.com|ApacheBench/2.3|user_login|            |         raaga|                  |               |127.0.0.1|2018-04-22 12:40:...|
       +------+--------------+----------------+-----------------+---------------+----------+------------+--------------+------------------+---------------+---------+--------------------+

       extracted_login_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/login_events')

 * I am going to check hadoop tmp/login_events directory for written parquet files

       docker-compose exec cloudera hadoop fs -ls /tmp/login_events/
       WHICH OUTPUTS
       -rw-r--r--   1 root supergroup          0 2018-22-14 23:19 /tmp/login_events/_SUCCESS
       -rw-r--r--   1 root supergroup       1900 2018-22-14 23:19 /tmp/login_events/part-00000-8c7c6cc7-23b8-4112-872b-06413968428e-c000.snappy.parquet
       
            
  * Similarly creating purchase sword event, show the messages and write it into hadoop

        @udf('boolean')
        def is_purchase(event_as_json):
            event = json.loads(event_as_json)
            #event
            if event['event_type'] == 'purchase_sword':
                return True
            return False

        purchase_events = raw_events \
           .select(raw_events.value.cast('string').alias('raw'),
                   raw_events.timestamp.cast('string')) \
           .filter(is_purchase('raw'))
        purchase_events.printSchema()  

        extracted_purchase_events = purchase_events \
            .rdd \
            .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
            .toDF()
        extracted_purchase_events.printSchema()
        extracted_purchase_events.show()


       PRINTS
       
        +------+--------------+----------------+-----------------+---------------+--------------+------------+--------------+--------------------+---------------+---------+--------------------+
        |Accept|Content-Length|    Content-Type|             Host|     User-Agent|    event_type|joined_guild|logged_in_user|  more_purchase_info|purchased_sword|remote_ip|           timestamp|
        +------+--------------+----------------+-----------------+---------------+--------------+------------+--------------+--------------------+---------------+---------+--------------------+
        |   */*|            97|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         divya|Map(comments -> I...|     supersword|127.0.0.1|2018-04-22 12:35:...|
        |   */*|            97|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         divya|Map(comments -> I...|     supersword|127.0.0.1|2018-04-22 12:35:...|
        |   */*|            97|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         divya|Map(comments -> I...|     supersword|127.0.0.1|2018-04-22 12:35:...|
        |   */*|            97|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         divya|Map(comments -> I...|     supersword|127.0.0.1|2018-04-22 12:35:...|
        |   */*|            97|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         divya|Map(comments -> I...|     supersword|127.0.0.1|2018-04-22 12:35:...|
        |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         raaga|Map(comments -> I...|         kataya|127.0.0.1|2018-04-22 12:40:...|
        |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         raaga|Map(comments -> I...|         kataya|127.0.0.1|2018-04-22 12:40:...|
        |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         raaga|Map(comments -> I...|         kataya|127.0.0.1|2018-04-22 12:40:...|
        |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         raaga|Map(comments -> I...|         kataya|127.0.0.1|2018-04-22 12:40:...|
        |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|purchase_sword|            |         raaga|Map(comments -> I...|         kataya|127.0.0.1|2018-04-22 12:40:...|
        +------+--------------+----------------+-----------------+---------------+--------------+------------+--------------+--------------------+---------------+---------+--------------------+
       
        extracted_purchase_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/sword_purchases')

 * I am checking for the written events at tmp/sword_purchases

       docker-compose exec cloudera hadoop fs -ls /tmp/sword_purchases/
       WHICH OUTPUTS
       -rw-r--r--   1 root supergroup          0 2018-22-14 23:19 /tmp/sword_purchases/_SUCCESS
       -rw-r--r--   1 root supergroup       2343 2018-22-14 23:19 /tmp/sword_purchases/part-00000-bb317fd6-0071-4724-a980-b5194e77de4c-c000.snappy.parquet
 
 * Same is the case for join a guild and logout events

 * Extract events based on specific conditions as below

       @udf('boolean')
       def is_user_join_guild(event_as_json):
           event = json.loads(event_as_json)
           #event
           event['more_purchase_info']
           if event['event_type'] == 'join_a_guild' and event['logged_in_user'] == 'raaga' 
           and event['joined_guild'] == 'ligers':
               return True
           return False

       user_guild_joined = raw_events \
           .select(raw_events.value.cast('string').alias('raw'),
                   raw_events.timestamp.cast('string')) \
           .filter(is_user_join_guild('raw'))
       user_guild_joined.printSchema()   

       extracted_user_guild_events = user_guild_joined \
           .rdd \
           .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
           .toDF()
       extracted_user_guild_events.printSchema()
       extracted_user_guild_events.show()


       PRINTS
       
       +------+--------------+----------------+-----------------+---------------+------------+------------+--------------+--------------------+---------------+---------+--------------------+
       |Accept|Content-Length|    Content-Type|             Host|     User-Agent|  event_type|joined_guild|logged_in_user|  more_purchase_info|purchased_sword|remote_ip|           timestamp|
       +------+--------------+----------------+-----------------+---------------+------------+------------+--------------+--------------------+---------------+---------+--------------------+
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|join_a_guild|      ligers|         raaga|Map(comments -> I...|               |127.0.0.1|2018-04-22 12:41:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|join_a_guild|      ligers|         raaga|Map(comments -> I...|               |127.0.0.1|2018-04-22 12:41:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|join_a_guild|      ligers|         raaga|Map(comments -> I...|               |127.0.0.1|2018-04-22 12:41:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|join_a_guild|      ligers|         raaga|Map(comments -> I...|               |127.0.0.1|2018-04-22 12:41:...|
       |   */*|            93|application/json|user1.comcast.com|ApacheBench/2.3|join_a_guild|      ligers|         raaga|Map(comments -> I...|               |127.0.0.1|2018-04-22 12:41:...|
       +------+--------------+----------------+-----------------+---------------+------------+------------+--------------+--------------------+---------------+---------+--------------------+

   
   	   extracted_user_guild_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/joined_guilds_user')
       
 * I am checking for the written events at tmp/joined_guilds_user

       docker-compose exec cloudera hadoop fs -ls /tmp/sword_purchases/
       WHICH OUTPUTS
       -rw-r--r--   1 root supergroup          0 2018-22-14 23:19 /tmp/joined_guilds_user/_SUCCESS
       -rw-r--r--   1 root supergroup       2343 2018-22-14 23:19 /tmp/joined_guilds_user/part-00000-bb317fd6-0074-4724-a980-b5194e77de4c-c000.snappy.parquet
       

 * I am creating a new file write_hive_table.py adding the spark code to read the events with presto,
    - Read parquet from what we wrote into hdfs
    - Register temp table
    - Create external table
    - Store as parquet

		    extracted_purchase_events.registerTempTable("extracted_purchase_events")

		    spark.sql("""
			create external table extracted_purchases
			stored as parquet
			location '/tmp/extracted_purchases'
			as
			select * from extracted_purchase_events
			""")	

 * I am reading the events with presto.

      	docker-compose exec presto presto --server presto:8080 --catalog hive --schema default
	
 * I am checking for the existing tables
 
        show tables
	
 * I am describing the purchase tables
      
       describe extracted_purchases
	

 * I am taking down the cluster
       
       docker-compose down  

