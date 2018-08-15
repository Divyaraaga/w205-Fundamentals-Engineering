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

# Assignment 10

## Summary
  * In this exercise. we are spun up a cluster with kafka, zookeeper and the mids container,
  * Created a web based gaming application with apis written using python
  * We are focusing on 2 events
       1. Default route event
       2. Purchase a sword event
       3. Join a guild event    
  * Publishing the events to kafka when respective events are triggered by the user 
  * Reading the publised events through spark
 
  
## Detailed Steps in the process.

 * I am spinning up my kafka, zookeeper and mids cluster
  
       docker-compose up -d
      
 * I am checking for kafka broker logs
     
       docker-compose logs -f kafka
      
 * I am checking for kafka broker logs
     
       docker-compose exec kafka \
       kafka-topics \
         --create \
         --topic eventsForm \
         --partitions 1 \
         --replication-factor 1 \
         --if-not-exists \
         --zookeeper zookeeper:32181

        OUTPUT
        Created Topic eventsForm.
        
 * I am publishing a sequence of 1 - 42 numbers as Messages to the topic eventsForm 
    to test if kafka is up and ready to consume messages
     
       docker-compose exec kafka \
       bash -c "seq 42 | kafka-console-producer \
        --request-required-acks 1 \
        --broker-list localhost:29092 \
        --topic eventsForm && echo 'Produced 42 messages.'"
       
       OUTPUT
       >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Produced 42 messages.
    
 * I am starting a consumer to read from the topic eventsForm
    
       docker-compose exec kafka \
       kafka-console-consumer \
         --bootstrap-server localhost:29092 \
         --topic eventsForm \
         --from-beginning \
         --max-messages 42


 * Create the function for "log_to_kafka" event in the game_api_with_json_events.py,
   This function publishes the events e.g : "purchase a sword" , "join_a_guild" to kafka 
   The python flask library is used to write the API server.
   
       def log_to_kafka(topic, event):
       event.update(request.headers)
       producer.send(topic, json.dumps(event).encode())

 * Create the default route event in the game_api_with_json_events.py,
   The python flask library is used to write the API server, this event captures the ip address of the user's machine 

       @app.route("/")
       def default_response():
           # Building the json to capture the event type and 
           # the ip address of the user's machine
           default_event = {
            'event_type': 'default',
            'remote_ip' : request.remote_addr
           }
       # log the event in the form of json to kafka
       log_to_kafka('eventsForm', default_event)
       return "This is the default response!\n" + request.remote_addr


 * Create the route for "purchase a sword" event in the game_api_with_json_events.py,
   The purchase event captures the user name and the ip address of the user's machine
   and logs it to kafka
   
       @app.route("/purchase_a_sword/<username>")
       def purchase_a_sword(username):
           # Building the json to capture the event type, the user name
           # the ip address of the user's machine
           purchase_sword_event = {
            'event_type': 'purchase_sword',
            'remote ip' : request.remote_addr,
            'purchase_user' : username
           }
       # log the event to kafka
       log_to_kafka('eventsForm', purchase_sword_event)
       return "Hello " + format(username) + "!! You purchased a Sword successfully!\n"  

 * Create the another route for "Join a guild" event in the game_api_with_json_events.py,
   The purchase event captures the user name, guild name and the ip address of the user's machine
   and logs it to kafka

       @app.route("/join_a_guild/<username>/<guildname>")
       def join_a_guild(username,guildname):
           # Building the json to capture the event type, the user name, the guild name
           # the ip address of the user's machine
           join_guild_event = {
            'event_type': 'purchase_sword',
            'remote ip' : request.remote_addr,
            'user_name' : username,
            'joined_guild':  guildname
           }
       # log the event to kafka
       log_to_kafka('eventsForm', join_guild_event)
       return "Hey " + format(username) + "!! You joined the guild " + format(guildname) + " successfully!\n"
        

 * I am running the game_api_with_json_events.py file as below
     
       docker-compose exec mids \
       env FLASK_APP=/w205/flask-with-kafka-and-spark/game_api_with_json_events.py \
       flask run --host 0.0.0.0

 * Testing the api server for the events, The app is running on port 5000 as per configuration in docker compose file
   - The below is the default route 

          docker-compose exec mids curl http://localhost:5000/  
       
     which gives the response 
        ```This is the default response! 172.18.0.1```

   - The below is the api call/curl command for testing "purchase a sword" event/api

         docker-compose exec mids curl http://localhost:5000/purchase_a_sword/divya
       
     which maps to single API call
     ```GET /purchase_a_sword```, which gives the response

       ```Hello divya!! You purchased a Sword successfully!```

   - The below is the api call/curl command for "join a guild" event/api

         docker-compose exec mids curl http://localhost:5000/join_a_guild/divya/masadons
       
     which maps to single API call
     ```GET /join_a_guild```, which gives the response

       ```Hey divya!! You joined the guild masadons successfully!```
       
 * I am using kafkacat to consume events from the *eventsForm* topic
   The below lists the events of the api server in the chronological order
    
       docker-compose exec mids bash -c "kafkacat -C -b kafka:29092 -t eventsForm -o beginning -e"
       
       which OUTPUTS
       
       {"event_type": "default"}
       {"event_type": "default"}
       {"event_type": "default"}
       {"event_type": "purchase_sword"}
       {"event_type": "purchase_sword"}
       {"event_type": "purchase_sword"}
       {"Accept-Language": "en-US,en;q=0.9", "event_type": "default", "Host": "0.0.0.0:5000", "Accept": "text/html,application/xhtml+xml,application/ xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "Upgrade-Insecure-Requests": "1", "Connection": "keep-alive", "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36", "Accept-Encoding": "gzip, deflate"}
       {"Accept-Language": "en-US,en;q=0.9", "event_type": "purchase_sword", "Host": "0.0.0.0:5000", "Accept": "text/html,application/xhtml+xml,appcation/xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "Upgrade-Insecure-Requests": "1", "Connection": "keep-alive", "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36", "Accept-Encoding": "gzip, deflate"}
       .......
       % Reached end of topic eventsForm [0] at offset 24: exiting


 * I am spinning up a pyspark process using the spark container
  
       docker-compose exec spark pyspark
      
 * I am reading messages from kafka in pyspark

       raw_events = spark \
       .read \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "kafka:29092") \
       .option("subscribe","eventsForm") \
       .option("startingOffsets", "earliest") \
       .option("endingOffsets", "latest") \
       .load()

 * I am printing the schema of messages
       
       raw_events.printSchema()

 * Command to see the messages
       
       raw_events.show()  
      
 * I am casting messages as strings and show the messages
       
       eventsForm = raw_events.select(raw_events.value.cast('string'))
       eventsForm.show()

 * Explore the events and put them into dataframe and show the messages  
       
       extracted_events = eventsForm.rdd.map(lambda x: json.loads(x.value)).toDF()
       extracted_events.show(50)
       PRINTS
        +--------------+
        |    event_type|
        +--------------+
        |       default|
        |       default|
        |       default|
        |purchase_sword|
        |purchase_sword|
        |purchase_sword|
        |       default|
        |purchase_sword|
        |purchase_sword|
        |       default|
        |       default|
        |       default|
        |purchase_sword|
        |purchase_sword|
        |purchase_sword|
        |purchase_sword|
        |       default|
        |       default|
        |       default|
        |       default|
        |purchase_sword|
        |purchase_sword|
        |purchase_sword|
        |purchase_sword|
        |       default|
        |purchase_sword|
        |purchase_sword|
        |purchase_sword|
        |  join_a_guild|
        |  join_a_guild|
        +--------------+  


 * I am printing the count of messages   
       
       extracted_events.count()   
       Prints 
       30
      
 * I am reading/retrieving the first value from the json messages
       
       extracted_events.select("event_type").take(1)
       Prints
       [Row(event_type='default')]

       extracted_events.select("event_type").take(27)[26]
       Prints
       Row(event_type='purchase_sword')

       extracted_events.select("event_type").take(30)[29]
       Prints
       Row(event_type='join_a_guild')

 * exit from pyspark
       
       exit()

 * I am taking down the cluster
       
       docker-compose down  




