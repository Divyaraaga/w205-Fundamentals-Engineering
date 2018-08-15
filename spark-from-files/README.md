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

# Assignment 11

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
 
  
## Detailed Steps in the process.

 * I am spinning up my kafka, zookeeper and mids cluster
  
       docker-compose up -d
      
 * I am checking for kafka broker logs
     
       docker-compose logs -f kafka

 * I am checking for Hadoop logs

       docker-compose logs -f cloudera      
      
 * I am checking for kafka broker logs
     
       docker-compose exec kafka \
       kafka-topics \
         --create \
         --topic filesEventsFilter \
         --partitions 1 \
         --replication-factor 1 \
         --if-not-exists \
         --zookeeper zookeeper:32181

        OUTPUT
        Created Topic filesEventsFilter.
        
 * I am publishing a sequence of 1 - 42 numbers as Messages to the topic filesEventsFilter 
    to test if kafka is up and ready to consume messages
     
       docker-compose exec kafka \
       bash -c "seq 42 | kafka-console-producer \
        --request-required-acks 1 \
        --broker-list localhost:29092 \
        --topic filesEventsFilter && echo 'Produced 42 messages.'"
       
       OUTPUT
       >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Produced 42 messages.
    
 * I am starting a consumer to read from the topic eventsForm
    
       docker-compose exec kafka \
       kafka-console-consumer \
         --bootstrap-server localhost:29092 \
         --topic filesEventsFilter \
         --from-beginning \
         --max-messages 42

 * I am checking at tmp/ directory in hdfs and see that what we want to write isn't there already

       docker-compose exec cloudera hadoop fs -ls /tmp/

 * Create the function for "log_to_kafka" event in the game_api.py,
   This function publishes the events e.g : "purchase a sword" , "join a guild" to kafka 
   The python flask library is used to write the API server.
   
       def log_to_kafka(topic, event):
       event.update(request.headers)
       producer.send(topic, json.dumps(event).encode())

 * Create the default route event in the game_api.py,
   The python flask library is used to write the API server, this event captures the ip address of the user's machine 
   Adding additional fields with empty values to get the entire schema to extract events based on varied filter criteria

       @app.route("/")
       def default_response():
           # Building the json to capture the event type and 
           # the ip address of the user's machine
           default_event = {
            'event_type': 'default',
            'remote_ip' : request.remote_addr,
            'purchase_user' : '',
            'joined_guild':  ''
           }
       # log the event in the form of json to kafka
       log_to_kafka('filesEventsFilter', default_event)
       return "This is the default response!\n" + request.remote_addr


 * Create the route for "purchase a sword" event in the game_api.py,
   The purchase event captures the user name and the ip address of the user's machine
   and logs it to kafka
   
       @app.route("/purchase_a_sword/<username>")
       def purchase_a_sword(username):
           # Building the json to capture the event type, the user name
           # the ip address of the user's machine
           purchase_sword_event = {
            'event_type': 'purchase_sword',
            'remote ip' : request.remote_addr,
            'purchase_user' : username,
            'joined_guild':  ''
           }
       # log the event to kafka
       log_to_kafka('filesEventsFilter', purchase_sword_event)
       return "Hello " + format(username) + "!! You purchased a Sword successfully!\n"  

 * Create the another route for "Join a guild" event in the game_api.py,
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
       log_to_kafka('filesEventsFilter', join_guild_event)
       return "Hey " + format(username) + "!! You joined the guild " + format(guildname) + " successfully!\n"
        

 * I am running the game_api.py file as below
     
       docker-compose exec mids \
       env FLASK_APP=/w205/flask-with-kafka-and-spark/game_api.py \
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
       
 * I am using kafkacat to consume events from the *filesEventsFilter* topic
   The below lists the events of the api server in the chronological order
    
       docker-compose exec mids bash -c "kafkacat -C -b kafka:29092 -t filesEventsFilter -o beginning -e"
       
       WHICH OUTPUTS

       {"Accept-Language": "en-US,en;q=0.9", "event_type": "default", "Accept-Encoding": "gzip, deflate", "joined_guild": "", "Host": "0.0.0.0:5000", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36", "Connection": "keep-alive", "Cache-Control": "max-age=0", "purchase_user": "", "Upgrade-Insecure-Requests": "1", "remote_ip": "172.23.0.1"}
       {"Accept-Language": "en-US,en;q=0.9", "event_type": "purchase_sword", "Accept-Encoding": "gzip, deflate", "joined_guild": "", "Host": "0.0.0.0:5000", "Accept": "text/       html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (       KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36", "Connection": "keep-alive", "Cache-Control": "max-age=0", "purchase_user": "divya", "Upgrade-Insecure-Requests":       "1", "remote_ip": "172.23.0.1"}
       {"Accept-Language": "en-US,en;q=0.9", "event_type": "purchase_sword", "Accept-Encoding": "gzip, deflate", "joined_guild": "", "Host": "0.0.0.0:5000", "Accept": "text/       html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (       KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36", "Connection": "keep-alive", "Cache-Control": "max-age=0", "purchase_user": "raaga", "Upgrade-Insecure-Requests":       "1", "remote_ip": "172.23.0.1"}
       {"Accept-Language": "en-US,en;q=0.9", "event_type": "join_a_guild", "Accept-Encoding": "gzip, deflate", "joined_guild": "masadons", "Host": "0.0.0.0:5000", "Accept":        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6)        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36", "Connection": "keep-alive", "Cache-Control": "max-age=0", "purchase_user": "divya",        "Upgrade-Insecure-Requests": "1", "remote_ip": "172.23.0.1"}
       {"Accept-Language": "en-US,en;q=0.9", "event_type": "join_a_guild", "Accept-Encoding": "gzip, deflate", "joined_guild": "wildhorses", "Host": "0.0.0.0:5000", "Accept":        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8", "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6)        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36", "Connection": "keep-alive", "Cache-Control": "max-age=0", "purchase_user": "raaga",        "Upgrade-Insecure-Requests": "1", "remote_ip": "172.23.0.1"}
       % Reached end of topic filesEventsFilter [0] at offset 5: exiting



 * I am creating/capturing the events in a py file called extract_events.py and running the file as below
  
       docker-compose exec spark \
       spark-submit \
       /w205/spark-from-files/extract_events.py
      
 * I have the below code in my extract_events.py file to extract the raw events

       raw_events = spark \
       .read \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "kafka:29092") \
       .option("subscribe","filesEventsFilter") \
       .option("startingOffsets", "earliest") \
       .option("endingOffsets", "latest") \
       .load()

 * I am casting messages as strings in 
 * I am going to extract the json fields , I'm going to get rdd (spark's distributed dataset), apply a map to it to work as a df
 * I am going to show the messages,
       
       events = raw_events.select(raw_events.value.cast('string'))
       extracted_events = events.rdd.map(lambda x: json.loads(x.value)).toDF()
       extracted_events.show() 

       PRINTS
       +--------------------+---------------+---------------+-------------+----------+------------+-------------------------+--------------------+--------------+------------+-------------+----------+
       |              Accept|Accept-Encoding|Accept-Language|Cache-Control|Connection|        Host|Upgrade-Insecure-Requests|          User-Agent|    event_type|joined_       guild|purchase_user| remote_ip|
       +--------------------+---------------+---------------+-------------+----------+------------+-------------------------+--------------------+--------------+------------+--       -----------+----------+
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|    max-age=0|keep-alive|0.0.0.0:5000|                        1|Mozilla/5.0 (Maci...|       default|                   |             |172.23.0.1|
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|    max-age=0|keep-alive|0.0.0.0:5000|                        1|Mozilla/5.0 (Maci...|purchase_sword|                   |        divya|172.23.0.1|
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|    max-age=0|keep-alive|0.0.0.0:5000|                        1|Mozilla/5.0 (Maci...|purchase_sword|                   |        raaga|172.23.0.1|
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|    max-age=0|keep-alive|0.0.0.0:5000|                        1|Mozilla/5.0 (Maci...|  join_a_guild|           masadons|        divya|172.23.0.1|
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|    max-age=0|keep-alive|0.0.0.0:5000|                        1|Mozilla/5.0 (Maci...|  join_a_guild|         wildhorses|        raaga|172.23.0.1|
       +--------------------+---------------+---------------+-------------+----------+------------+-------------------------+--------------------+--------------+------------+--       -----------+----------+

 * I am writing it into hadoop

       extracted_events \
        .write \
        .parquet("/tmp/extracted_events")
        
 * I am going to check hadoop tmp/extracted_events directory for written parquet files
       
       docker-compose exec cloudera hadoop fs -ls /tmp/extracted_events/
       WHICH OUTPUTS
       Found 2 items
       -rw-r--r--   1 root supergroup          0 2018-04-14 20:41 /tmp/extracted_events/_SUCCESS
       -rw-r--r--   1 root supergroup       1811 2018-04-14 20:41 /tmp/extracted_events/part-00000-fda08220-d2f4-4488-84d4-b8df51bb23b6-c000.snappy.parquet

 * I am creating a new file called transform_events.py and create the function to extract munged events

       @udf('string')
       def munge_event(event_as_json):
       event = json.loads(event_as_json)
       event['Host'] = "divya" # silly change to show it works
       event['Cache-Control'] = "no-cache"
       return json.dumps(event) 

 * I am extracting the munged events and creating a column to save them, casting events as strings and showing them

       munged_events = raw_events \
       .select(raw_events.value.cast('string').alias('raw'), \
               raw_events.timestamp.cast('string')) \
       .withColumn('munged', munge_event('raw'))
       munged_events.show()      
       
       PRINTS 
       +--------------------+--------------------+--------------------+
       |                 raw|           timestamp|              munged|
       +--------------------+--------------------+--------------------+
       |{"Host": "localho...|2018-04-14 20:33:...|{"Host": "divya",...|
       |{"Accept-Language...|2018-04-14 20:38:...|{"Accept-Language...|
       |{"Accept-Language...|2018-04-14 20:38:...|{"Accept-Language...|
       |{"Accept-Language...|2018-04-14 20:38:...|{"Accept-Language...|
       |{"Accept-Language...|2018-04-14 20:39:...|{"Accept-Language...|
       |{"Accept-Language...|2018-04-14 20:39:...|{"Accept-Language...|
       |{"Accept-Language...|2018-04-14 22:35:...|{"Accept-Language...|
       +--------------------+--------------------+--------------------+

 * I am extracting events from munged column of munged events

       extracted_events = munged_events.rdd.map(lambda r: json.loads(r.munged)).toDF()
       extracted_events.show()

       PRINTS

       +--------------------+---------------+---------------+-------------+----------+------------+-------------------------+--------------------+--------------+------------+-------------+----------+
       |              Accept|Accept-Encoding|Accept-Language|Cache-Control|Connection|        Host|Upgrade-Insecure-Requests|          User-Agent|    event_type|joined_       guild|purchase_user| remote_ip|
       +--------------------+---------------+---------------+-------------+----------+------------+-------------------------+--------------------+--------------+------------+--       -----------+----------+
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|    max-age=0|keep-alive|0.0.0.0:5000|                        1|Mozilla/5.0 (Maci...|       default|                   |             |172.23.0.1|
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|    max-age=0|keep-alive|0.0.0.0:5000|                        1|Mozilla/5.0 (Maci...|purchase_sword|                   |        divya|172.23.0.1|
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|    max-age=0|keep-alive|0.0.0.0:5000|                        1|Mozilla/5.0 (Maci...|purchase_sword|                   |        raaga|172.23.0.1|
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|    max-age=0|keep-alive|0.0.0.0:5000|                        1|Mozilla/5.0 (Maci...|  join_a_guild|           masadons|        divya|172.23.0.1|
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|    max-age=0|keep-alive|0.0.0.0:5000|                        1|Mozilla/5.0 (Maci...|  join_a_guild|         wildhorses|        raaga|172.23.0.1|
       +--------------------+---------------+---------------+-------------+----------+------------+-------------------------+--------------------+--------------+------------+--       -----------+----------+
      
 * I am writing it to hadoop

       extracted_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/extracted_events_munged")


 * I am going to check hadoop tmp/extracted_events_munged directory for written parquet files

       docker-compose exec cloudera hadoop fs -ls /tmp/extracted_events_munged/
       WHICH OUTPUTS
       -rw-r--r--   1 root supergroup          0 2018-04-14 23:07 /tmp/extracted_events_munged/_SUCCESS
       -rw-r--r--   1 root supergroup       2006 2018-04-14 23:07 /tmp/extracted_events_munged/part-00000-7a6344c1-5193-4a77-ba10-f8d54aa50858-c000.snappy.parquet

 * I am creating another file called transform_events_filtered.py in which I would extract events based on event type

 * The first event would be default, show the messages and write it into hadoop

       default_hits = extracted_events \
        .filter(extracted_events.event_type == 'default')

       default_hits.show()

       PRINTS 

       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+----------+------------+-------------+----------+
       |              Accept|Accept-Encoding|Accept-Language|Cache-Control|Connection| Host|Upgrade-Insecure-Requests|          User-Agent|event_type|joined_guild|purchase_      user| remote_ip|
       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+----------+------------+-------------      +----------+
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|     no-cache|keep-alive|divya|                        1|Mozilla/5.0 (Maci...|   default|            |                   |172.23.0.1|
       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+----------+------------+-------------      +----------+

       default_hits \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/default_hits")

 * I am going to check hadoop tmp/default_hits directory for written parquet files

       docker-compose exec cloudera hadoop fs -ls /tmp/default_hits/
       WHICH OUTPUTS
       -rw-r--r--   1 root supergroup          0 2018-04-14 23:19 /tmp/default_hits/_SUCCESS
       -rw-r--r--   1 root supergroup       1900 2018-04-14 23:19 /tmp/default_hits/part-00000-8c7c6cc7-23b8-4112-872b-06413968428e-c000.snappy.parquet
            
  * Similarly creating purchase sword event, show the messages and write it into hadoop

       sword_purchases = extracted_events \
        .filter(extracted_events.event_type == 'purchase_sword')

       sword_purchases.show()

       PRINTS
       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+--------------+------------+-------------+----------+
       |              Accept|Accept-Encoding|Accept-Language|Cache-Control|Connection| Host|Upgrade-Insecure-Requests|          User-Agent|    event_type|joined_guild|purchase_       user| remote_ip|
       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+--------------+------------+---------       ----+----------+
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|     no-cache|keep-alive|divya|                        1|Mozilla/5.0 (Maci...|purchase_sword|            |               divya|172.23.0.1|
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|     no-cache|keep-alive|divya|                        1|Mozilla/5.0 (Maci...|purchase_sword|            |               raaga|172.23.0.1|
       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+--------------+------------+---------       ----+----------+

       sword_purchases \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/sword_purchases_hits")

 * I am checking for the written events at tmp/sword_purchases_hits

       docker-compose exec cloudera hadoop fs -ls /tmp/sword_purchases_hits/
       WHICH OUTPUTS
       -rw-r--r--   1 root supergroup          0 2018-04-14 23:19 /tmp/sword_purchases_hits/_SUCCESS
       -rw-r--r--   1 root supergroup       2343 2018-04-14 23:19 /tmp/sword_purchases_hits/part-00000-bb317fd6-0071-4724-a980-b5194e77de4c-c000.snappy.parquet
 
 * Same is the case for join a guild.

       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+------------+------------+-------------+----------+
       |              Accept|Accept-Encoding|Accept-Language|Cache-Control|Connection| Host|Upgrade-Insecure-Requests|          User-Agent|  event_type|joined_guild|purchase_      user| remote_ip|
       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+------------+------------+-----------      --+----------+
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|     no-cache|keep-alive|divya|                        1|Mozilla/5.0 (Maci...|join_a_guild|    masadons|               divya|172.23.0.1|
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|     no-cache|keep-alive|divya|                        1|Mozilla/5.0 (Maci...|join_a_guild|  wildhorses|               raaga|172.23.0.1|
       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+------------+------------+-----------      --+----------+

 * Extract events based on specific conditions as below

       # Extract a events for a specific user irrespective of the event type
       user_sword_purchases = extracted_events \
           .filter(extracted_events.purchase_user == 'divya')
       user_sword_purchases.show()
       PRINTS

       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+--------------+------------+-------------+----------+
       |              Accept|Accept-Encoding|Accept-Language|Cache-Control|Connection| Host|Upgrade-Insecure-Requests|          User-Agent|    event_type|joined_guild|purchase_       user| remote_ip|
       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+--------------+------------+---------       ----+----------+
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|     no-cache|keep-alive|divya|                        1|Mozilla/5.0 (Maci...|purchase_sword|            |               divya|172.23.0.1|
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|     no-cache|keep-alive|divya|                        1|Mozilla/5.0 (Maci...|  join_a_guild|    masadons|               divya|172.23.0.1|
       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+--------------+------------+---------       ----+----------+
   
       user_sword_purchases \
           .write \
           .mode("overwrite") \
           .parquet("/tmp/sword_purchases_hits_divya")     
       
       # Extract a events for a specific guild irrespective of the event type
       joined_guild_user = extracted_events \
           .filter(extracted_events.joined_guild == 'masadons')
       joined_guild_user.show()
       PRINTS

       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+------------+------------+-------------+----------+
       |              Accept|Accept-Encoding|Accept-Language|Cache-Control|Connection| Host|Upgrade-Insecure-Requests|          User-Agent|  event_type|joined_guild|purchase_       user| remote_ip|
       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+------------+------------+-----------       --+----------+
       |text/html,applica...|  gzip, deflate| en-US,en;q=0.9|     no-cache|keep-alive|divya|                        1|Mozilla/5.0 (Maci...|join_a_guild|    masadons|               divya|172.23.0.1|
       +--------------------+---------------+---------------+-------------+----------+-----+-------------------------+--------------------+------------+------------+-----------       --+----------+ 
       

       joined_guild_user \
           .write \
           .mode("overwrite") \
           .parquet("/tmp/joined_guilds_user") 
       

 * I am taking down the cluster
       
       docker-compose down  




