# My annotations, Assignment 7

## Here I'm getting/downloading the data/json.
  530  curl -L -o assessment-attempts-20180128-121051-nested.json https://goo.gl/f5bRm4

## I'm spinning up my kafka, zookeeper, mids cluster.
  561  docker-compose up -d

## I am checking for kafka broker logs
  562  docker-compose logs -f kafka

## I'm am creating the kafka topic here
  563   docker-compose exec kafka \
        kafka-topics \
          --create \
          --topic assgn7real \
          --partitions 1 \
          --replication-factor 1 \
          --if-not-exists \
          --zookeeper zookeeper:32181

## I am checking the created topic assgn7real
   564  docker-compose exec kafka \
        kafka-topics \
          --describe \
          --topic assgn7real \
          --zookeeper zookeeper:32181

## Checking out messages from the downloaded json
    567  docker-compose exec mids bash -c 
        "cat /w205/spark-with-kafka/assessment-attempts-20180128-121051-nested.json"

    568  docker-compose exec mids bash -c 
        "cat /w205/spark-with-kafka/assessment-attempts-20180128-121051-nested.json | jq '.'"        

## I am publishing 100 Messages to the topic assgn6real from the downloaded json
   
    575  docker-compose exec mids   
          bash -c "cat /w205/spark-with-kafka/assessment-attempts-20180128-121051-nested.json \
          | jq '.[]' -c \
          | kafkacat -P -b kafka:29092 -t assgn7real && echo 'Produced 100 messages.'"

## I am spinning up a pyspark process using the spark container
    579  docker-compose exec spark pyspark

##  Once I spun up the pyspark process, history didnt capture the commands. 
##  I am reading messages from kafka in pyspark

    messages = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe","assgn7real") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

## I am printing the schema of messages
    messages.printSchema()    

## Command to see the messages
   messages.show()    

## I am casting messages as strings
   messages_as_strings=messages.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

## I am printing the schema of messages
   messages_as_strings.printSchema()

## I am printing the count of messages   
   messages_as_strings.count()   
   Prints 
   6560

## I am printing the messages
   messages_as_strings.show()

    +----+--------------------+
    | key|               value|
    +----+--------------------+
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    |null|{"keen_timestamp"...|
    +----+--------------------+
    only showing top 20 rows

## I am reading/retrieving the first value from the json messages
    messages_as_strings.select('value').take(1)

    Prints

    [Row(value='{"keen_timestamp":"1516717442.735266","max_attempts":"1.0","started_at":"2018-01-23T14:23:19.082Z","base_exam_    id":"37f0a30a-7464-11e6-aa92-a8667f27e5dc","user_exam_id":"6d4089e4-bde5-4a22-b65f-18bce9ab79c8","sequences":{"questions":[{"user_    incomplete":true,"user_correct":false,"options":[{"checked":true,"at":"2018-01-23T14:23:24.670Z","id":"49c574b4-5c82-4ffd-9bd1-c3358    faf850d","submitted":1,"correct":true},{"checked":true,"at":"2018-01-23T14:23:25.914Z","id":"f2528210-35c3-4320-acf3-9056567ea19f","    submitted":1,"correct":true},{"checked":false,"correct":true,"id":"d1bf026f-554f-4543-bdd2-54dcf105b826"}],"user_   submitted":true,"id":"7a2ed6d3-f492-49b3-b8aa-d080a8aad986","user_result":"missed_some"},{"user_incomplete":false,"user_correct":fal  se,"options":[{"checked":true,"at":"2018-01-23T14:23:30.116Z","id":"a35d0e80-8c49-415d-b8cb-c21a02627e2b","submitted":1},{"checked":   false,"correct":true,"id":"bccd6e2e-2cef-4c72-8bfa-317db0ac48bb"},{"checked":true,"at":"2018-01-23T14:23:41.791   Z","id":"7e0b639a-2ef8-4604-b7eb-5018bd81a91b","submitted":1,"correct":true}],"user_   submitted":true,"id":"bbed4358-999d-4462-9596-bad5173a6ecb","user_result":"incorrect"},{"user_incomplete":false,"user_correct":true,   "options":[{"checked":false,"at":"2018-01-23T14:23:52.510Z","id":"a9333679-de9d-41ff-bb3d-b239d6b95732"},{"checked":false,"id":"8579   5acc-b4b1-4510-bd6e-41648a3553c9"},{"checked":true,"at":"2018-01-23T14:23:54.223Z","id":"c185ecdb-48fb-4edb-ae4e-0204ac7a0909","subm   itted":1,"correct":true},{"checked":true,"at":"2018-01-23T14:23:53.862Z","id":"77a66c83-d001-45cd-9a5a-6bba8eb7389e","submitted":1,"   correct":true}],"user_submitted":true,"id":"e6ad8644-96b1-4617-b37b-a263dded202c","user_result":"correct"},{"user_   incomplete":false,"user_correct":true,"options":[{"checked":false,"id":"59b9fc4b-f239-4850-b1f9-912d1fd3ca13"},{"checked":false,"id"   :"2c29e8e8-d4a8-406e-9cdf-de28ec5890fe"},{"checked":false,"id":"62feee6e-9b76-4123-bd9e-c0b35126b1f1"},{"checked":true,"at":"2018-01   -23T14:24:00.807Z","id":"7f13df9c-fcbe-4424-914f-2206f106765c","submitted":1,"correct":true}],"user_   submitted":true,"id":"95194331-ac43-454e-83de-ea8913067055","user_result":"correct"}],"attempt":1,"id":"5b28a462-7a3b-42e0-b508-09f3   906d1703","counts":{"incomplete":1,"submitted":4,"incorrect":1,"all_correct":false,"correct":2,"total":4,"unanswered":0}},"keen_   created_at":"1516717442.735266","certification":"false","keen_id":"5a6745820eb8ab00016be1f1","exam_name":"Normal Forms and All That    Jazz Master Class"}')]


## I am reading/retrieving the first value from the first json message
    messages_as_strings.select('value').take(1)[0].value

    Prints json which is a string

    '{"keen_timestamp":"1516717442.735266","max_attempts":"1.0","started_at":"2018-01-23T14:23:19.082Z","base_exam_   id":"37f0a30a-7464-11e6-aa92-a8667f27e5dc","user_exam_id":"6d4089e4-bde5-4a22-b65f-18bce9ab79c8","sequences":{"questions":[{"user_  incomplete":true,"user_correct":false,"options":[{"checked":true,"at":"2018-01-23T14:23:24.670Z","id":"49c574b4-5c82-4ffd-9bd1-c3358   faf850d","submitted":1,"correct":true},{"checked":true,"at":"2018-01-23T14:23:25.914Z","id":"f2528210-35c3-4320-acf3-9056567ea19f","  submitted":1,"correct":true},{"checked":false,"correct":true,"id":"d1bf026f-554f-4543-bdd2-54dcf105b826"}],"user_    submitted":true,"id":"7a2ed6d3-f492-49b3-b8aa-d080a8aad986","user_result":"missed_some"},{"user_incomplete":false,"user_correct":fal    se,"options":[{"checked":true,"at":"2018-01-23T14:23:30.116Z","id":"a35d0e80-8c49-415d-b8cb-c21a02627e2b","submitted":1},{"checked":    false,"correct":true,"id":"bccd6e2e-2cef-4c72-8bfa-317db0ac48bb"},{"checked":true,"at":"2018-01-23T14:23:41.791   Z","id":"7e0b639a-2ef8-4604-b7eb-5018bd81a91b","submitted":1,"correct":true}],"user_  submitted":true,"id":"bbed4358-999d-4462-9596-bad5173a6ecb","user_result":"incorrect"},{"user_incomplete":false,"user_correct":true,   "options":[{"checked":false,"at":"2018-01-23T14:23:52.510Z","id":"a9333679-de9d-41ff-bb3d-b239d6b95732"},{"checked":false,"id":"8579  5acc-b4b1-4510-bd6e-41648a3553c9"},{"checked":true,"at":"2018-01-23T14:23:54.223Z","id":"c185ecdb-48fb-4edb-ae4e-0204ac7a0909","subm   itted":1,"correct":true},{"checked":true,"at":"2018-01-23T14:23:53.862Z","id":"77a66c83-d001-45cd-9a5a-6bba8eb7389e","submitted":1,"  correct":true}],"user_submitted":true,"id":"e6ad8644-96b1-4617-b37b-a263dded202c","user_result":"correct"},{"user_   incomplete":false,"user_correct":true,"options":[{"checked":false,"id":"59b9fc4b-f239-4850-b1f9-912d1fd3ca13"},{"checked":false,"id"  :"2c29e8e8-d4a8-406e-9cdf-de28ec5890fe"},{"checked":false,"id":"62feee6e-9b76-4123-bd9e-c0b35126b1f1"},{"checked":true,"at":"2018-01   -23T14:24:00.807Z","id":"7f13df9c-fcbe-4424-914f-2206f106765c","submitted":1,"correct":true}],"user_  submitted":true,"id":"95194331-ac43-454e-83de-ea8913067055","user_result":"correct"}],"attempt":1,"id":"5b28a462-7a3b-42e0-b508-09f3   906d1703","counts":{"incomplete":1,"submitted":4,"incorrect":1,"all_correct":false,"correct":2,"total":4,"unanswered":0}},"keen_  created_at":"1516717442.735266","certification":"false","keen_id":"5a6745820eb8ab00016be1f1","exam_name":"Normal Forms and All That    Jazz Master Class"}'

## I am importing the json to retrieve the json string as the JSON.
   import the json

## I am loading the first value into the variable first_message
   first_message=json.loads(messages_as_strings.select('value').take(1)[0].value)

## Printing the first_message
   first_message

   Prints

  {'keen_timestamp': '1516717442.735266', 'max_attempts': '1.0', 'started_at': '2018-01-23T14:23:19.082Z', 'base_exam_id':  '37f0a30a-7464-11e6-aa92-a8667f27e5dc', 'user_exam_id': '6d4089e4-bde5-4a22-b65f-18bce9ab79c8', 'sequences': {'questions': [{'user_  incomplete': True, 'user_correct': False, 'options': [{'checked': True, 'at': '2018-01-23T14:23:24.670Z', 'id':   '49c574b4-5c82-4ffd-9bd1-c3358faf850d', 'submitted': 1, 'correct': True}, {'checked': True, 'at': '2018-01-23T14:23:25.914Z', 'id':   'f2528210-35c3-4320-acf3-9056567ea19f', 'submitted': 1, 'correct': True}, {'checked': False, 'correct': True, 'id':   'd1bf026f-554f-4543-bdd2-54dcf105b826'}], 'user_submitted': True, 'id': '7a2ed6d3-f492-49b3-b8aa-d080a8aad986', 'user_result':  'missed_some'}, {'user_incomplete': False, 'user_correct': False, 'options': [{'checked': True, 'at': '2018-01-23T14:23:30.116Z',  'id': 'a35d0e80-8c49-415d-b8cb-c21a02627e2b', 'submitted': 1}, {'checked': False, 'correct': True, 'id':   'bccd6e2e-2cef-4c72-8bfa-317db0ac48bb'}, {'checked': True, 'at': '2018-01-23T14:23:41.791Z', 'id':  '7e0b639a-2ef8-4604-b7eb-5018bd81a91b', 'submitted': 1, 'correct': True}], 'user_submitted': True, 'id':   'bbed4358-999d-4462-9596-bad5173a6ecb', 'user_result': 'incorrect'}, {'user_incomplete': False, 'user_correct': True, 'options':  [{'checked': False, 'at': '2018-01-23T14:23:52.510Z', 'id': 'a9333679-de9d-41ff-bb3d-b239d6b95732'}, {'checked': False, 'id':  '85795acc-b4b1-4510-bd6e-41648a3553c9'}, {'checked': True, 'at': '2018-01-23T14:23:54.223Z', 'id':   'c185ecdb-48fb-4edb-ae4e-0204ac7a0909', 'submitted': 1, 'correct': True}, {'checked': True, 'at': '2018-01-23T14:23:53.862Z', 'id':   '77a66c83-d001-45cd-9a5a-6bba8eb7389e', 'submitted': 1, 'correct': True}], 'user_submitted': True, 'id':  'e6ad8644-96b1-4617-b37b-a263dded202c', 'user_result': 'correct'}, {'user_incomplete': False, 'user_correct': True, 'options':   [{'checked': False, 'id': '59b9fc4b-f239-4850-b1f9-912d1fd3ca13'}, {'checked': False, 'id':   '2c29e8e8-d4a8-406e-9cdf-de28ec5890fe'}, {'checked': False, 'id': '62feee6e-9b76-4123-bd9e-c0b35126b1f1'}, {'checked': True, 'at':  '2018-01-23T14:24:00.807Z', 'id': '7f13df9c-fcbe-4424-914f-2206f106765c', 'submitted': 1, 'correct': True}], 'user_submitted':   True, 'id': '95194331-ac43-454e-83de-ea8913067055', 'user_result': 'correct'}], 'attempt': 1, 'id':   '5b28a462-7a3b-42e0-b508-09f3906d1703', 'counts': {'incomplete': 1, 'submitted': 4, 'incorrect': 1, 'all_correct': False,   'correct': 2, 'total': 4, 'unanswered': 0}}, 'keen_created_at': '1516717442.735266', 'certification': 'false', 'keen_id':   '5a6745820eb8ab00016be1f1', 'exam_name': 'Normal Forms and All That Jazz Master Class'}

## Printing different fields : submitted field
    print(first_message['sequences']['counts']['submitted'])

    Prints
    4

## Printing different fields : 1 question from sequence key
   print(first_message['sequences']['questions'][0])     

   Prints    
   {'user_incomplete': True, 'user_correct': False, 'options': [{'checked': True, 'at': '2018-01-23T14:23:24.670Z', 'id':   '49c574b4-5c82-4ffd-9bd1-c3358faf850d', 'submitted': 1, 'correct': True}, {'checked': True, 'at': '2018-01-23T14:23:25.914Z', 'id':  'f2528210-35c3-4320-acf3-9056567ea19f', 'submitted': 1, 'correct': True}, {'checked': False, 'correct': True, 'id':   'd1bf026f-554f-4543-bdd2-54dcf105b826'}], 'user_submitted': True, 'id': '7a2ed6d3-f492-49b3-b8aa-d080a8aad986', 'user_result':   'missed_some'}
   
## exit from pyspark
   exit()

##  I am taking down the cluster
    docker-compose down
