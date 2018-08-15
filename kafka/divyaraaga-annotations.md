# My annotations, Assignment 6

## Here I'm getting the data.
  514  curl -L -o assessment-attempts-20180128-121051-nested.json https://goo.gl/f5bRm4

## I'm spinning up my kafka, zookeeper, mids cluster.
  503  docker-compose up -d

## I am checking for kafka broker logs
  504  docker-compose logs -f kafka

## I'm am creating the kafka topic here
  506   docker-compose exec kafka assgn6json
       kafka-topics       
       --create       
       --topic assgn6json       
       --partitions 1       
       --replication-factor 1       
       --if-not-exists       
       --zookeeper zookeeper:32181

## I am checking the created topic assgn6json
  507  docker-compose exec kafka   
	  kafka-topics     
	  --describe     
	  --topic assgn6json     
	  --zookeeper zookeeper:32181

## I am publishing 100 Messages to the topic assgn6json

  508  docker-compose exec mids bash -c 
  "cat /w205/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c |
   kafkacat -P -b kafka:29092 -t assgn6json && 
   echo 'Produced 100 messages.'"

## I am starting a consumer to read from the topic assgn6json

  509  docker-compose exec kafka   
  kafka-console-consumer     
  --bootstrap-server kafka:29092     
  --topic assgn6json     
  --from-beginning     
  --max-messages 42

##  I am taking down the cluster
  510  docker-compose down
