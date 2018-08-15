# My annotations, Assignment 8

## Here I'm getting the data.
  560  curl -L -o assessment-attempts-20180128-121051-nested.json https://goo.gl/f5bRm4

## I'm spinning up my zookeeper,kafka,hadoop,spark-python and mids cluster.
  556  docker-compose up -d

## I am checking for kafka broker logs
  560  docker-compose logs -f kafka

## I am checking at tmp/ directory in hdfs and see that what we want to write isn't there already
  561  docker-compose exec cloudera hadoop fs -ls /tmp/

## I'm am creating the kafka topic here
  562   docker-compose exec kafka \
        kafka-topics \
          --create \
          --topic exams \
          --partitions 1 \
          --replication-factor 1 \
          --if-not-exists \
          --zookeeper zookeeper:32181
      

## I am publishing test Messages to the topic exams from the downloaded json using kafkacat
   
  564  docker-compose exec mids bash -c "cat /w205/spark-with-kafka-and-hdfs/assessment-attempts-20180128-121051-nested.json \
    | jq '.[]' -c \
    | kafkacat -P -b kafka:29092 -t exams"

## I am spinning up a pyspark process using the spark container
  571  docker-compose exec spark pyspark

##  Once I spun up the pyspark process, history didnt capture the commands. 

##  I am reading messages from kafka in pyspark 

   raw_exams= spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe","exams") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load() 

## I am caching raw_exams to cut back on warnings later
   raw_exams.cache()

## I am printing the schema of raw_exams
    raw_exams.printSchema()    

## I am caching raw_exams to cut back on warnings later
   raw_exams.cache()    

## Command to see the messages
   raw_exams.show()    

## I am casting messages as strings
   exams = raw_exams.select(raw_exams.value.cast('string'))

## I am writing exams to HDFS
   exams.write.parquet("/tmp/exams")

## I am importing json for df
   import json  

## I am going to extract the json fields , I'm going to get rdd (spark's distributed dataset), apply a map to it to work as a df
   extracted_exams = exams.rdd.map(lambda x: json.loads(x.value)).toDF()

## Json is deprecated, The current recommended approach to this is to explicitly create our Row objects from the json fields
   from pyspark.sql import Row
   extracted_exams = exams.rdd.map(lambda x: Row(**json.loads(x.value))).toDF()

## Command to see the extracted df messages
   extracted_exams.show()  

## I am printing the schema of messages to identify the fields and nested structure of messages
   extracted_exams.printSchema()

## I am creating a Spark "TempTable" (aka "View")
   extracted_exams.registerTempTable('exams')

## I am creating a Spark "TempTable" (aka "View")
   extracted_exams.registerTempTable('exams')

## I am using SparkSQL to pick and choose the fields to promote to columns.
## I am creating DataFrames from queries
   spark.sql("select exam_name,user_exam_id,max_attempts, sequences.questions[0].user_correct from exams limit 10").show()
   spark.sql("select exam_name,user_exam_id,max_attempts, sequences.questions from exams limit 10").show()

## I am saving the query or df i want to save in HDFS
   some_exam_info  = spark.sql("select exam_name,user_exam_id,max_attempts, sequences.questions from exams limit 10")

## exit from pyspark
   exit()

## I am checking at tmp/ directory in hdfs and see commits and some_commit_info saved
   572  docker-compose exec cloudera hadoop fs -ls /tmp/   

##  I am taking down the cluster
    docker-compose down
