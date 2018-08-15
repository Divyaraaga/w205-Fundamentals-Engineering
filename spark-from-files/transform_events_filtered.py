
#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('string')
def munge_event(event_as_json):
    event = json.loads(event_as_json)
    event['Host'] = "divya" # silly change to show it works
    event['Cache-Control'] = "no-cache"
    return json.dumps(event)


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "filesEventsFilter") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    munged_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'), \
                raw_events.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_events = munged_events.rdd.map(lambda r: json.loads(r.munged)).toDF()
    extracted_events.printSchema()
    extracted_events.show()  

    default_hits = extracted_events \
        .filter(extracted_events.event_type == 'default')
    default_hits.show()
    default_hits \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/default_hits")

    sword_purchases = extracted_events \
        .filter(extracted_events.event_type == 'purchase_sword')
    sword_purchases.show()
    sword_purchases \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/sword_purchases_hits")  


    joined_guild = extracted_events \
        .filter(extracted_events.event_type == 'join_a_guild')
    joined_guild.show()
    joined_guild \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/joined_guilds")


    # Extract a events for a specific user irrespective of the event type
    user_sword_purchases = extracted_events \
        .filter(extracted_events.purchase_user == 'divya')
    user_sword_purchases.show()

    user_sword_purchases \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/sword_purchases_hits_divya")     
    
    # Extract a events for a specific guild irrespective of the event type
    joined_guild_user = extracted_events \
        .filter(extracted_events.joined_guild == 'masadons')
    joined_guild_user.show()

    joined_guild_user \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/joined_guilds_user") 


if __name__ == "__main__":
    main()