#!/usr/bin/env python
"""Extract projectEvents from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf

# You have a ton of columns and each one should be an argument to Row
# Use a dictionary comprehension to make this easier
def record_to_row(record):
    schema = {'column{i:d}'.format(i = col_idx):record[col_idx] for col_idx in range(1,100+1)}
    return Row(**schema)


@udf('string')
def munge_event(event_as_json):
    event = json.loads(event_as_json)
    event['Host'] = "moe"
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
        .option("subscribe", "curlProjectEvents") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    munged_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_events = munged_events \
        .rdd \
        .map(lambda x: Row(timestamp=x.timestamp, **json.loads(x.raw))) \
        .toDF()
    extracted_events.show()    
    
    # Login events
    login_events = extracted_events \
        .filter(extracted_events.event_type == 'user_login')
    login_events.show()
    # login_events \
    #     .write \
    #     .mode("overwrite") \
    #     .parquet("/tmp/login_events")
    

    sword_purchases = extracted_events \
        .filter(extracted_events.event_type == 'purchase_sword')
    sword_purchases.show()
    # sword_purchases \
    #     .write \
    #     .mode("overwrite") \
    #     .parquet("/tmp/sword_purchases")


    # Joined the guild events
    joined_guild = extracted_events \
        .filter(extracted_events.event_type == 'join_a_guild')
    joined_guild.show()
    # joined_guild \
    #     .write \
    #     .mode("overwrite") \
    #     .parquet("/tmp/joined_guilds")


    # Extract events for a specific user irrespective of the event type
    user_sword_purchases = extracted_events \
        .filter(extracted_events.logged_in_user == 'divya')
    user_sword_purchases.show()
    # user_sword_purchases \
    #     .write \
    #     .mode("overwrite") \
    #     .parquet("/tmp/sword_purchases_hits_divya")     
    
    # Extract events for a specific guild irrespective of the event type
    joined_guild_user = extracted_events \
        .filter(extracted_events.joined_guild == 'masadons')
    joined_guild_user.show()
    # joined_guild_user \
    #     .write \
    #     .mode("overwrite") \
    #     .parquet("/tmp/joined_guilds_user") 

    # Logout events
    logout_events = extracted_events \
        .filter(extracted_events.event_type == 'user_logout')
    logout_events.show()
    # logout_events \
    #     .write \
    #     .mode("overwrite") \
    #     .parquet("/tmp/logout_events")

if __name__ == "__main__":
    main()
