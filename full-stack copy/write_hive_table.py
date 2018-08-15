#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('boolean')
def is_login(event_as_json):
    event = json.loads(event_as_json)
    event
    if event['event_type'] == 'user_login':
        return True
    return False

@udf('boolean')
def is_logout(event_as_json):
    event = json.loads(event_as_json)
    #event
    if event['event_type'] == 'user_logout':
        return True
    return False  

@udf('boolean')
def is_purchase(event_as_json):
    event = json.loads(event_as_json)
    #event
    if event['event_type'] == 'purchase_sword':
        return True
    return False

@udf('boolean')
def is_user_purchase(event_as_json):
    event = json.loads(event_as_json)
    #event
    if event['event_type'] == 'purchase_sword' and event['logged_in_user'] == 'divya':
        return True
    return False

@udf('boolean')
def is_join_guild(event_as_json):
    event = json.loads(event_as_json)
    #event
    if event['event_type'] == 'join_a_guild':
        return True
    return False

@udf('boolean')
def is_user_join_guild(event_as_json):
    event = json.loads(event_as_json)
    #event
    event['more_purchase_info']
    if event['event_type'] == 'join_a_guild' and event['logged_in_user'] == 'divya' and event['joined_guild'] == 'masadons':
        return True
    return False


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
    
    extracted_login_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/login_events')

    extracted_login_events.registerTempTable("extracted_login_events")

    spark.sql("""
        create external table extracted_logins
        stored as parquet
        location '/tmp/extracted_logins'
        as
        select * from extracted_login_events
        """)   

    # Sword purchase events and write into HDFS
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

    extracted_purchase_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/sword_purchases')


    extracted_purchase_events.registerTempTable("extracted_purchase_events")

    spark.sql("""
        create external table extracted_purchases
        stored as parquet
        location '/tmp/extracted_purchases'
        as
        select * from extracted_purchase_events
        """)


   # join a guild events and write into HDFS
    guild_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_join_guild('raw'))
    guild_events.printSchema()   

    extracted_guild_events = guild_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_guild_events.printSchema()
    extracted_guild_events.show()

    extracted_guild_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/joined_guilds')


    extracted_guild_events.registerTempTable("extracted_guild_events")

    spark.sql("""
        create external table extracted_guilds
        stored as parquet
        location '/tmp/extracted_guilds'
        as
        select * from extracted_guild_events
        """)    
    
    # Extract events for a specific user irrespective of the event type
    user_sword_purchases = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_user_purchase('raw'))
    user_sword_purchases.printSchema()   

    extracted_user_sword_purchases = user_sword_purchases \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_user_sword_purchases.printSchema()
    extracted_user_sword_purchases.show()

    extracted_user_sword_purchases \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/user_sword_purchases')

    extracted_user_sword_purchases.registerTempTable("extracted_user_sword_purchases")

    spark.sql("""
        create external table extracted_user_swords
        stored as parquet
        location '/tmp/extracted_user_swords'
        as
        select * from extracted_user_sword_purchases
        """)
    
    # Extract events for a specific user and specific guild 
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

    extracted_user_guild_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/joined_guilds_user')

    extracted_user_guild_events.registerTempTable("extracted_user_guild_events")

    spark.sql("""
        create external table extracted_user_guilds
        stored as parquet
        location '/tmp/extracted_user_guilds'
        as
        select * from extracted_user_guild_events
        """)   

    # Logout events and write into HDFS
    logout_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_logout('raw'))
    logout_events.printSchema()   

    extracted_logout_events = logout_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_logout_events.printSchema()
    extracted_logout_events.show()    
    
    extracted_logout_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/logout_events')

    extracted_logout_events.registerTempTable("extracted_logout_events")

    spark.sql("""
        create external table extracted_logouts
        stored as parquet
        location '/tmp/extracted_logouts'
        as
        select * from extracted_logout_events
        """)    

if __name__ == "__main__":
    main()
