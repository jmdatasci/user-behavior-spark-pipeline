#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType


def event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_type: string (nullable = true)
    |---- sword_type: string (nullable = true)
    |---- guild_name: string (nullable = true)
    |-- timestamp: string (nullable = true)
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("sword_type", StringType(), True),
        StructField("guild_name", StringType(), True),
    ])


@udf('integer')
def event_type(event_as_json):
    """filter type of event
    """
    event = json.loads(event_as_json)
    if event['event_type'] == 'purchase_sword':
        return 1
    elif event['event_type'] == 'join_guild':
        return 2
    return 3

def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .enableHiveSupport() \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    sword_purchases = raw_events \
        .filter(event_type(raw_events.value.cast('string')) == 1) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')

#     extracted_sword_purchases = sword_purchases \
#         .rdd \
#         .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw_event))) \
#         .toDF()
#     extracted_sword_purchases.printSchema()
#     extracted_sword_purchases.show()
    
#     extracted_sword_purchases.registerTempTable("extracted_sword_purchases")
    
#     spark.sql("""
#     create external table sword_purchases
#     stored as parquet
#     location '/tmp/sword_purchases'
#     as
#     select * from extracted_sword_purchases
#     """)
    
#     guild_joins = raw_events \
#         .filter(event_type(raw_events.value.cast('string')) == 2) \
#         .select(raw_events.value.cast('string').alias('raw_event'),
#                 raw_events.timestamp.cast('string'),
#                 from_json(raw_events.value.cast('string'),
#                           event_schema()).alias('json')) \
#         .select('raw_event', 'timestamp', 'json.*')
    
#     extracted_guild_joins = guild_joins \
#         .rdd \
#         .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw_event))) \
#         .toDF()
#     extracted_guild_joins.printSchema()
#     extracted_guild_joins.show()
    
#     extracted_guild_joins.registerTempTable("extracted_guild_joins")
    
#     spark.sql("""
#     create external table guild_joins
#     stored as parquet
#     location '/tmp/guild_joins'
#     as
#     select * from extracted_guild_joins
#     """)
    
    default_events = raw_events \
        .filter(event_type(raw_events.value.cast('string')) == 3) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')
    default_events.printSchema()
    default_events.show(100)
    
    sword_sink = sword_purchases \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_sword_purchases") \
        .option("path", "/tmp/sword_purchases") \
        .trigger(processingTime="10 seconds") \
        .start()
    sword_sink.await_termination()
    
    guild_sink = guild_joins \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_guild_joins") \
        .option("path", "/tmp/guild_joins") \
        .trigger(processingTime="10 seconds") \
        .start()
    guild_sink.await_termination()
    
    default_sink = default_events \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_default_events") \
        .option("path", "/tmp/default_events") \
        .trigger(processingTime="10 seconds") \
        .start()
    default_sink.await_termination()
    
if __name__ == "__main__":
    main()
