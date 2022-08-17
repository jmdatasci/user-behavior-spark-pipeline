# Understanding User Behavior
## Scenario
- You're a data scientist at a game development company  

- Your latest mobile game has two events you're interested in tracking: `buy a
  sword` & `join guild`

- Each has metadata characterstic of such events (i.e., sword type, guild name,
  etc)


## Tasks

- Instrument your API server to log events to Kafka

- Assemble a data pipeline to catch these events: use Spark streaming to filter
  select event types from Kafka, land them into HDFS/parquet to make them
  available for analysis using Presto. 

- Use Apache Bench to generate test data for your pipeline.

- Produce an analytics report where you provide a description of your pipeline
  and some basic analysis of the events. Explaining the pipeline is key for this project!

- Submit your work as a git PR as usual. AFTER you have received feedback you have to merge 
  the branch yourself and answer to the feedback in a comment. Your grade will not be 
  complete unless this is done!

Use a notebook to present your queries and findings. Remember that this
notebook should be appropriate for presentation to someone else in your
business who needs to act on your recommendations. 

It's understood that events in this pipeline are _generated_ events which make
them hard to connect to _actual_ business decisions.  However, we'd like
students to demonstrate an ability to plumb this pipeline end-to-end, which
includes initially generating test data as well as submitting a notebook-based
report of at least simple event analytics. That said the analytics will only be a small
part of the notebook. The whole report is the presentation and explanation of your pipeline 
plus the analysis!

## Prerequisites  
All code in the project has been tested and generated in Google Cloud Platform (GCP). This has not been validated on any external Cloud Compute services.  
  
All following code is expected to be executed in the directory:

```bash
~/w205/project-3-jmdatasci
```

## Building the Data Pipeline

The first part of building a data pipeline is to consider the data's use case at the end of the day. Based on this project the intention is to have data get generated via player events, stored into HDFS and then queryable by the data scientists using Presto.  

Working backwards we can determine what we need to make sure we can meet these requirements. 

1. First have a **Presto** instance running to query the data.  

2. Next we need a **Hadoop (Cloudera)**  instance running to store the data in HDFS.  

3. Next we need to create a **Spark** instance to filter, transform and land the data to **Hadoop** and generate Hive tables for **Presto**.  

4. The next step we need is to get the data to **Spark** from the event listener.  
5. Next we will use a **Kafka** instance to act as a broker on the data, and In this context **Spark** is the **Kafka consumer** and we still need to create the **Kafka** in order to receive the data from the **Kafka Producer**.  
  
6. Finally to build the **Kafka producer** and event listener we use a flask app to collect and generate the data that will go into **Kafka**. 

7. To improve the pipeline we have also added a **Redis** instance which can act as a messaging broker where events can be logged from **Kafka** and send notifications as desired.

---
With all of this in mind. We can create a `docker-compose.yml` file in order to spin up all of these required instances at once. The `docker-compose.yml` is present in this directory and the contents are as follows, broken up to add explanations. Using this file we end up with 6 different docker containers that get spun up all for different purposes.  


```yml
  ---
version: '2'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 32181
      ZOOKEEPER_TICK_TIME: 2000
    expose:
      - "2181"
      - "2888"
      - "32181"
      - "3888"
    extra_hosts:
      - "moby:127.0.0.1"
```

1. `zookeeper` - This function coexists with kafka directs the data as needed to different events.  

```yml
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:32181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    expose:
      - "9092"
      - "29092"
    extra_hosts:
      - "moby:127.0.0.1"
```
2. `kafka` - Kafka acts as the kafka broker that will take the data from flask and pass it through to whatever consumer wants it (Spark in this case)  


```yml
  cloudera:
    image: midsw205/hadoop:0.0.2
    hostname: cloudera
    expose:
      - "8020" # nn
      - "8888" # hue
      - "9083" # hive thrift
      - "10000" # hive jdbc
      - "50070" # nn http
    ports:
      - "8888:8888"
    extra_hosts:
      - "moby:127.0.0.1"
```
3. `cloudera` - This container hosts Hadoop including HDFS and Hive functionality  

```yml
  spark:
    image: midsw205/spark-python:0.0.6
    stdin_open: true
    tty: true
    volumes:
      - ~/w205:/w205
    expose:
      - "8888"
    #ports:
    #  - "8888:8888"
    depends_on:
      - cloudera
    environment:
      HADOOP_NAMENODE: cloudera
      HIVE_THRIFTSERVER: cloudera:9083
    extra_hosts:
      - "moby:127.0.0.1"
    command: bash
```
4. `spark` - This container holds spark which runs our data stream processing and data land into HDFS  


```yml
  presto:
    image: midsw205/presto:0.0.1
    hostname: presto
    volumes:
      - ~/w205:/w205
    expose:
      - "8080"
    environment:
      HIVE_THRIFTSERVER: cloudera:9083
    extra_hosts:
      - "moby:127.0.0.1"
```
5. `presto` - This container is used to query the data directly from HDFS based Hive tables and can execute standard `SQL` queries.  

```yml
  mids:
    image: midsw205/base:0.1.9
    stdin_open: true
    tty: true
    volumes:
      - ~/w205:/w205
    expose:
      - "5000"
    ports:
      - "5000:5000"
    extra_hosts:
      - "moby:127.0.0.1"
```
6. `mids` - This container holds additional functionality that enables integration and some functions such as `curl` and `ab` which are used for generating test data and most importantly Flask.

```yml 
  redis:
    image: redis:latest
    expose:
      - "6379"
    ports:
      - "6379:6379"
```

7. `redis` - This contianer creates the in-memory cache to send out push notifications, view/update entries or add decorations that removes burden from Spark from doing so.  

Once this `docker-compose.yml` file is complete, we first navigate to the project working directory by using the command in a new `Terminal` instance.

```bash
cd w205/project-2-jmdatasci
```

Once we are in the directory we can spin up the containers. We run the code:

```bash
docker-compose up -d
```
This starts up the containers in a detached mode and gives control back of the Terminal. When this code is executed, docker-compose checks the versions of these docker images and if not installed, they are downloaded and then started up.  

Once the containers are up, we can begin to tell them what to do.  

---
First we can start with kafka. We invoke this process by running this code in terminal. 
```bash
docker-compose exec kafka kafka-topics --create --topic events --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:29092
```  
This code invokes the kafka container to create a new topic called events. It also specifies that we only want a single copy of each queue and a single partition rather than the default redundancy that is expected in a production environment. This also opens port 29092 on the `localhost` ip to be able to send data from Flask to kafka
  
When executed properly you will be greeted with the confirmation message:  
```bash
Created topic events.
```
  
Another nice feature of kafka is that we can use kafkacat from our mids container as a consumer to view the data that is entered in real time. To view the data in a new terminal we use the command:  

```bash
docker-compose exec mids  kafkacat -C -b kafka:29092 -t events -o beginning
```  
This subscribes us to the events topic and starts from the beginning with no ending offset allowing it to remain open to view all data passing through.

---

Now that Kafka is up to received our data we can start our Flask app which will receive the HTTP requests and direct them to Kafka in a json format. The Flask app that we will use is in this directory entitled `game_api.py` and the contents are as follows, broken up into chunks for added explanations.

What this app does is it listens for any HTTP requests on `localhost` (on port 5000 as this is opened on mids container). Once a request is received via a `curl` or `ab` command, the app does some simple logic to extract data of the request and determine what success message should be output and what data to pass along to kafka.  

```python
#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())
```


The available commands are `/join_guild` and `/purchase_a_sword` or a default command at `/` which is a sinkhole for anything beyond these two.. These two functions tracking a user's purchase of a sword or joining of a guild by default with a `GET` request. When a user submits a `DELETE` request the sword is destroyed or the guild is left.  

```python
@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"

@app.route("/purchase_a_sword", methods=["GET"])
def purchase_a_sword():
    event_detail = request.args.get('event_detail', default='wood')  
    sword_event = {'event_type' : 'sword_event', 
                    'direction' : 'increase',
                    'event_detail' : event_detail}
    log_to_kafka('events', sword_event)
    return event_detail.title() + " Sword Purchased!\n"

@app.route("/purchase_a_sword", methods=["DELETE"])
def destroy_sword():
    event_detail = request.args.get('event_detail', default='wood')  
    sword_event = {'event_type' : 'sword_event', 
                    'direction' : 'reduce',
                    'event_detail' : event_detail}
    log_to_kafka('events', sword_event)
    return event_detail.title() + " Sword Destroyed!\n"

@app.route("/join_guild", methods=["GET"])
def join_guild():
    event_detail = request.args.get('event_detail', default='starter guild')
    guild_event = {'event_type' : 'guild_event',
                    'direction' : 'increase',
                    'event_detail' : event_detail}
    log_to_kafka('events', guild_event)
    return "Joined Guild " + event_detail.title() + "\n"

@app.route("/join_guild", methods=["DELETE"])
def leave_guild():
    event_detail = request.args.get('event_detail', default='starter guild')
    guild_event = {'event_type' : 'guild_event',
                    'direction' : 'reduce',
                    'event_detail' : event_detail}
    log_to_kafka('events', guild_event)
    return "Left Guild " + event_detail.title() + "\n"
    
```

Through additional functionality with the `requests` package a user can also specify an `event_detail`. This may be something such as the type of sword or the color of sword. By default, the sword is a wood sword. If `event_detail` is used for joining a guild, it represents the name of the guild joined or left. With more time, a robust validation system could be developed in order to track how many swords are available, how much currency a user has, what guild the user is currently in and restrictions may be added. There could also be additional attributes such as sword attributes and tooltips/style text that is visible within game or guild colors and guildmates but as this is only a proof of concept we have kept it simple with a single event_detail argument.  
  
This Flask app can now be started with the `mids` container via the command:  
```bash
docker-compose exec mids env FLASK_APP=/w205/project-3-jmdatasci/game_api.py flask run --host 0.0.0.0
```

This simple line runs the Flask app at location `/w205/project-3-jmdatasci/game_api.py` in the `mids` container on `0.0.0.0` which is how we can access at `localhost`. We access via port `5000` as this is the port we have opened why spinning up `mids`.  

Once flask is started you are given the success message:  
```bash
* Serving Flask app "game_api"
* Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
```  

---  
Now that Kafka and Flask are up and running we can move over to the other side of the pipeline and look at our spark container. For this project we have generated a Spark streaming file that repeats every 10 seconds enabling data in HDFS to be refreshed quickly.  
  
The Spark streaming file used for this project is entitled `rpg_spark_stream.py` within this same directory. The contents of this file are below broken up in chunks to explain the functionality.  
  
```python
#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType
```
Flag file to be run with pyton and import all necessary packages  
```python
def event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- direction: string (nullable = true)
    |-- event_detail: string (nullable = true)
    |-- timestamp: string (nullable = true)
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("direction", StringType(), True),
        StructField("event_detail", StringType(), True),
    ])
```
Explicitly define the schema for our data that is to be used for this process.  
```python
@udf('boolean')
def is_valid_event(event_as_json):
    """udf for filtering events
    """
    event = json.loads(event_as_json)
    if (event['event_type'] == 'sword_event' or event['event_type'] == 'guild_event'):
        return True
    return False
```
User defined function that is used to filter if events are valid or not. Typically there would be more robust data validation but this is sufficient for the proof of concept.  
```python
def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .enableHiveSupport() \
        .getOrCreate()
```
The first part of the `main()` function creates a new `SparkSession`, creates the job and enables Hive integration.  

```python
    raw_events = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .load()
```

The next portion of `main()` reads the stream from the kafka queue via port 29092 and subscribes only to the topic `events`. The nice thing about using kafka is that there may be multiple topics and you can specify which data is useful in which context rather than needing to filter everything out.  
```python
    valid_events = raw_events \
        .filter(is_valid_event(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')
```

This next portion of `main()` takes the raw data that was ingested from kafka, recasts the data as string format, filters out only valid events as speficied by our user defined function above. 

```python
    spark.sql("drop table if exists valid_events")
    sql_string = """
        create external table if not exists valid_events (
            raw_event string,
            timestamp string,
            Accept string,
            Host string,
            `User-Agent` string,
            event_type string,
            direction string,
            event_detail string
            )
            stored as parquet
            location '/tmp/valid_events'
            tblproperties ("parquet.compress"="SNAPPY")
            """
    spark.sql(sql_string)
 ```
This portion of code generates the Hive table. First, using `spark.sql` the program attempts to drop the table of valid_events if it exists. Next it generates the table with the specified schema and lands the table in HDFS at `/tmp/valid_events` as a parquet file. This would be the HDFS location in a production environment. This method of storing to hive while streaming was adapted from Doris Schioberg PhD's course notes.  
  
 ```python
    event_sink = valid_events \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoint_events") \
        .option("path", "/tmp/valid_events") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    event_sink.awaitTermination()
```
The final section of `main()` writes the data stream to our Hive table's location at `/tmp/valid_events` in HDFS every 10 seconds and keeps the same parquet formatting. There is also a checkpoint for data resiliency. The last two lines here are to start this event sink and finally to await for a termination command. 

```python
if __name__ == "__main__":
    main()
```

The last snippet of this file is just the execution of the `main()` file described above.  
  
Now that our spark streaming job is created, we just need to execute it in our spark container with the command:  
  
```
docker-compose exec spark spark-submit /w205/project-3-jmdatasci/rpg_spark_stream.py
```
  
  This command generates a large output and leaves the terminal running. Output is shown in the Appendix.  
  
---  
With spark now running in the background, we are now able to generate our data. There are a few ways to do this, the simplest being through a `curl` command. This can be done in another terminal window as shown below:  

```

docker-compose exec mids curl http://localhost:5000/purchase_a_sword
```
This command greets you with the default success message we created in Flask of 
```bash
Wood Sword Purchased!
```

When we submit this we can begin to see the data flow through our pipeline in different locations:  
```bash
Kafka: 
{"direction": "increase", "event_type": "sword_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "wood", "Host": "localhost:5000"}

Flask:
127.0.0.1 - - [03/Dec/2021 20:45:42] "GET /purchase_a_sword HTTP/1.1" 200 -

Spark: 
21/12/03 20:45:51 INFO StreamExecution: Streaming query made progress: {
  "id" : "9c84798c-83aa-42fe-a0f4-657fef3acc5c",
  "runId" : "4ae4f7cd-22e6-4b51-8526-75b5caa659ed",
  "name" : null,
  "timestamp" : "2021-12-03T20:45:50.000Z",
  "numInputRows" : 1,
  "inputRowsPerSecond" : 0.1,
  "processedRowsPerSecond" : 0.9478672985781991,
  "durationMs" : {
    "addBatch" : 885,
    "getBatch" : 13,
    "getOffset" : 2,
    "queryPlanning" : 22,
    "triggerExecution" : 1055,
    "walCommit" : 120
  },
  "stateOperators" : [ ],
  "sources" : [ {
    "description" : "KafkaSource[Subscribe[events]]",
    "startOffset" : {
      "events" : {
        "0" : 0
      }
    },
    "endOffset" : {
      "events" : {
        "0" : 1
      }
    },
    "numInputRows" : 1,
    "inputRowsPerSecond" : 0.1,
    "processedRowsPerSecond" : 0.9478672985781991
  } ],
  "sink" : {
    "description" : "FileSink[/tmp/valid_events]"
  }
}
```
This is a great sign that our data is going through. Finally we can check that the data is landed in cloudera HDFS location with the command:  
```bash
docker-compose exec cloudera hadoop fs -ls -h /tmp/valid_events
```

The output of this command shows us that events have been logged to HDFS  
```bash
drwxr-xr-x   - root supergroup          0 2021-12-03 20:45 /tmp/valid_events/_spark_metadata
-rw-r--r--   1 root supergroup        864 2021-12-03 20:37 /tmp/valid_events/part-00000-6d948a0a-3b41-4daa-9591-2002944793d2-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.6 K 2021-12-03 20:45 /tmp/valid_events/part-00000-cf867acf-5f26-46f3-9e70-f42f4059f11c-c000.snappy.parquet
```

We are all good! Now that the pipeline is all connected we can go ahead and do some more testing.  
  
We can try using our http `DELETE` commands to destroy the sword we just purchased  
```bash
docker-compose exec mids curl -X DELETE http://localhost:5000/purchase_a_sword
```
And we are met with the expected output:
```bash
Wood Sword Destroyed!
```

We can buy a few swords of different types (output and commands shown together.)  

```bash
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl http://localhost:5000/purchase_a_sword?event_detail=long
Long Sword Purchased!
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl http://localhost:5000/purchase_a_sword?event_detail=short
Short Sword Purchased!
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl http://localhost:5000/purchase_a_sword?event_detail=two-handed
Two-Handed Sword Purchased!
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl http://localhost:5000/purchase_a_sword?event_detail=bronze
Bronze Sword Purchased!
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl http://localhost:5000/purchase_a_sword?event_detail=bronze
Bronze Sword Purchased!
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl http://localhost:5000/purchase_a_sword?event_detail=long
Long Sword Purchased!
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl http://localhost:5000/purchase_a_sword?event_detail=glass
Glass Sword Purchased!
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl -X DELETE http://localhost:5000/purchase_a_sword?event_detail=glass
Glass Sword Destroyed!
```
With our sword commands working well we can also do the same sort of tests on the guild events.  

```bash
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl http://localhost:5000/join_guild
Joined Guild Starter Guild
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl http://localhost:5000/join_guild?event_detail=PVP-Friends
Joined Guild Pvp-Friends
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl http://localhost:5000/join_guild?event_detail=Data-Engineers
Joined Guild Data-Engineers
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids curl -X DELETE http://localhost:5000/join_guild?event_detail=Data-Engineers
Left Guild Data-Engineers
```
Great.  

We can also see this output still flowing through the pipeline (spark not shown):  
```bash
Kafka:
{"direction": "increase", "event_type": "guild_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "PVP-Friends", "Host": "localhost:5000"}
{"direction": "increase", "event_type": "guild_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "Data-Engineers", "Host": "localhost:5000"}
{"direction": "reduce", "event_type": "guild_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "Data-Engineers", "Host": "localhost:5000"}  

Flask: 
127.0.0.1 - - [03/Dec/2021 20:58:46] "GET /join_guild HTTP/1.1" 200 -
127.0.0.1 - - [03/Dec/2021 20:59:02] "GET /join_guild?event_detail=PVP-Friends HTTP/1.1" 200 -
127.0.0.1 - - [03/Dec/2021 20:59:19] "GET /join_guild?event_detail=Data-Engineers HTTP/1.1" 200 -
127.0.0.1 - - [03/Dec/2021 20:59:38] "DELETE /join_guild?event_detail=Data-Engineers HTTP/1.1" 200 -
```
---

With everything flowing well here and some test data available we can spin up the last part of our data pipeline which is Presto. This is where we take off our Data Engineer Hat and put the Data Scientist hat on with the assumption that we have no knowledge of the pipeline before this point.  
  
To start presto we open yet another terminal, change to our working director and execute the command:  

```bash
docker-compose exec presto presto --server presto:8080 --catalog hive --schema default
```
This prompts us with a presto interactive prompt:  
```sql
presto:default> 
```

With Presto up and running we can execute some basic SQL queries to demonstrate a Data Scientist's role. First locate the table:

```sql
presto:default> show tables;
    Table     
--------------
 valid_events 
(1 row)

Query 20211203_211754_00006_5ipsi, FINISHED, 1 node
Splits: 2 total, 1 done (50.00%)
0:00 [1 rows, 37B] [6 rows/s, 249B/s]

```
Review our schema:

```sql
presto:default> describe valid_events;
    Column    |  Type   | Comment 
--------------+---------+---------
 raw_event    | varchar |         
 timestamp    | varchar |         
 accept       | varchar |         
 host         | varchar |         
 user-agent   | varchar |         
 event_type   | varchar |         
 direction    | varchar |         
 event_detail | varchar |         
(8 rows)

Query 20211205_165031_00072_5ipsi, FINISHED, 1 node
Splits: 2 total, 1 done (50.00%)
0:00 [8 rows, 569B] [23 rows/s, 1.66KB/s]
```

Count the rows:  
```sql
presto:default> select count(*) as num_entries from valid_events;
 num_entries 
-------------
          15 
(1 row)

Query 20211203_211843_00009_5ipsi, FINISHED, 1 node
Splits: 15 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

View the first 10 entries:

```sql
presto:default> select * from valid_events limit 10;
                                                                            raw_event                        
-------------------------------------------------------------------------------------------------------------
 {"direction": "increase", "event_type": "sword_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event
----------------------------------------------------------------------------------------------------------------------------------------
 {"direction": "increase", "event_type": "sword_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "wood", "Host": "l
 {"direction": "increase", "event_type": "sword_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "bronze", "Host": 
 {"direction": "increase", "event_type": "sword_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "long", "Host": "l
 {"direction": "increase", "event_type": "sword_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "short", "Host": "
 {"direction": "increase", "event_type": "sword_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "two-handed", "Hos
 {"direction": "reduce", "event_type": "sword_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "wood", "Host": "loc
 {"direction": "increase", "event_type": "guild_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "starter guild", "
 {"direction": "increase", "event_type": "guild_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "Data-Engineers", 
 {"direction": "increase", "event_type": "sword_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "glass", "Host": "
 {"direction": "reduce", "event_type": "guild_event", "Accept": "*/*", "User-Agent": "curl/7.47.0", "event_detail": "Data-Engineers", "H
(10 rows)
```

Now for some fancier queries:  

```sql
presto:default> select direction, count(*) as num_events from valid_events group by direction;
 direction | num_events 
-----------+------------
 increase  |      11724 
 reduce    |          3 
(2 rows)

Query 20211205_165504_00084_5ipsi, FINISHED, 1 node
Splits: 29 total, 15 done (51.72%)
0:01 [9.46K rows, 89.2KB] [18.4K rows/s, 174KB/s]


```

All looks good in Presto. From here onwards I will just be showing the count to demonstrate the pipeline is working in a streaming state.  

---  
Now that our pipeline is all built and tested we can run some heavier tests using apache bench.  

```bash
docker-compose exec mids  ab -n 100  -H "Host: Player 1"  http://localhost:5000/purchase_a_sword?event_detail=test_sword_1
```


We can see the command processed: 
```bash
(base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec mids  ab -n 100  -H "Host: Player 1"  http://localhost:5000/purchase_a_sword?event_detail=test_sword_1
This is ApacheBench, Version 2.3 <$Revision: 1706008 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking localhost (be patient).....done


Server Software:        Werkzeug/0.14.1
Server Hostname:        localhost
Server Port:            5000

Document Path:          /purchase_a_sword?event_detail=test_sword_1
Document Length:        30 bytes

Concurrency Level:      1
Time taken for tests:   0.451 seconds
Complete requests:      100
Failed requests:        0
Total transferred:      18500 bytes
HTML transferred:       3000 bytes
Requests per second:    221.80 [#/sec] (mean)
Time per request:       4.509 [ms] (mean)
Time per request:       4.509 [ms] (mean, across all concurrent requests)
Transfer rate:          40.07 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.1      0       1
Processing:     2    4   1.5      4       9
Waiting:        1    3   1.4      3       8
Total:          2    4   1.5      4       9

Percentage of the requests served within a certain time (ms)
  50%      4
  66%      5
  75%      5
  80%      5
  90%      6
  95%      8
  98%      9
  99%      9
 100%      9 (longest request)
 ```
 
Now that we see the data has come through we can add much more and see the counts increasing in presto.  

```bash
docker-compose exec mids  ab -n 100  -H "Host: Player 2"  http://localhost:5000/purchase_a_sword?event_detail=test_sword_2

docker-compose exec mids  ab -n 100  -H "Host: Player 3"  http://localhost:5000/purchase_a_sword?event_detail=test_sword_3

docker-compose exec mids  ab -n 100  -H "Host: Player 3"  http://localhost:5000/join_guild?event_detail=W205-Test

docker-compose exec mids  ab -n 100  -H "Host: Player 2"  http://localhost:5000/join_guild?event_detail=Office-Hours
```

```sql
resto:default> select count(*) as num_events from valid_events;
 num_events 
------------
        615 
(1 row)

Query 20211205_014306_00003_5ipsi, FINISHED, 1 node
Splits: 21 total, 17 done (80.95%)
0:00 [412 rows, 46KB] [959 rows/s, 107KB/s]
```

Now to add a few more and make sure this is ready to submit:  

```bash
~/w205/project-3-jmdatasci$ docker-compose exec mids  ab -n 11111  -H "Host: Jordan Meyer"  http://localhost:5000/join_guild?event_detail=Ready_to_submit

```

Immediately hopping over to the presto terminal we see that the total of 615 entries increased to 11726 as expected.  

```sql
presto:default> select count(*) as num_events from valid_events;
 num_events 
------------
      11726 
(1 row)

Query 20211205_014644_00004_5ipsi, FINISHED, 1 node
Splits: 27 total, 13 done (48.15%)
0:01 [4.66K rows, 64.6KB] [7.7K rows/s, 107KB/s]
```

Now we can see how many events and types people did. We can use presto to garner whatever detail we need about this.  

```sql
presto:default> select host,event_type,count(*) as num_events from valid_events group by host,event_type order by event_type desc;
      host      | event_type  | num_events 
----------------+-------------+------------
 localhost:5000 | sword_event |         12 
 Player 3       | sword_event |        100 
 Player 1       | sword_event |        100 
 Player 2       | sword_event |        200 
 Jordan Meyer   | guild_event |      11111 
 Player 3       | guild_event |        100 
 localhost:5000 | guild_event |          4 
 Player 2       | guild_event |        100 
(8 rows)

Query 20211205_165709_00086_5ipsi, FINISHED, 1 node
Splits: 29 total, 28 done (96.55%)
0:01 [11.7K rows, 131KB] [19K rows/s, 212KB/s]

presto:default> select host, event_type, event_detail from valid_events group by host,event_type, event_detail order by event_type, event_detail  desc;
      host      | event_type  |  event_detail   
----------------+-------------+-----------------
 localhost:5000 | guild_event | starter guild   
 Player 3       | guild_event | W205-Test       
 Jordan Meyer   | guild_event | Ready_to_submit 
 localhost:5000 | guild_event | PVP-Friends     
 Player 2       | guild_event | Office-Hours    
 localhost:5000 | guild_event | Data-Engineers  
 localhost:5000 | sword_event | wood            
 localhost:5000 | sword_event | two-handed      
 Player 3       | sword_event | test_sword_3    
 Player 2       | sword_event | test_sword_2    
 Player 1       | sword_event | test_sword_1    
 localhost:5000 | sword_event | short           
 localhost:5000 | sword_event | long            
 localhost:5000 | sword_event | glass           
 localhost:5000 | sword_event | bronze          
(15 rows)

Query 20211205_165841_00088_5ipsi, FINISHED, 1 node
Splits: 29 total, 17 done (58.62%)
0:01 [7.24K rows, 83.6KB] [11.7K rows/s, 135KB/s]

```

At this point we are ready and have a working pipeline that we can roll out for the data scientist team. Using SQL commands in presto the team can export the data or use another hive connector to analyze the data. If we had other tables available we could also do joins if we were to link tables of shops, players and events etc. 


-------

## Appendix 
---
### Exhibit 1 Spark Stream Execution Output

```
  (base) jupyter@jordan-205:~/w205/project-3-jmdatasci$ docker-compose exec spark spark-submit /w205/project-3-jmdatasci/rpg_spark_stream.py
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
21/12/03 20:36:53 INFO SparkContext: Running Spark version 2.2.0
21/12/03 20:36:54 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/12/03 20:36:54 INFO SparkContext: Submitted application: ExtractEventsJob
21/12/03 20:36:54 INFO SecurityManager: Changing view acls to: root
21/12/03 20:36:54 INFO SecurityManager: Changing modify acls to: root
21/12/03 20:36:54 INFO SecurityManager: Changing view acls groups to: 
21/12/03 20:36:54 INFO SecurityManager: Changing modify acls groups to: 
21/12/03 20:36:54 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
21/12/03 20:36:54 INFO Utils: Successfully started service 'sparkDriver' on port 40339.
21/12/03 20:36:54 INFO SparkEnv: Registering MapOutputTracker
21/12/03 20:36:54 INFO SparkEnv: Registering BlockManagerMaster
21/12/03 20:36:54 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
21/12/03 20:36:54 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
21/12/03 20:36:54 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-49154ec1-c450-44e5-92b3-a5cd60ed4e2d
21/12/03 20:36:54 INFO MemoryStore: MemoryStore started with capacity 366.3 MB
21/12/03 20:36:55 INFO SparkEnv: Registering OutputCommitCoordinator
21/12/03 20:36:55 INFO Utils: Successfully started service 'SparkUI' on port 4040.
21/12/03 20:36:55 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://172.24.0.6:4040
21/12/03 20:36:55 INFO SparkContext: Added file file:/w205/project-3-jmdatasci/rpg_spark_stream.py at file:/w205/project-3-jmdatasci/rpg_spark_stream.py with timestamp 1638563815419
21/12/03 20:36:55 INFO Utils: Copying /w205/project-3-jmdatasci/rpg_spark_stream.py to /tmp/spark-0d7352fe-bcdc-4821-a934-ed7b047e5c62/userFiles-59e91c11-0eaa-41d2-b409-2c34ba03ab75/rpg_spark_stream.py
21/12/03 20:36:55 INFO Executor: Starting executor ID driver on host localhost
21/12/03 20:36:55 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 45481.
21/12/03 20:36:55 INFO NettyBlockTransferService: Server created on 172.24.0.6:45481
21/12/03 20:36:55 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
21/12/03 20:36:55 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.24.0.6, 45481, None)
21/12/03 20:36:55 INFO BlockManagerMasterEndpoint: Registering block manager 172.24.0.6:45481 with 366.3 MB RAM, BlockManagerId(driver, 172.24.0.6, 45481, None)
21/12/03 20:36:55 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.24.0.6, 45481, None)
21/12/03 20:36:55 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.24.0.6, 45481, None)
21/12/03 20:36:56 INFO SharedState: loading hive config file: file:/spark-2.2.0-bin-hadoop2.6/conf/hive-site.xml
21/12/03 20:36:56 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse').
21/12/03 20:36:56 INFO SharedState: Warehouse path is 'file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse'.
21/12/03 20:36:56 INFO HiveUtils: Initializing HiveMetastoreConnection version 1.2.1 using Spark classes.
21/12/03 20:36:57 INFO metastore: Trying to connect to metastore with URI thrift://cloudera:9083
21/12/03 20:36:57 INFO metastore: Connected to metastore.
21/12/03 20:36:58 INFO SessionState: Created HDFS directory: /tmp/hive/root
21/12/03 20:36:58 INFO SessionState: Created local directory: /tmp/root
21/12/03 20:36:58 INFO SessionState: Created local directory: /tmp/be29495a-9255-4294-ab6c-73c925ef8351_resources
21/12/03 20:36:58 INFO SessionState: Created HDFS directory: /tmp/hive/root/be29495a-9255-4294-ab6c-73c925ef8351
21/12/03 20:36:58 INFO SessionState: Created local directory: /tmp/root/be29495a-9255-4294-ab6c-73c925ef8351
21/12/03 20:36:58 INFO SessionState: Created HDFS directory: /tmp/hive/root/be29495a-9255-4294-ab6c-73c925ef8351/_tmp_space.db
21/12/03 20:36:58 INFO HiveClientImpl: Warehouse location for Hive client (version 1.2.1) is file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse
21/12/03 20:36:58 INFO SessionState: Created local directory: /tmp/70ac2a43-390a-40fb-926f-cc21cafb7a0b_resources
21/12/03 20:36:58 INFO SessionState: Created HDFS directory: /tmp/hive/root/70ac2a43-390a-40fb-926f-cc21cafb7a0b
21/12/03 20:36:58 INFO SessionState: Created local directory: /tmp/root/70ac2a43-390a-40fb-926f-cc21cafb7a0b
21/12/03 20:36:58 INFO SessionState: Created HDFS directory: /tmp/hive/root/70ac2a43-390a-40fb-926f-cc21cafb7a0b/_tmp_space.db
21/12/03 20:36:58 INFO HiveClientImpl: Warehouse location for Hive client (version 1.2.1) is file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse
21/12/03 20:36:58 INFO StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
21/12/03 20:37:00 INFO CatalystSqlParser: Parsing command: string
21/12/03 20:37:00 INFO CatalystSqlParser: Parsing command: string
21/12/03 20:37:00 INFO CatalystSqlParser: Parsing command: string
21/12/03 20:37:00 INFO CatalystSqlParser: Parsing command: string
21/12/03 20:37:01 INFO SparkSqlParser: Parsing command: drop table if exists valid_events
21/12/03 20:37:01 INFO SparkSqlParser: Parsing command: 
        create external table if not exists valid_events (
            raw_event string,
            timestamp string,
            Accept string,
            Host string,
            `User-Agent` string,
            event_type string,
            direction string,
            event_detail string
            )
            stored as parquet
            location '/tmp/valid_events'
            tblproperties ("parquet.compress"="SNAPPY")
            
21/12/03 20:37:02 INFO StreamExecution: Starting [id = 9c84798c-83aa-42fe-a0f4-657fef3acc5c, runId = 4ae4f7cd-22e6-4b51-8526-75b5caa659ed]. Use /tmp/checkpoint_events to store the query checkpoint.
21/12/03 20:37:02 INFO ConsumerConfig: ConsumerConfig values: 
        metric.reporters = []
        metadata.max.age.ms = 300000
        partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
        reconnect.backoff.ms = 50
        sasl.kerberos.ticket.renew.window.factor = 0.8
        max.partition.fetch.bytes = 1048576
        bootstrap.servers = [kafka:29092]
        ssl.keystore.type = JKS
        enable.auto.commit = false
        sasl.mechanism = GSSAPI
        interceptor.classes = null
        exclude.internal.topics = true
        ssl.truststore.password = null
        client.id = 
        ssl.endpoint.identification.algorithm = null
        max.poll.records = 1
        check.crcs = true
        request.timeout.ms = 40000
        heartbeat.interval.ms = 3000
        auto.commit.interval.ms = 5000
        receive.buffer.bytes = 65536
        ssl.truststore.type = JKS
        ssl.truststore.location = null
        ssl.keystore.password = null
        fetch.min.bytes = 1
        send.buffer.bytes = 131072
        value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
        group.id = spark-kafka-source-12fc8571-e9dc-433e-a68a-939b5ec6f429-515774707-driver-0
        retry.backoff.ms = 100
        sasl.kerberos.kinit.cmd = /usr/bin/kinit
        sasl.kerberos.service.name = null
        sasl.kerberos.ticket.renew.jitter = 0.05
        ssl.trustmanager.algorithm = PKIX
        ssl.key.password = null
        fetch.max.wait.ms = 500
        sasl.kerberos.min.time.before.relogin = 60000
        connections.max.idle.ms = 540000
        session.timeout.ms = 30000
        metrics.num.samples = 2
        key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
        ssl.protocol = TLS
        ssl.provider = null
        ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
        ssl.keystore.location = null
        ssl.cipher.suites = null
        security.protocol = PLAINTEXT
        ssl.keymanager.algorithm = SunX509
        metrics.sample.window.ms = 30000
        auto.offset.reset = earliest

21/12/03 20:37:02 INFO ConsumerConfig: ConsumerConfig values: 
        metric.reporters = []
        metadata.max.age.ms = 300000
        partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
        reconnect.backoff.ms = 50
        sasl.kerberos.ticket.renew.window.factor = 0.8
        max.partition.fetch.bytes = 1048576
        bootstrap.servers = [kafka:29092]
        ssl.keystore.type = JKS
        enable.auto.commit = false
        sasl.mechanism = GSSAPI
        interceptor.classes = null
        exclude.internal.topics = true
        ssl.truststore.password = null
        client.id = consumer-1
        ssl.endpoint.identification.algorithm = null
        max.poll.records = 1
        check.crcs = true
        request.timeout.ms = 40000
        heartbeat.interval.ms = 3000
        auto.commit.interval.ms = 5000
        receive.buffer.bytes = 65536
        ssl.truststore.type = JKS
        ssl.truststore.location = null
        ssl.keystore.password = null
        fetch.min.bytes = 1
        send.buffer.bytes = 131072
        value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
        group.id = spark-kafka-source-12fc8571-e9dc-433e-a68a-939b5ec6f429-515774707-driver-0
        retry.backoff.ms = 100
        sasl.kerberos.kinit.cmd = /usr/bin/kinit
        sasl.kerberos.service.name = null
        sasl.kerberos.ticket.renew.jitter = 0.05
        ssl.trustmanager.algorithm = PKIX
        ssl.key.password = null
        fetch.max.wait.ms = 500
        sasl.kerberos.min.time.before.relogin = 60000
        connections.max.idle.ms = 540000
        session.timeout.ms = 30000
        metrics.num.samples = 2
        key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
        ssl.protocol = TLS
        ssl.provider = null
        ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
        ssl.keystore.location = null
        ssl.cipher.suites = null
        security.protocol = PLAINTEXT
        ssl.keymanager.algorithm = SunX509
        metrics.sample.window.ms = 30000
        auto.offset.reset = earliest

21/12/03 20:37:02 INFO AppInfoParser: Kafka version : 0.10.0.1
21/12/03 20:37:02 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
21/12/03 20:37:02 INFO SessionState: Created local directory: /tmp/dba54b48-bb2c-47e0-a7a3-32eaa03f2807_resources
21/12/03 20:37:02 INFO SessionState: Created HDFS directory: /tmp/hive/root/dba54b48-bb2c-47e0-a7a3-32eaa03f2807
21/12/03 20:37:03 INFO SessionState: Created local directory: /tmp/root/dba54b48-bb2c-47e0-a7a3-32eaa03f2807
21/12/03 20:37:03 INFO SessionState: Created HDFS directory: /tmp/hive/root/dba54b48-bb2c-47e0-a7a3-32eaa03f2807/_tmp_space.db
21/12/03 20:37:03 INFO HiveClientImpl: Warehouse location for Hive client (version 1.2.1) is file:/spark-2.2.0-bin-hadoop2.6/spark-warehouse
21/12/03 20:37:03 INFO StreamExecution: Starting new streaming query.
21/12/03 20:37:04 INFO AbstractCoordinator: Discovered coordinator kafka:29092 (id: 2147483646 rack: null) for group spark-kafka-source-12fc8571-e9dc-433e-a68a-939b5ec6f429-515774707-driver-0.
21/12/03 20:37:04 INFO ConsumerCoordinator: Revoking previously assigned partitions [] for group spark-kafka-source-12fc8571-e9dc-433e-a68a-939b5ec6f429-515774707-driver-0
21/12/03 20:37:04 INFO AbstractCoordinator: (Re-)joining group spark-kafka-source-12fc8571-e9dc-433e-a68a-939b5ec6f429-515774707-driver-0
21/12/03 20:37:07 INFO AbstractCoordinator: Successfully joined group spark-kafka-source-12fc8571-e9dc-433e-a68a-939b5ec6f429-515774707-driver-0 with generation 1
21/12/03 20:37:07 INFO ConsumerCoordinator: Setting newly assigned partitions [events-0] for group spark-kafka-source-12fc8571-e9dc-433e-a68a-939b5ec6f429-515774707-driver-0
21/12/03 20:37:08 INFO KafkaSource: Initial offsets: {"events":{"0":0}}
21/12/03 20:37:08 INFO StreamExecution: Committed offsets for batch 0. Metadata OffsetSeqMetadata(0,1638563828039,Map(spark.sql.shuffle.partitions -> 200))
21/12/03 20:37:08 INFO KafkaSource: GetBatch called with start = None, end = {"events":{"0":0}}
21/12/03 20:37:08 INFO KafkaSource: Partitions added: Map()
21/12/03 20:37:08 INFO KafkaSource: GetBatch generating RDD of offset range: KafkaSourceRDDOffsetRange(events-0,0,0,None)
21/12/03 20:37:08 INFO ParquetFileFormat: Using default output committer for Parquet: org.apache.parquet.hadoop.ParquetOutputCommitter
21/12/03 20:37:08 INFO CodeGenerator: Code generated in 261.231584 ms
21/12/03 20:37:09 INFO SparkContext: Starting job: start at NativeMethodAccessorImpl.java:0
21/12/03 20:37:09 INFO DAGScheduler: Got job 0 (start at NativeMethodAccessorImpl.java:0) with 1 output partitions
21/12/03 20:37:09 INFO DAGScheduler: Final stage: ResultStage 0 (start at NativeMethodAccessorImpl.java:0)
21/12/03 20:37:09 INFO DAGScheduler: Parents of final stage: List()
21/12/03 20:37:09 INFO DAGScheduler: Missing parents: List()
21/12/03 20:37:09 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[6] at start at NativeMethodAccessorImpl.java:0), which has no missing parents
21/12/03 20:37:09 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 81.5 KB, free 366.2 MB)
21/12/03 20:37:09 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 31.4 KB, free 366.2 MB)
21/12/03 20:37:09 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.24.0.6:45481 (size: 31.4 KB, free: 366.3 MB)
21/12/03 20:37:09 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1006
21/12/03 20:37:09 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (MapPartitionsRDD[6] at start at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
21/12/03 20:37:09 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
21/12/03 20:37:09 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 5001 bytes)
21/12/03 20:37:09 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
21/12/03 20:37:09 INFO Executor: Fetching file:/w205/project-3-jmdatasci/rpg_spark_stream.py with timestamp 1638563815419
21/12/03 20:37:09 INFO Utils: /w205/project-3-jmdatasci/rpg_spark_stream.py has been previously copied to /tmp/spark-0d7352fe-bcdc-4821-a934-ed7b047e5c62/userFiles-59e91c11-0eaa-41d2-b409-2c34ba03ab75/rpg_spark_stream.py
21/12/03 20:37:09 INFO ConsumerConfig: ConsumerConfig values: 
        metric.reporters = []
        metadata.max.age.ms = 300000
        partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
        reconnect.backoff.ms = 50
        sasl.kerberos.ticket.renew.window.factor = 0.8
        max.partition.fetch.bytes = 1048576
        bootstrap.servers = [kafka:29092]
        ssl.keystore.type = JKS
        enable.auto.commit = false
        sasl.mechanism = GSSAPI
        interceptor.classes = null
        exclude.internal.topics = true
        ssl.truststore.password = null
        client.id = 
        ssl.endpoint.identification.algorithm = null
        max.poll.records = 2147483647
        check.crcs = true
        request.timeout.ms = 40000
        heartbeat.interval.ms = 3000
        auto.commit.interval.ms = 5000
        receive.buffer.bytes = 65536
        ssl.truststore.type = JKS
        ssl.truststore.location = null
        ssl.keystore.password = null
        fetch.min.bytes = 1
        send.buffer.bytes = 131072
        value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
        group.id = spark-kafka-source-12fc8571-e9dc-433e-a68a-939b5ec6f429-515774707-executor
        retry.backoff.ms = 100
        sasl.kerberos.kinit.cmd = /usr/bin/kinit
        sasl.kerberos.service.name = null
        sasl.kerberos.ticket.renew.jitter = 0.05
        ssl.trustmanager.algorithm = PKIX
        ssl.key.password = null
        fetch.max.wait.ms = 500
        sasl.kerberos.min.time.before.relogin = 60000
        connections.max.idle.ms = 540000
        session.timeout.ms = 30000
        metrics.num.samples = 2
        key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
        ssl.protocol = TLS
        ssl.provider = null
        ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
        ssl.keystore.location = null
        ssl.cipher.suites = null
        security.protocol = PLAINTEXT
        ssl.keymanager.algorithm = SunX509
        metrics.sample.window.ms = 30000
        auto.offset.reset = none

21/12/03 20:37:09 INFO ConsumerConfig: ConsumerConfig values: 
        metric.reporters = []
        metadata.max.age.ms = 300000
        partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
        reconnect.backoff.ms = 50
        sasl.kerberos.ticket.renew.window.factor = 0.8
        max.partition.fetch.bytes = 1048576
        bootstrap.servers = [kafka:29092]
        ssl.keystore.type = JKS
        enable.auto.commit = false
        sasl.mechanism = GSSAPI
        interceptor.classes = null
        exclude.internal.topics = true
        ssl.truststore.password = null
        client.id = consumer-2
        ssl.endpoint.identification.algorithm = null
        max.poll.records = 2147483647
        check.crcs = true
        request.timeout.ms = 40000
        heartbeat.interval.ms = 3000
        auto.commit.interval.ms = 5000
        receive.buffer.bytes = 65536
        ssl.truststore.type = JKS
        ssl.truststore.location = null
        ssl.keystore.password = null
        fetch.min.bytes = 1
        send.buffer.bytes = 131072
        value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
        group.id = spark-kafka-source-12fc8571-e9dc-433e-a68a-939b5ec6f429-515774707-executor
        retry.backoff.ms = 100
        sasl.kerberos.kinit.cmd = /usr/bin/kinit
        sasl.kerberos.service.name = null
        sasl.kerberos.ticket.renew.jitter = 0.05
        ssl.trustmanager.algorithm = PKIX
        ssl.key.password = null
        fetch.max.wait.ms = 500
        sasl.kerberos.min.time.before.relogin = 60000
        connections.max.idle.ms = 540000
        session.timeout.ms = 30000
        metrics.num.samples = 2
        key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
        ssl.protocol = TLS
        ssl.provider = null
        ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
        ssl.keystore.location = null
        ssl.cipher.suites = null
        security.protocol = PLAINTEXT
        ssl.keymanager.algorithm = SunX509
        metrics.sample.window.ms = 30000
        auto.offset.reset = none

21/12/03 20:37:09 INFO AppInfoParser: Kafka version : 0.10.0.1
21/12/03 20:37:09 INFO AppInfoParser: Kafka commitId : a7a17cdec9eaa6c5
21/12/03 20:37:09 INFO KafkaSourceRDD: Beginning offset 0 is the same as ending offset skipping events 0
21/12/03 20:37:09 INFO CodeGenerator: Code generated in 30.667158 ms
21/12/03 20:37:09 INFO CodeGenerator: Code generated in 17.266302 ms
21/12/03 20:37:10 INFO PythonRunner: Times: total = 576, boot = 562, init = 14, finish = 0
21/12/03 20:37:10 INFO CodeGenerator: Code generated in 24.634058 ms
21/12/03 20:37:10 INFO CodeGenerator: Code generated in 30.257636 ms
21/12/03 20:37:10 INFO CodecConfig: Compression: SNAPPY
21/12/03 20:37:10 INFO CodecConfig: Compression: SNAPPY
21/12/03 20:37:10 INFO ParquetOutputFormat: Parquet block size to 134217728
21/12/03 20:37:10 INFO ParquetOutputFormat: Parquet page size to 1048576
21/12/03 20:37:10 INFO ParquetOutputFormat: Parquet dictionary page size to 1048576
21/12/03 20:37:10 INFO ParquetOutputFormat: Dictionary is on
21/12/03 20:37:10 INFO ParquetOutputFormat: Validation is off
21/12/03 20:37:10 INFO ParquetOutputFormat: Writer version is: PARQUET_1_0
21/12/03 20:37:10 INFO ParquetOutputFormat: Maximum row group padding size is 0 bytes
21/12/03 20:37:10 INFO ParquetOutputFormat: Page size checking is: estimated
21/12/03 20:37:10 INFO ParquetOutputFormat: Min row count for page size check is: 100
21/12/03 20:37:10 INFO ParquetOutputFormat: Max row count for page size check is: 10000
21/12/03 20:37:10 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
{
  "type" : "struct",
  "fields" : [ {
    "name" : "raw_event",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "timestamp",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Accept",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Host",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "User-Agent",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "event_type",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "direction",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "event_detail",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  } ]
}
and corresponding Parquet message type:
message spark_schema {
  optional binary raw_event (UTF8);
  optional binary timestamp (UTF8);
  optional binary Accept (UTF8);
  optional binary Host (UTF8);
  optional binary User-Agent (UTF8);
  optional binary event_type (UTF8);
  optional binary direction (UTF8);
  optional binary event_detail (UTF8);
}

       
21/12/03 20:37:10 INFO CodecPool: Got brand-new compressor [.snappy]
21/12/03 20:37:10 INFO InternalParquetRecordWriter: Flushing mem columnStore to file. allocated memory: 0
21/12/03 20:37:10 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2524 bytes result sent to driver
21/12/03 20:37:10 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1399 ms on localhost (executor driver) (1/1)
21/12/03 20:37:10 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
21/12/03 20:37:10 INFO DAGScheduler: ResultStage 0 (start at NativeMethodAccessorImpl.java:0) finished in 1.427 s
21/12/03 20:37:10 INFO DAGScheduler: Job 0 finished: start at NativeMethodAccessorImpl.java:0, took 1.705254 s
21/12/03 20:37:10 INFO FileStreamSinkLog: Set the compact interval to 10 [defaultCompactInterval: 10]
21/12/03 20:37:10 INFO ManifestFileCommitProtocol: Committed batch 0
21/12/03 20:37:10 INFO FileFormatWriter: Job null committed.
21/12/03 20:37:10 INFO StreamExecution: Streaming query made progress: {
  "id" : "9c84798c-83aa-42fe-a0f4-657fef3acc5c",
  "runId" : "4ae4f7cd-22e6-4b51-8526-75b5caa659ed",
  "name" : null,
  "timestamp" : "2021-12-03T20:37:03.017Z",
  "numInputRows" : 0,
  "processedRowsPerSecond" : 0.0,
  "durationMs" : {
    "addBatch" : 2487,
    "getBatch" : 239,
    "getOffset" : 5006,
    "queryPlanning" : 89,
    "triggerExecution" : 7905,
    "walCommit" : 49
  },
  "stateOperators" : [ ],
  "sources" : [ {
    "description" : "KafkaSource[Subscribe[events]]",
    "startOffset" : null,
    "endOffset" : {
      "events" : {
        "0" : 0
      }
    },
    "numInputRows" : 0,
    "processedRowsPerSecond" : 0.0
  } ],
  "sink" : {
    "description" : "FileSink[/tmp/valid_events]"
  }
}
21/12/03 20:37:10 INFO StreamExecution: Streaming query made progress: {
  "id" : "9c84798c-83aa-42fe-a0f4-657fef3acc5c",
  "runId" : "4ae4f7cd-22e6-4b51-8526-75b5caa659ed",
  "name" : null,
  "timestamp" : "2021-12-03T20:37:10.989Z",
  "numInputRows" : 0,
  "inputRowsPerSecond" : 0.0,
  "processedRowsPerSecond" : 0.0,
  "durationMs" : {
    "getOffset" : 5,
    "triggerExecution" : 5
  },
  "stateOperators" : [ ],
  "sources" : [ {
    "description" : "KafkaSource[Subscribe[events]]",
    "startOffset" : {
      "events" : {
        "0" : 0
      }
    },
    "endOffset" : {
      "events" : {
        "0" : 0
      }
    },
    "numInputRows" : 0,
    "inputRowsPerSecond" : 0.0,
    "processedRowsPerSecond" : 0.0
  } ],
  "sink" : {
    "description" : "FileSink[/tmp/valid_events]"
  }
}
21/12/03 20:37:30 INFO StreamExecution: Streaming query made progress: {
  "id" : "9c84798c-83aa-42fe-a0f4-657fef3acc5c",
  "runId" : "4ae4f7cd-22e6-4b51-8526-75b5caa659ed",
  "name" : null,
  "timestamp" : "2021-12-03T20:37:30.000Z",
  "numInputRows" : 0,
  "inputRowsPerSecond" : 0.0,
  "processedRowsPerSecond" : 0.0,
  "durationMs" : {
    "getOffset" : 3,
    "triggerExecution" : 3
  },
  "stateOperators" : [ ],
  "sources" : [ {
    "description" : "KafkaSource[Subscribe[events]]",
    "startOffset" : {
      "events" : {
        "0" : 0
      }
    },
    "endOffset" : {
      "events" : {
        "0" : 0
      }
    },
    "numInputRows" : 0,
    "inputRowsPerSecond" : 0.0,
    "processedRowsPerSecond" : 0.0
  } ],
  "sink" : {
    "description" : "FileSink[/tmp/valid_events]"
  }
}
21/12/03 20:37:50 INFO StreamExecution: Streaming query made progress: {
  "id" : "9c84798c-83aa-42fe-a0f4-657fef3acc5c",
  "runId" : "4ae4f7cd-22e6-4b51-8526-75b5caa659ed",
  "name" : null,
  "timestamp" : "2021-12-03T20:37:50.000Z",
  "numInputRows" : 0,
  "inputRowsPerSecond" : 0.0,
  "processedRowsPerSecond" : 0.0,
  "durationMs" : {
    "getOffset" : 3,
    "triggerExecution" : 3
  },
  "stateOperators" : [ ],
  "sources" : [ {
    "description" : "KafkaSource[Subscribe[events]]",
    "startOffset" : {
      "events" : {
        "0" : 0
      }
    },
    "endOffset" : {
      "events" : {
        "0" : 0
      }
    },
    "numInputRows" : 0,
    "inputRowsPerSecond" : 0.0,
    "processedRowsPerSecond" : 0.0
  } ],
  "sink" : {
    "description" : "FileSink[/tmp/valid_events]"
  }
}
```
  
---
