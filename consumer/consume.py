import findspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
findspark.init()
from pyspark import SparkContext, SparkConf
from kafka import KafkaConsumer
from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka  KafkaUtils


from json import loads


findspark.add_packages(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.1"])

TOPICO = "raw_message"
KAFKA_SERVER = "localhost:9092"

spark = SparkSession.builder.appName("test").getOrCreate()
kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_SERVER).option("startingOffsets", "latest").option("subscribe", TOPICO).load()
query = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.format("console").option("checkpointLocation", "path/to/HDFS/dir").start()
query.awaitTermination()

print('##fim sucesso##')




#for message in consumer:
 #   message = message.value
    #collection.insert_one(message)
    #print('{} added to {}')