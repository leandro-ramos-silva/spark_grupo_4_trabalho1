import findspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
findspark.init()
from pyspark import SparkContext, SparkConf
from kafka import KafkaConsumer
from pyspark.streaming import StreamingContext
from pyspark.sql.avro.functions import from_avro
import avro.schema
import time
from avro.io import DatumWriter
from kafka import KafkaClient, KafkaProducer
from datetime import datetime


#from pyspark.streaming.kafka  KafkaUtils


from json import loads


findspark.add_packages(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.1,org.apache.spark:spark-avro_2.12:3.3.0"])

TOPICO = "raw_message"
KAFKA_SERVER = "localhost:9092"

SCHEMA_PATH = "trabalho1.avsc"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())



avroSchema = '''{
                    "type": "record",
                    "name": "Operacao",
                    "namespace": "avro.trabalho1",
                    "fields": [
                                {"name": "codigo_cliente", "type": "string"},
                                {"name": "agencia", "type": "int"},
                                {"name": "valor_operacao", "type": "int"},
                                {"name": "tipo_operacao", "type": "string"},
                                {"name": "data", "type": "double"},
                                {"name": "saldo_conta", "type": "int"}
                    ]
                    }'''

spark = SparkSession.builder.appName("test").getOrCreate()
kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_SERVER).option("startingOffsets", "earliest").option("subscribe", TOPICO).load()
#query = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").start()
#query.awaitTermination()



#value_df = kafka_df.select((from_avro(col("value"), avroSchema)).alias("operacao"))


#value_df.show(2)
    # value_df.show()

    # Wait for 20 seconds, collect all events and create a file save on output folder.
#write_query = value_df.writeStream.format("json").option("checkpointLocation", "../chk-point-dir").option("path", "../output").outputMode("append").trigger(processingTime="5 seconds").start()
#write_query = value_df.writeStream.outputMode("complete").option("checkpointLocation", "path/to/HDFS/dir").format("memory").start()

###
#operacaoDF = kafka_df.select(from_avro(col("value"), avroSchema).alias("operacao")).select("operacao.*")


operacaoDF = kafka_df.select(from_avro(col("value"), avroSchema).alias("operacao")).select("operacao.*") #.writeStream.queryName("sample").outputMode("append").format("kafka").option("checkpointLocation", "../checkpoint/kafka/").start()


rawQuery = operacaoDF.writeStream.queryName("qraw").format("memory").start().processAllAvailable()
raw = spark.sql("select * from qraw")
raw.show()



#operacaoDF.awaitTermination()
#personDF.printSchema()
print("###select####")



#personDF.writeStream.format("console").outputMode("append").start().awaitTermination()
#if __name__ == "__main__":
 #   i = 1
 #   while  i==1:
        #coluna = personDF.collect()
        #print(coluna)
        #df = personDF.writeStream.format("memory").outputMode("append").queryName("aggregates").start() #.trigger(processingTime="5 seconds").start().processAllAvailable()
        #personDF.writeStream.format('json').start().processAllAvailable()
        
        #df.processAllAvailable()
        #spark.sql("select * from aggregates").show()
        
        #time.sleep(4)
        #df.stop()
       #print("##new loop##")
        #i = 2


print("###writeStream####")


#https://sparkbyexamples.com/spark/spark-streaming-consume-and-produce-kafka-messages-in-avro-format/

###

#write_query.awaitTermination()

#kafka_df.printSchema()