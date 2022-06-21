#import findspark
from  createMessage import CreateMessage
from kafka import KafkaProducer
import logging
import time
import avro.schema

#from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
#findspark.init()
#from pyspark import SparkContext, SparkConf




logging.basicConfig(level=logging.INFO)
import json

def avro_serializer(data):
    return avro.schema.parse(json_serializer(data))

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)
#producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=avro_serializer)

if __name__ == "__main__":
    while  1==1:
       randomMessage =  CreateMessage.randomMessage() 
       print("##New Message##")
       producer.send("raw_message", randomMessage )
       time.sleep(3)

#producer.send('receive_raw_message', b'Message from PyCharm')
#producer.send('receive_raw_message', key=b'message-two', value=b'This is Kafka-Python')
#print("iniciado flush")
#producer.flush()
#print("fim flush")




#avro.schema.parse(json_schema_string)

###spark = SparkContext(appName="aula2_exercicio1")

#spark = SparkSession.builder.appName("aula3_exercicio2").getOrCreate()
# carregar dataframe

#print(CreateMessage.randomMessage() )
#print(createMessage())
#df_scimago =spark.read.load(scimago, format="csv", sep=";", inferSchema=True, header=True)
#df_scimago.show()
#spark.stop()


#juntar arquivos title e rankin left join 
#print("### fim ###")


