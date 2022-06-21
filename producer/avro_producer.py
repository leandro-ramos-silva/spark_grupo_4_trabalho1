import io
import random
import avro.schema
import time
from avro.io import DatumWriter
from kafka import KafkaClient, KafkaProducer
from datetime import datetime

# To send messages synchronously
#KAFKA = KafkaClient('localhost:9092')


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Kafka topic
TOPICO = "raw_message"

# Path to user.avsc avro schema
SCHEMA_PATH = "trabalho1.avsc"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())


if __name__ == "__main__":
    while  1==1:  
        codigo_cliente  = "Python"
        agencia = random.randint(1000,9999)
        valor_operacao = random.randint(-10000,10000)
        if valor_operacao == 0:
            valor_operacao = 1145

        #deposito, saque
        if valor_operacao > 0:
            tipo_operacao = "deposito"
        else:
            tipo_operacao = "saque"
        #timestamp    
        data = datetime.now().timestamp()
        saldo_conta = valor_operacao + random.randint(0, 10000)

        writer = DatumWriter(SCHEMA)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write({"codigo_cliente":codigo_cliente, "agencia": agencia,  "valor_operacao":valor_operacao ,"tipo_operacao":tipo_operacao ,"data":data , "saldo_conta":saldo_conta }, encoder)   
        raw_bytes = bytes_writer.getvalue()
        print("##New Message##")
        producer.send(TOPICO, raw_bytes )
        time.sleep(3)


