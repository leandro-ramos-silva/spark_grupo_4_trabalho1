import json
from datetime import datetime
import random
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter



class CreateMessage:

    def randomMessage():
        # Define Variable
        codigo_cliente  = "Python"
        agencia = random.randint(1000,9999)
        valor_operacao = random.randint(-10000,10000)
        #deposito, saque
        if valor_operacao > 0:
            tipo_operacao = "deposito"
        else:
            tipo_operacao = "saque"
        #timestamp    
        data = datetime.now().timestamp()
        saldo_conta = valor_operacao + random.randint(0, 10000) 
        # Create Dictionary
        value = {
            "codigo_cliente": codigo_cliente,
            "agencia": agencia,
            "valor_operacao":valor_operacao,
            "tipo_operacao": tipo_operacao,
            "data": data,
            "saldo_conta" : saldo_conta
        }
 
        
        return json.dumps(value) 

def get_codigo_cliente():
    return  random.randint(1000,9999)
    
def get_valor_operacao():
        valor = random.randint(-10000,10000) 
        if valor == 0:
            valor = 10
        return  valor 

def get_agencia():
    return random.randint(1000,9999)

def get_data_timestamp():
    return datetime.now().timestamp()

def get_tipo_operacao(valor_operacao):
    if valor_operacao > 0:
        return "deposito"
    else:
        return "saque"


