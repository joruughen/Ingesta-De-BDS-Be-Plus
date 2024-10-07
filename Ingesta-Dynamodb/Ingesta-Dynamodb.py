import boto3
import json
from decimal import Decimal

nombreTabla = "Products"
nombreBucket = "bucket-ingesta-parcial"
ficheroUpload = "data.json"

dynamodb = boto3.resource('dynamodb', endpoint_url="http://3.230.28.178:8000", region_name="us-west-1")
table = dynamodb.Table(nombreTabla)

def obtener_datos_dynamodb():
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return data

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

data = obtener_datos_dynamodb()

with open(ficheroUpload, 'w') as file:
    json.dump(data, file, indent=4, default=decimal_default)

print(f"Datos exportados a {ficheroUpload} correctamente.")

s3 = boto3.client('s3')

try:
    s3.upload_file(ficheroUpload, nombreBucket, ficheroUpload)
    print(f"Archivo {ficheroUpload} subido a S3 correctamente en el bucket {nombreBucket}.")
except Exception as e:
    print(f"Error al subir el archivo a S3: {e}")
