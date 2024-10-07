import boto3
import json
from decimal import Decimal

# Configuración de DynamoDB y S3
nombreTabla = "Products"  # Nombre de la tabla en DynamoDB
nombreBucket = "bucket-ingesta-parcial"  # Nombre del bucket en S3
ficheroUpload = "data.json"  # Nombre del archivo JSON a subir a S3

# Conectar a DynamoDB (usa el endpoint si es DynamoDB local)
dynamodb = boto3.resource('dynamodb', endpoint_url="http://3.230.28.178:8000", region_name="us-west-1")
table = dynamodb.Table(nombreTabla)

# Función para obtener los datos de DynamoDB
def obtener_datos_dynamodb():
    response = table.scan()  # Realiza un scan para obtener todos los elementos
    data = response['Items']

    # Manejar paginación si hay más elementos que los retornados en el primer scan
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])  # Agregar los nuevos elementos

    return data

# Función personalizada para manejar objetos de tipo Decimal
def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)  # Convertir Decimal a float
    raise TypeError

# Obtener los datos de DynamoDB
data = obtener_datos_dynamodb()

# Guardar los datos en un archivo JSON localmente
with open(ficheroUpload, 'w') as file:
    json.dump(data, file, indent=4, default=decimal_default)  # Usar la función para convertir Decimals

print(f"Datos exportados a {ficheroUpload} correctamente.")

# Subir el archivo JSON al bucket de S3
s3 = boto3.client('s3')

try:
    s3.upload_file(ficheroUpload, nombreBucket, ficheroUpload)
    print(f"Archivo {ficheroUpload} subido a S3 correctamente en el bucket {nombreBucket}.")
except Exception as e:
    print(f"Error al subir el archivo a S3: {e}")
