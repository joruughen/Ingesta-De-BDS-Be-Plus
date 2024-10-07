import mysql.connector
import pandas as pd
import boto3

# Conexión a la base de datos MySQL
db_connection = mysql.connector.connect(
    host='3.230.28.178',
    user='root',
    password='utec',
    database='mysql',
    port=8002
)

# Crear un cursor para ejecutar las consultas
cursor = db_connection.cursor()

# Función para extraer datos de una tabla y subirlos a S3
def export_table_to_s3(table_name, bucket_name):
    # Consulta para obtener los datos de la tabla
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)

    # Obtener los resultados de la consulta
    result = cursor.fetchall()

    # Obtener los nombres de las columnas
    column_names = [i[0] for i in cursor.description]

    # Convertir los resultados a un DataFrame de pandas
    df = pd.DataFrame(result, columns=column_names)

    # Guardar los datos en un archivo CSV
    csv_filename = f'{table_name}_datos_mysql.csv'
    df.to_csv(csv_filename, index=False)

    # Subir el archivo CSV al bucket de S3
    s3 = boto3.client('s3')
    s3.upload_file(csv_filename, bucket_name, csv_filename)
    print(f"Archivo CSV para la tabla {table_name} subido exitosamente a S3")

# Nombre del bucket de S3
bucket_name = 'mysql-ingesta-testbench'

# Exportar las tablas 'rockies' y 'accesorios' a S3
export_table_to_s3('rockies', bucket_name)
export_table_to_s3('accesorios', bucket_name)

# Cerrar la conexión a la base de datos
cursor.close()
db_connection.close()
