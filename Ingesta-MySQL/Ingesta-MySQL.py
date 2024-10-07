import mysql.connector
import pandas as pd
import boto3

db_connection = mysql.connector.connect(
    host='3.230.28.178',
    user='root',
    password='utec',
    database='mysql',
    port=8002
)

cursor = db_connection.cursor()

def export_table_to_s3(table_name, bucket_name):
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)

    result = cursor.fetchall()

    column_names = [i[0] for i in cursor.description]

    df = pd.DataFrame(result, columns=column_names)

    csv_filename = f'{table_name}_datos_mysql.csv'
    df.to_csv(csv_filename, index=False)

    s3 = boto3.client('s3')
    s3.upload_file(csv_filename, bucket_name, csv_filename)
    print(f"Archivo CSV para la tabla {table_name} subido exitosamente a S3")


bucket_name = 'bucket-ingesta-parcial'

export_table_to_s3('rockies', bucket_name)
export_table_to_s3('accesorios', bucket_name)

cursor.close()
db_connection.close()
