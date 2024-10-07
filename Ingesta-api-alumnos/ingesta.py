import boto3
import psycopg2 
import csv

db_host = "3.230.28.178"
db_user = "postgres"
db_password = "utec"
db_name = "rockie"
db_port = 8001

ficheroUpload_activities = "activities.csv"
ficheroUpload_student = "student.csv"
nombreBucket = "bucket-ingesta-parcial"

conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    user=db_user,
    password=db_password,
    database=db_name
)
cursor = conn.cursor()

def export_table_to_csv(table_name, file_name):
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)

    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)
        writer.writerows(rows)

export_table_to_csv('activities', ficheroUpload_activities)
export_table_to_csv('students', ficheroUpload_student)

cursor.close()
conn.close()

s3 = boto3.client('s3')
s3.upload_file(ficheroUpload_activities, nombreBucket, ficheroUpload_activities)
s3.upload_file(ficheroUpload_student, nombreBucket, ficheroUpload_student)

print("Ingesta completada y archivos subidos a S3")
