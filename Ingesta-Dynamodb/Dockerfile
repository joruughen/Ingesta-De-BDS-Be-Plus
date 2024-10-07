# Usar una imagen base oficial de Python
FROM python:3.12-slim

# Establecer el directorio de trabajo en /app
WORKDIR /app

# Copiar el archivo requirements.txt al contenedor
COPY requirements.txt .

# Instalar las dependencias necesarias utilizando pip
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el script de ingesta al contenedor
COPY ingesta_dynamodb.py .

# Comando para ejecutar el script de ingesta cuando se inicie el contenedor
CMD ["python", "ingesta_dynamodb.py"]
