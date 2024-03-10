import mysql.connector

# Credenciales de la base de datos

host = 'fdb1034.awardspace.net'
port = '3306'
database = '4429487_pfhenry'
user = '4429487_pfhenry'
password = 'Pfhenry54321'

# Crear la conexion 
connection = mysql.connector.connect(
    host = host,
    port = port,
    database = database,
    user = user,
    password = password
)

cursor = connection.cursor()