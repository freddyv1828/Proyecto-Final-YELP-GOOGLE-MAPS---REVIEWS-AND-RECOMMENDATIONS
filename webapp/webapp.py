from flask import Flask, request, send_from_directory
from google.cloud import storage
import os

app = Flask(__name__)

# Obtén la información de autenticación desde la variable de entorno en lugar del archivo JSON
credentials_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

# Si la variable de entorno no está definida, se generará un error, así que asegúrate de manejarlo adecuadamente
if credentials_json is None:
    raise ValueError("La variable de entorno GOOGLE_APPLICATION_CREDENTIALS no está definida")


# Escribe el contenido del JSON en un archivo
with open('credentials.json', 'w') as f:
    f.write(credentials_json)

# Crea el cliente de almacenamiento utilizando las credenciales
storage_client = storage.Client.from_service_account_json('credentials.json')
bucket_name = 'yelp-gmaps-data'

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return 'No se ha proporcionado ningún archivo', 400
    
    file = request.files['file']
    if file.filename == '':
        return 'No se ha seleccionado ningún archivo', 400

    # Obtiene la carpeta de destino basada en el valor del campo oculto "folder" en el formulario
    destination_folder = request.form['folder']
    if destination_folder not in ['yelp', 'google-maps']:
        return 'La carpeta de destino no es válida', 400

    # Sube el archivo al bucket
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'{destination_folder}/{file.filename}')
    blob.upload_from_string(
        file.read(),
        content_type=file.content_type
    )

    return 'Archivo subido exitosamente', 200

if __name__ == '__main__':
    app.run(debug=True)
