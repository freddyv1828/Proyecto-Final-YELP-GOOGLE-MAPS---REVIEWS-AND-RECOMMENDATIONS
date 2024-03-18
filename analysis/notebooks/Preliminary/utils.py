from google.oauth2 import service_account
from google.cloud import storage
import pandas as pd

def set_config():
    # Configuración de Google Cloud Storage
    bucket_name = "yelp-ggmaps-data"

    # Ruta al archivo JSON de credenciales
    credentials_path = "../credentials/eminent-cycle-415715-3ef9bde04901.json"

    # Crear credenciales a partir del archivo JSON
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # Crear cliente de almacenamiento con las credenciales
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)

    return bucket

def save_in_storage(bucket,path,df):
     #Exportar / Guardar el DataFrame filtrado ya definito / procesado
       
        processed_blob_path = path
        processed_blob = bucket.blob(processed_blob_path)
        csv_data = df.to_csv(index=False).encode('utf-8')

        processed_blob.upload_from_string(csv_data, content_type='text/csv')

        print(f"El DF se guardó correctamente en {processed_blob_path}")

def cargar_df(path):
    try:
        # Cargar el archivo en un DataFrame
        df = pd.read_csv(path)  # Cambiar a read_excel si es un archivo Excel
        return df
    except Exception as e:
        print("Error al cargar el DataFrame:", e)
        return None