import functions_framework
import utils as ut
import etl_functions as etl
import yelp_etl
import google_maps_etl

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    print(cloud_event.data["name"])

   
    nombre_archivo = cloud_event.data["name"]

    bucket_entrada_nombre = 'yelp-gmaps-data'
    bucket_entrada = ut.get_bucket(bucket_entrada_nombre)
    
    bucket_salida_nombre = 'yelp-gmaps-work'
    bucket_salida = ut.get_bucket(bucket_salida_nombre)    

    #Ver si es archivo de google-maps o yelp

    tipo_archivo = ut.asignar_tipo_archivo(nombre_archivo)

    #Validar tamaño, si el tipo es "business" debe pesar menos de 1 megabyte
    if tipo_archivo == "business" or tipo_archivo == "sitio":
        if not ut.verificar_tamanio_dataframe(bucket_entrada,nombre_archivo,1): # 1 megabyte de peso maximo por el momento
            print("El tamaño del archivo de business de yelp es superior al admitido")
            return False


    #Verificar formato de columnas y  llamar funcion de ETL para google o yelp segun corresponda

    df = ut.descargar_archivo_gcs(bucket_entrada,nombre_archivo)    

    if "yelp" in nombre_archivo:
        if not etl.check_rows_yelp(df,tipo_archivo):
            print("No se cumple el formato de columnas de un archivo en Yelp")
            return False
        else:            
            yelp_etl.process_file(df,bucket_salida,tipo_archivo)

    elif "google" in nombre_archivo:
        if not etl.check_rows_google(df,tipo_archivo):
            print("No se cumple el formato de columnas de un archivo en Yelp")
            return False
        else:
            google_maps_etl.process_file(df,bucket_salida,tipo_archivo)

    else:
        print("El archivo no es de Yelp ni de Google")
        return False
    

    

    
  

