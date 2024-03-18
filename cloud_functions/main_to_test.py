import utils as ut
import etl_functions as etl
import yelp_etl
import google_maps_etl
import pandas as pd

# Triggered by a change in a storage bucket

def hello_gcs(nombre_archivo):
    
        
    bucket_salida_nombre = 'yelp-gmaps-work'
    bucket_salida = ut.get_bucket(bucket_salida_nombre)    

    #Ver si es archivo de google-maps o yelp

    tipo_archivo = ut.asignar_tipo_archivo(nombre_archivo)    

    df = pd.read_json(nombre_archivo,lines=True) 

    if "yelp" in nombre_archivo:
        if not etl.check_rows_yelp(df,tipo_archivo):
            print("No se cumple el formato de columnas de un archivo en Yelp")
            return False
        else:            
            yelp_etl.process_file(df,bucket_salida,tipo_archivo)

    elif "google" in nombre_archivo:
        if not etl.check_rows_google(df,tipo_archivo):
            print("No se cumple el formato de columnas de un archivo en google_maps")
            return False
        else:
            google_maps_etl.process_file(df,bucket_salida,tipo_archivo)

    else:
        print("El archivo no es de Yelp ni de Google")
        return False
    
hello_gcs("raw_google_reviews_sample.json")