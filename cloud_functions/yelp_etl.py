import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import io
import importlib

import utils as ut
import etl_functions as etl

from os.path import exists

importlib.reload(ut)

def process_file(df_nuevo,bucket_salida,tipo_archivo): # Retorna verdadero  o falso si todo sale bien o mal    

    # Casos para hacer cosas en función del parámetro
    if tipo_archivo == "business":       
        
        # Normalmente, los datos de yelp de negocios vienen con las columnas duplicadas a partir de la 14
        df_nuevo = df_nuevo.iloc[:, :14]

        # Corregir estados y ciudades y filtrar los establecimientos por estados específicos
        df_nuevo = ut.corregir_ubicaciones(df_nuevo)
        df_nuevo = df_nuevo[(df_nuevo['state'] == 'Florida') | (df_nuevo['state'] == 'California') | (df_nuevo['state'] == 'Nevada')]

        if exists(f'{bucket_salida}/used_ids/business_ids.csv'):
            # Si existe, leer el archivo
            data_business_unique_ids = ut.obtener_data_archivo_a_actualizar(bucket_salida,'used_ids/business_ids.csv')
            unique_ids = pd.read_csv(io.BytesIO(data_business_unique_ids))
        else:
            # Si no existe, se crea un dataframe nuevo para ser usado y guardado posteriormente
            unique_ids = pd.DataFrame()
            unique_ids['business_id'] = []


        # Proceso ETL
        ruta_archivo_a_actualizar = f"processed/business.csv" # actuaiza esto en todos
      
        data_archivo_a_actualizar = ut.obtener_data_archivo_a_actualizar(bucket_salida,ruta_archivo_a_actualizar)
        

        if data_archivo_a_actualizar is not None:           
            df_a_actualizar = pd.read_csv(io.BytesIO(data_archivo_a_actualizar))          
            df_final = etl.procesar_yelp(df_a_actualizar,df_nuevo, unique_ids,bucket_salida, tipo_archivo)
        else:
            
            df_final = etl.procesar_yelp(None,df_nuevo, unique_ids,bucket_salida, tipo_archivo)

        ut.save_in_storage(bucket_salida,ruta_archivo_a_actualizar,df_final)

        pass

    elif tipo_archivo == "review":
        pre_post_procesamiento(bucket_salida,tipo_archivo)
        pass

    elif tipo_archivo == "checkin":
        pre_post_procesamiento(bucket_salida,tipo_archivo)
        pass

def pre_post_procesamiento(bucket_salida,tipo_archivo):
    #Abrir dataframe de id's de negocios para verificar si las reseñas corresponden a negocios existentes y actualizarlo
        if exists(f'{bucket_salida}/used_ids/business_ids.csv'):
            # Si existe, leer el archivo
            data_business_unique_ids = ut.obtener_data_archivo_a_actualizar(bucket_salida,'used_ids/business_ids.csv')
            unique_ids = pd.read_csv(io.BytesIO(data_business_unique_ids))
        else:
            return False
    
        ruta_archivo_a_actualizar = f"processed/{tipo_archivo}.csv"

        data_archivo_a_actualizar = ut.obtener_data_archivo_a_actualizar(bucket_salida,ruta_archivo_a_actualizar)       

        #Filtrar df nuevo dejando solo los asociados a negocios existentes
        df_nuevo = df_nuevo[df_nuevo['business_id'].isin(unique_ids['business_id'])]

        # Proceso ETL
        if data_archivo_a_actualizar is not None:
            df_a_actualizar = pd.read_csv(io.BytesIO(data_archivo_a_actualizar))
            df_final = etl.procesar_yelp(df_a_actualizar,df_nuevo, unique_ids,bucket_salida, tipo_archivo)
        else: 
            df_final = etl.procesar_nulos_duplicados(None,df_nuevo, unique_ids,bucket_salida, tipo_archivo)

        ut.save_in_storage(bucket_salida,ruta_archivo_a_actualizar,df_final)