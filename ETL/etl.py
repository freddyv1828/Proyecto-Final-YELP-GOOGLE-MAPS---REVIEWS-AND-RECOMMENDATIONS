from matplotlib.pylab import f
import pandas as pd
from sqlalchemy import true
import funciones
import numpy as np

df_business = pd.read_pickle('DATASETS/business.pkl')

 # Verificar si un dataset cumple con las columnas especificadas para un tipo de archivo
def check_rows(df, nombre_df):
    
    # Nombres de las columnas que corresponden a cada tabla
    columnas = {
        "business": ["business_id", "name", "address", "city", "state", "postal_code", "latitude", "longitude", "stars", "review_count", "is_open", "attributes", "categories", "hours"],
        "review": ["review_id", "user_id", "business_id", "stars", "date", "text", "useful", "funny", "cool"],
        "tip": ["text", "date", "compliment_count", "business_id", "user_id"],
        "checkin": ["business_id", "date"],
        "user": ["user_id", "name", "review_count", "yelping_since", "friends", "useful", "funny", "cool", "fans", "elite", "average_stars", "compliment_hot", "compliment_more", "compliment_profile", "compliment_cute", "compliment_list", "compliment_note", "compliment_plain", "compliment_cool", "compliment_funny", "compliment_writer", "compliment_photos"]
    }
    
    # Validar Nombre_df
    nombre_df_valido = set(columnas.keys())
    if nombre_df not in nombre_df_valido:
        raise ValueError(f"Parámetro no válido: '{nombre_df}'. Opciones válidas: {', '.join(nombre_df_valido)}")
    
    # Obtener columnas esperadas intersectando los conjntos
    columnas_esperadas = set(columnas[nombre_df]) & set(df.columns)
    
    # Comprobar si estan presentes las columnas requeridas
    if len(columnas_esperadas) != len(columnas[nombre_df]):
        columnas_faltantes = set(columnas[nombre_df]) - columnas_esperadas
        return False, f"Faltan columnas obligatorias: {', '.join(columnas_faltantes)}"
    
    return print(f"Dataset {nombre_df}, cargado con exito")

check_rows(df_business, "business")

# Normalizacion de los datos

# Carga de los datasets
