import numpy as np
import pandas as pd
import utils as ut
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.sentiment import SentimentIntensityAnalyzer

def check_rows_yelp(df, parametro):
    # Diccionario que contiene los nombres de columnas correspondientes a cada tabla
    # Basta que tengan todas las columnas mencionadas, se ignoraran las columnas sobrantes
    columnas = {
        "business": ["business_id", "name", "address", "city", "state", "postal_code", "latitude", "longitude", "stars", "review_count", "is_open", "attributes", "categories", "hours"],
        "review": ["review_id", "user_id", "business_id", "stars", "date", "text", "useful", "funny", "cool"],
        "checkin": ["business_id", "date"]
        }
    
    # Verificar si el parámetro es válido
    if parametro not in columnas:
        return False
        
    # Verificar si el DataFrame contiene todas las columnas correspondientes al parámetro
    if all(col in df.columns for col in columnas[parametro]):
        return True
    else:     
        return False


def check_rows_google(df, parametro):
    # Diccionario que contiene los nombres de columnas correspondientes a cada tabla

    # Basta que tengan todas las columnas mencionadas, se ignoraran las columnas sobrantes
    columnas = {
        "sitio": ["gmap_id", "name", "address", "latitude", "longitude", "avg_rating", "num_of_reviews", "category"],
        "review": ["gmap_id", "rating", "time", "text"]       
    }
    
    # Verificar si el parámetro es válido
    if parametro not in columnas:
        print(parametro)        
        return False
        
    # Verificar si el DataFrame contiene todas las columnas correspondientes al parámetro
    if all(col in df.columns for col in columnas[parametro]):
        return True
    else:    
        return False      


def procesar_yelp(df_base,df_nuevo,df_unique_business_ids,bucket,tipo):        

        # Casos para hacer cosas en función del parámetro
        if tipo == "business":            
            # Dejamos solo las columnas relevantes para el analisis
            columnas_a_conservar = ["business_id", "name", "address", "city", "state", "latitude", "longitude", "stars", "review_count", "attributes", "categories"]
            df_nuevo = df_nuevo.loc[:, columnas_a_conservar]

            # Definir criterio de integridad de fila 70% de fila debe tener datos no nulos
            threshold = int(0.7 * len(df_nuevo.columns))

            # Eliminar filas que no cumplen con el criterio de integridad
            df_nuevo = df_nuevo.dropna(thresh=threshold)

            # Reemplazar nulos en columnas categóricas con "sin datos"
            df_nuevo.loc[:, ['name', 'address', 'city', 'state', 'attributes', 'categories']] = df_nuevo.loc[:, ['name', 'address', 'city', 'state', 'attributes', 'categories']].fillna('sin datos')

            # Eliminar filas con valores nulos en 'business_id ', 'stars' o 'review_count'
            df_nuevo = df_nuevo.dropna(subset=['business_id', 'stars', 'review_count', 'latitude', 'longitude'])            

            # Redefinir las categorias y dejar una sola por fila en la columna "category"
            df_nuevo = ut.filtrar_por_categoria(df_nuevo)

            # Redefinir los atributos y crear nuevas columnas
            df_nuevo = ut.generar_atributos(df_nuevo)       
        
            # Concatenar dataframe base con dataframe nuevo
            if df_base is not None:
                # Concatenar dataframe base con dataframe nuevo               
                df_concat = concatenar_dataframes(df_base, df_nuevo)
            else:    
                df_concat = df_nuevo
            
            # Eliminar filas duplicadas basadas en 'business_id' dejando solo la que mayor cantidad de reviews tenga
            df_concat = df_concat.loc[df_concat.groupby('business_id')['review_count'].idxmax()]
            
            # Eliminamos duplicados si se mezclaron datos de distintas fuentes mediante coordenadas y nombre del negocio
            df_concat = eliminar_duplicados_distintas_fuentes(df_concat)

            # Resetear los índices después de las operaciones
            df_concat = df_concat.reset_index(drop=True)

           # Identificar business_id únicos presentes en df_concat pero no en df_unique_business_ids
            nuevos_business_ids = set(df_concat['business_id']) - set(df_unique_business_ids['business_id'])

            # Filtrar filas de df_concat que contienen los nuevos business_id
            nuevos_datos = df_concat[df_concat['business_id'].isin(nuevos_business_ids)]          

            # Concatenar los nuevos datos al DataFrame df_unique_business_ids, manteniendo solo la columna "business_id"
            df_unique_business_ids = pd.concat([df_unique_business_ids, nuevos_datos[['business_id']]], ignore_index=True)

            # Eliminar duplicados en el dataframe df_unique_business_ids
            df_unique_business_ids = df_unique_business_ids.drop_duplicates(subset=['business_id'])

            # Guardar el dataframe actualizado en el archivo CSV

            ut.save_in_storage(bucket,"used_ids/business_ids.csv",df_unique_business_ids)
            

            pass
        elif tipo == "review":          
            # Dejamos solo las columnas relevantes para el analisis
            columnas_a_conservar = ['business_id','stars','text','date']
            df_nuevo = df_nuevo.loc[:, columnas_a_conservar]

            # Realizar la comprobación de ids existentes, no agregamos reseñas que tengas id's de negocios que no existan en la base de antemano
            df_nuevo = df_nuevo[df_nuevo['business_id'].isin(df_unique_business_ids['business_id'])]

            # Eliminar filas con valores nulos en algunas columnas de importancia
            columnas_clave = ['business_id', 'text']
            df_nuevo = df_nuevo.loc[df_nuevo[columnas_clave].notnull().all(axis=1)]

            # Imputar valores nulos en el DataFrame
            df_nuevo['date'] = df_nuevo['date'].fillna(pd.NaT)
            df_nuevo['stars'] = df_nuevo['stars'].fillna(np.nan)

            if df_base is not None:
                # Concatenar dataframe base con dataframe nuevo
                df_concat = concatenar_dataframes(df_base, df_nuevo)
            else:    
                df_concat = df_nuevo

            # Eliminar filas duplicadas, conservando la entrada con la fecha más reciente en caso de duplicados en review_id
            df_concat = df_concat.sort_values('date', ascending=False).drop_duplicates('review_id')

            # Agregar analisis de sentimiento
            df_concat = agregar_puntajes_sentimiento(df_concat)

            # Resetear los índices después de las operaciones
            df_concat = df_concat.reset_index(drop=True)

            pass

        elif tipo == "checkin":            

            # Eliminar filas con valores nulos en business_id o date
            df_nuevo = df_nuevo.loc[(df_nuevo['business_id'].notnull()) & (df_nuevo['date'].notnull())]

            # Realizar la comprobación de ids únicos
            df_nuevo = df_nuevo[df_nuevo['business_id'].isin(df_unique_business_ids['business_id'])]

            # Agregamos una nueva columna 'count' al DataFrame
            df_nuevo['count'] = df_nuevo['date'].apply(calculate_count)

            if df_base is not None:
                # Concatenar dataframe base con dataframe nuevo
                df_concat = concatenar_dataframes(df_base, df_nuevo)
            else:    
                df_concat = df_nuevo
            # Ordenar por business_id y 'count' en orden descendente para mantener la fila con la lista más larga
            df_concat = df_concat.sort_values(by=['business_id', 'count'], ascending=[True, False])

            # Mantener la primera fila para cada business_id (la más larga)
            df_concat = df_concat.drop_duplicates(subset='business_id', keep='first')          

            pass     

        return df_concat

def concatenar_dataframes(df_base, df_nuevo):
    # Concatena los DataFrames
    df_concat = pd.concat([df_base, df_nuevo], ignore_index=True)
    
    # Elimina las duplicaciones
    df_concat = df_concat.drop_duplicates()

    return df_concat
        

def generar_sentimiento(df):
    return True

def calculate_count(date_string):
    date_list = date_string.split(', ')
    return len(date_list) if date_list != [''] else 0
    

def procesar_google(df_base,df_nuevo,df_unique_business_ids,bucket,tipo):        

        # Casos para hacer cosas en función del parámetro
        if tipo == "sitio":
            # Dejamos solo las columnas relevantes para el analisis
            columnas_a_conservar = ["gmap_id","name","address","latitude","longitude","category","avg_rating","num_of_reviews","price","city","state"]
            df_nuevo = df_nuevo.loc[:, columnas_a_conservar]

            # Definir criterio de integridad de fila 70% de fila debe tener datos no nulos
            threshold = int(0.7 * len(df_nuevo.columns))

            # Eliminar filas que no cumplen con el criterio de integridad
            df_nuevo = df_nuevo.dropna(thresh=threshold)

            # Reemplazar nulos en columnas categóricas con "sin datos"
            df_nuevo.loc[:, ['name', 'address']] = df_nuevo.loc[:,  ['name', 'address']].fillna('sin datos')

            # Transformar los valores de la columna price (cantidad de signos a numero)
            #Aplica la función a la columna 'price' del dataframe
            df_nuevo['price'] = df_nuevo['price'].apply(contar_signos)

            # Eliminar filas con valores nulos en columnas clave
            df_nuevo = df_nuevo.dropna(subset=['gmap_id', 'avg_rating', 'category',"latitude","longitude","num_of_reviews"])            


            # Redefinir las categorias y dejar una sola por fila en la columna "category"
            df_nuevo = ut.filtrar_por_categoria_google(df_nuevo)
         
            # Se renombran las columnas para unificarlas con el dataset de yelp
            df_nuevo.rename(columns={'num_of_reviews': 'review_count', 'gmap_id': 'business_id', 'avg_rating':'stars'}, inplace=True)

            # Concatenar dataframe base con dataframe nuevo            
            if df_base is not None:
                # Concatenar dataframe base con dataframe nuevo
                df_concat = concatenar_dataframes(df_base, df_nuevo)
            else:    
                df_concat = df_nuevo

                        
            # Eliminar filas duplicadas basadas en 'business_id' dejando solo la que mayor cantidad de reviews tenga
            df_concat = df_concat.loc[df_concat.groupby('business_id')['review_count'].idxmax()]
           
                       
            # Eliminamos duplicados si se mezclaron datos de distintas fuentes mediante coordenadas y nombre del negocio
            df_concat = eliminar_duplicados_distintas_fuentes(df_concat)

            # Redondea estrellas en incrementos de 0.5 
            df_concat['stars'] = df_concat['stars'].apply(lambda x: round(x * 2) / 2)

            # Resetear los índices después de las operaciones            
            df_concat = df_concat.reset_index(drop=True)
            
            # Identificar business_id únicos presentes en df_concat pero no en df_unique_business_ids
            nuevos_business_ids = set(df_concat['business_id']) - set(df_unique_business_ids['business_id'])

            # Filtrar filas de df_concat que contienen los nuevos business_id
            nuevos_datos = df_concat[df_concat['business_id'].isin(nuevos_business_ids)]            

            # Concatenar los nuevos datos al DataFrame df_unique_business_ids, manteniendo solo la columna "business_id"
            df_unique_business_ids = pd.concat([df_unique_business_ids, nuevos_datos[['business_id']]], ignore_index=True)


            # Eliminar duplicados en el dataframe df_unique_business_ids
            df_unique_business_ids = df_unique_business_ids.drop_duplicates(subset=['business_id'])

            # Guardar el dataframe actualizado en el archivo CSV

            ut.save_in_storage(bucket,"used_ids/business_ids.csv",df_unique_business_ids)
            

            pass
        elif tipo == "review":          
            # Dejamos solo las columnas relevantes para el analisis
            columnas_a_conservar = ["time", "rating", "text", "gmap_id"]
            df_nuevo = df_nuevo.loc[:, columnas_a_conservar]
        
            # Realizar la comprobación de ids existentes, no agregamos reseñas que tengas id's de sitios que no existan en la base de antemano
            df_nuevo = df_nuevo[df_nuevo['gmap_id'].isin(df_unique_business_ids['business_id'])]

            # Eliminar filas con valores nulos en algunas columnas de importancia
            columnas_clave = ['gmap_id', 'text']
            df_nuevo = df_nuevo.loc[df_nuevo[columnas_clave].notnull().all(axis=1)]
                 
       
            df_nuevo['rating'] = df_nuevo['rating'].fillna(np.nan)

            # Se renombran las columnas para unificarlas con el dataset de yelp
            df_nuevo.rename(columns={'time': 'date', 'gmap_id': 'business_id', 'rating':'stars'}, inplace=True)

            if df_base is not None:
                # Concatenar dataframe base con dataframe nuevo
                df_concat = concatenar_dataframes(df_base, df_nuevo)
            else:                                 
                df_concat = df_nuevo            

            print(df_concat['date'])    
            # Filtrar filas por fechas validas y convertir fecha Unix en Pd.time
            df_concat = ut.filtrar_fechas_validas(df_concat)
            print(df_concat['date'])   

            print(df_concat['business_id'])   
            # Eliminar filas duplicadas, conservando la entrada con la fecha más reciente en caso de duplicados en review_id
            df_concat = df_concat.sort_values('date', ascending=False).drop_duplicates('business_id')
           
            # Agregar analisis de sentimiento
            df_concat = agregar_puntajes_sentimiento(df_concat)            

            # Resetear los índices después de las operaciones
            df_concat = df_concat.reset_index(drop=True)

        return df_concat


def agregar_puntajes_sentimiento(df):
    # Descargar recursos de NLTK si no están disponibles
    import nltk
    nltk.download('stopwords')
    nltk.download('wordnet')
    nltk.download('vader_lexicon')
    
    # Crear una copia del DataFrame para evitar modificar el original
    df_copy = df.copy()
    
    # Convertir texto a minúsculas
    df_copy["text"] = df_copy["text"].str.lower()

    # Eliminar puntuación y símbolos innecesarios
    df_copy["text"] = df_copy["text"].str.replace("[^\w\s]", "")

    # Eliminar stopwords 
    stop_words = stopwords.words('english')
    df_copy["text"] = df_copy["text"].apply(lambda x: ' '.join([word for word in x.split() if word not in stop_words]))

    # Lematización 
    lemmatizer = WordNetLemmatizer()
    df_copy["text"] = df_copy["text"].apply(lambda x: ' '.join([lemmatizer.lemmatize(word) for word in x.split()]))

    # Análisis de sentimiento
    analyzer = SentimentIntensityAnalyzer()
    df_copy["sentiment"] = df_copy["text"].apply(analyzer.polarity_scores)
    
    # Agregar columnas de puntajes de sentimiento
    df_copy["pos"] = df_copy["sentiment"].apply(lambda x: x["pos"])
    df_copy["neg"] = df_copy["sentiment"].apply(lambda x: x["neg"])
    df_copy["neu"] = df_copy["sentiment"].apply(lambda x: x["neu"])
    
    # Eliminar columnas adicionales si no son necesarias
    df_copy.drop(["sentiment"], axis=1, inplace=True)
    
    return df_copy

# Define una función para contar el número de signos de pesos en una cadena
def contar_signos(cadena):
    if "$" in cadena or "₩" in cadena:
        return cadena.count("$") + cadena.count("₩")
    else:
        return np.nan

from Levenshtein import distance
from scipy.spatial.distance import cdist
import Levenshtein

def calculate_distance(df):
    try:
        # Convierte las coordenadas a formato numérico
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        
        # Elimina filas con coordenadas que no se pudieron convertir a numérico
        df = df.dropna(subset=['latitude', 'longitude'])
        
        # Calcula la distancia entre cada par de coordenadas
        coords = df[['latitude', 'longitude']].values
        distance_matrix = cdist(coords, coords, metric='euclidean')
        return distance_matrix
    except Exception as e:     
        return None


def levenshtein_similarity(s1, s2):
    # Calcula la distancia de Levenshtein entre dos cadenas y la normaliza
    distance = Levenshtein.distance(s1.lower(), s2.lower())
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 1.0
    else:
        return 1 - distance / max_len

def eliminar_duplicados_distintas_fuentes(df, threshold_distance=0.01, threshold_similarity=0.8):
    # Calcula la distancia entre las coordenadas
    distance_matrix = calculate_distance(df)
    
    # Encuentra índices de duplicados y los elimina
    duplicates = set()
    for i in range(len(df)):
        for j in range(i + 1, len(df)):
            if distance_matrix[i, j] <= threshold_distance:
                similarity = levenshtein_similarity(df.iloc[i]['name'], df.iloc[j]['name'])
                if similarity >= threshold_similarity:
                    if df.iloc[i]['review_count'] > df.iloc[j]['review_count']:
                        duplicates.add(j)
                    else:
                        duplicates.add(i)
    
    # Elimina filas duplicadas   
    df_cleaned = df.drop(index=duplicates)
    return df_cleaned
