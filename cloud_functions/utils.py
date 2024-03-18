import ast
from google.oauth2 import service_account
from google.cloud import storage
import pandas as pd
import reverse_geocoder as rg
import io
import etl_functions as etl

def get_bucket(bucket_name):    
    
    # Crear cliente de almacenamiento con las credenciales
    client = storage.Client()
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
    
def descargar_archivo_gcs(bucket, ruta_archivo):
    # Obtener el blob (archivo) dentro del bucket
    blob = bucket.blob(ruta_archivo)

    # Descargar el contenido del archivo como bytes
    contenido_bytes = blob.download_as_bytes()

    # Convertir bytes a DataFrame
    try:
        # Decodificar bytes a cadena
        contenido_str = contenido_bytes.decode('utf-8')
        
        # Determinar el tipo de archivo basado en la extensión
        if ruta_archivo.endswith('.csv'):
            df = pd.read_csv(io.StringIO(contenido_str))
        elif ruta_archivo.endswith('.json'):
            try:
                df = pd.read_json(io.StringIO(contenido_str))
            except  Exception as e:
                df = pd.read_json(io.StringIO(contenido_str),lines=True)
        elif ruta_archivo.endswith('.pkl'):
            df = pd.read_pickle(io.BytesIO(contenido_bytes))
        else:
            print("Formato de archivo no compatible:", ruta_archivo)
            return None
        
        return df
    except Exception as e:
        print("Error al leer el archivo:", e)
        return None 

    
def obtener_data_archivo_a_actualizar(bucket, ruta):  
    try:
        blob = bucket.blob(ruta)
        if blob.exists():            
            data = blob.download_as_bytes()
            return data
        else:
            return None
    except Exception:
        return None
    
def asignar_tipo_archivo(ruta):
    if "yelp" in ruta:
        if "business" in ruta:
            tipo_archivo = "business"
        elif "checkin" in ruta:
            tipo_archivo = "checkin"
        elif "review" in ruta:
            tipo_archivo = "review"
        elif "sitio" in ruta:
            tipo_archivo = "sitio"
        else:
            tipo_archivo = None
    elif "google" in ruta:
        if "review" in ruta:
            tipo_archivo = "review"
        elif "sitio" in ruta:
            tipo_archivo = "sitio"
        else:
            tipo_archivo = None
    else:
        tipo_archivo = None

    return tipo_archivo

def corregir_ubicaciones(df):
    # Aplicar la función obtener_ubicacion a las columnas latitude y longitude
    df['ubicacion'] = df.apply(lambda row: obtener_ubicacion(row['latitude'], row['longitude']), axis=1)
    
    # Actualizar los campos 'state' y 'city' basados en las nuevas columnas generadas
    df['state'] = df['ubicacion'].apply(lambda x: x['estado'])
    df['city'] = df['ubicacion'].apply(lambda x: x['ciudad'])
    
    # Eliminar la columna 'ubicacion' generada auxiliar
    df = df.loc[:, df.columns != 'ubicacion']
    
    return df


# Función para obtener estado y ciudad a partir de latitud y longitud
def obtener_ubicacion(latitud, longitud):
    try:
        # Obtener información de ubicación utilizando Reverse Geocoder
        results = rg.search((latitud, longitud))

        # Extraer el estado y la ciudad de los resultados
        estado = results[0]['admin1']
        ciudad = results[0]['name']

        return {"estado": estado, "ciudad": ciudad}
    except Exception:
        # Si no se encuentra la ubicación, devolver valores vacíos
        return {"estado": "sin datos", "ciudad": "sin datos"}
    
def filtrar_por_categoria(df):
    # Agregar una nueva columna llamada 'category'
    df['category'] = ''

    # Iterar sobre cada fila del DataFrame
    for index, row in df.iterrows():
        categories_list = row['categories']
        if isinstance(categories_list, str):  # Verificar si 'categories' es una cadena
            categories_list = categories_list.split(', ')
            # Verificar si 'Restaurants' está en la lista de categorías
            if 'Restaurants' in categories_list or 'Pop-Up Restaurants' in categories_list:
                df.at[index, 'category'] = 'Restaurant'
            # Verificar si 'Hotels & Travel' o 'Hotels' están en la lista de categorías
            if 'Hotels & Travel' in categories_list or 'Hotels' in categories_list:
                df.at[index, 'category'] = 'Hotel'

    # Eliminar la columna 'categories'
    df.drop(columns=['categories'], inplace=True)

    # Eliminar filas donde 'category' no sea ni 'Hotel' ni 'Restaurant'
    df = df[df['category'].isin(['Hotel', 'Restaurant'])]

    return df

def tratar_valores_nulos_y_normalizar(df):
    # Rellenar los valores nulos con "Sin datos" en las columnas relevantes
    columnas_con_nulos = ['NoiseLevel', 'BusinessAcceptsCreditCards']
    df[columnas_con_nulos] = df[columnas_con_nulos].replace('None', 'sin datos')
    df[columnas_con_nulos] = df[columnas_con_nulos].fillna('sin datos')

    # Eliminar el prefijo 'u' en la columna 'NoiseLevel'
    df['NoiseLevel'] = df['NoiseLevel'].str.replace("u'", "").str.replace("'", "")

    # Estandarizar los valores y tratar los nulos como "sin datos"
    df['WiFi'] = df['WiFi'].fillna('Sin datos').apply(lambda x: 'Free' if 'free' in x else 'Paid' if 'paid' in x else 'No')

    # Modificar la columna 'BusinessParking' para que sea True si al menos un tipo de estacionamiento es verdadero
    def parse_business_parking(x):
        try:
            x_dict = ast.literal_eval(x)
            if isinstance(x_dict, dict):
                return any(value == True for value in x_dict.values())
        except (SyntaxError, ValueError):
            pass
        return False

    df['BusinessParking'] = df['BusinessParking'].fillna(False).apply(parse_business_parking)

    # Reemplazar los valores None y "None" por "sin dato" en las columnas especificadas
    df['RestaurantsDelivery'] = df['RestaurantsDelivery'].fillna('sin datos').replace('None', 'sin datos')
    df['HasTV'] = df['HasTV'].fillna('sin datos').replace('None', 'sin datos')
    df['RestaurantsTakeOut'] = df['RestaurantsTakeOut'].fillna('sin datos').replace('None', 'sin datos')

    return df

def generar_atributos(df):
    # Definir una función para obtener las claves de primer nivel
    def get_first_level_keys(attr):
        try:
            attr_dict = ast.literal_eval(attr)
            if isinstance(attr_dict, dict):
                return attr_dict
            else:                
                return {}
        except (SyntaxError, ValueError):            
            return {}
    
    # Lista de claves de interés
    keys_of_interest = ['NoiseLevel', 'BusinessParking', 'BusinessAcceptsCreditCards', 'WiFi', 'RestaurantsDelivery', 'HasTV', 'RestaurantsTakeOut']

    # Generar nuevas columnas basadas en las claves de interés
    for key in keys_of_interest:
        df[key] = df['attributes'].apply(lambda attr: get_first_level_keys(attr).get(key, None))

    # Llamar a la función de tratamiento de valores nulos y normalización
    df = tratar_valores_nulos_y_normalizar(df)

    # Eliminar la columna 'attributes' original
    df.drop(columns=['attributes'], inplace=True)

    df.columns = df.columns.str.lower()

    # Renombrar las columnas por nombres más cortos y entendibles
    df.rename(columns={
        'noislevel': 'noise_level',
        'businessacceptscreditcards': 'accepts_credit_cards',       
        'restaurantsdelivery': 'restaurant_delivery',
        'hastv': 'has_tv',
        'restaurantstakeout': 'restaurant_takeout',
        'businessparking': 'parking'
    }, inplace=True)

    return df


def obtener_estado(nombre_archivo):
    if "florida" in nombre_archivo.lower():
        return "Florida"
    elif "nevada" in nombre_archivo.lower():
        return "Nevada"
    elif "california" in nombre_archivo.lower():
        return "California"
    else:
        return False

def filtrar_por_categoria_google(df):
  """
  Filtra un dataframe por categoría de Google y elimina filas sin categoría.

  Parámetros:
    df: Un dataframe con una columna "category" que contiene listas de strings.

  Retorno:
    Un dataframe con la columna "category" reemplazada por "Restaurant" o "Hotel" 
    dependiendo de la presencia de esas palabras en la lista. Se eliminan las filas
    que no contienen ninguna de las dos palabras.
  """

  df["category"] = df["category"].str.lower() # Convertir a minúsculas

  # Función para determinar la categoría
  def determinar_categoria(categorias):
    if "restaurant" in categorias or "restaurante" in categorias:
      return "Restaurant"
    elif "hotel" in categorias:
      return "Hotel"
    else:
      return None

  # Aplicar la función a cada fila
  df["category"] = df["category"].apply(determinar_categoria)

  # Filtrar por filas con categoría
  df = df[df["category"].notnull()]

  return df
  
def filtrar_fechas_validas(df, date_column='date', min_date='1970-01-01', max_date='2038-01-19'):
    # Función para convertir fechas Unix en milisegundos a formato datetime
    def convertir_fecha_unix(fecha):
        try:
            return pd.to_datetime(fecha, unit='ms', origin='unix')
        except:
            return pd.NaT  # Retorna NaT si hay un error en la conversión
    
    # Convertir las fechas en la columna especificada a formato datetime
    df[date_column] = df[date_column].apply(convertir_fecha_unix)
    
    # Filtrar las fechas dentro del rango especificado
    df_filtrado = df[(df[date_column] >= min_date) & (df[date_column] <= max_date)]
    
    return df_filtrado

def verificar_tamanio_dataframe(bucket, blob_name, max_tamanio_mb):    
 
    # Leer el DataFrame desde el buffer
    dataframe = descargar_archivo_gcs(bucket,blob_name)
    
    # Calcular el tamaño del DataFrame en megabytes
    tamanio_mb = dataframe.memory_usage(deep=True).sum() / (1024 ** 2)
    
    # Verificar si el tamaño del DataFrame es menor que el máximo especificado
    if tamanio_mb < max_tamanio_mb:
        return True
    else:
        return False