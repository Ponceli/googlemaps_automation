import os
import sys
import json
import pandas as pd

# Forzar utf-8
sys.stdout = open('output_barista.txt', 'w', encoding='utf-8')
sys.stderr = sys.stdout

from dotenv import load_dotenv
import populartimes
from databricks.connect import DatabricksSession

# 1. Cargar variables de entorno
load_dotenv()
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
PLACE_ID = "ChIJ1zW6in_LvJURopHjKrC-zGg"

if not API_KEY:
    print("❌ ERROR: Falta configurar GOOGLE_MAPS_API_KEY en el archivo .env")
    exit(1)

print(f"Buscando información para el local (Place ID): {PLACE_ID}")
try:
    # 2. Obtener datos de Popular Times
    resultado = populartimes.get_id(API_KEY, PLACE_ID)
    
    # 3. Mostrar resumen básico
    print(f"\n✅ Datos obtenidos exitosamente para: {resultado.get('name')}")
    print(f"Ubicación: {resultado.get('address')}")
    print(f"Rating: {resultado.get('rating')} (basado en {resultado.get('rating_n')} reseñas)")
    
    # 4. Procesar los horarios populares
    pop_times = resultado.get('populartimes', [])
    if not pop_times:
         print("Este negocio no tiene horarios populares registrados.")
    else:
        # Convertir a un DataFrame de Pandas estructurado
        data = []
        for day_data in pop_times:
            day_name = day_data['name']
            data_list = day_data['data']
            # Iterar cada hora para aplanarlo
            for hour, popularity in enumerate(data_list):
                data.append({
                    "negocio": resultado.get('name'),
                    "dia": day_name,
                    "hora": hour,
                    "popularidad": popularity
                })
        
        pdf = pd.DataFrame(data)
        
        # Iniciar sesión de Spark Serverless
        print("\nLevantando sesión de Databricks Serverless para guardar los datos...")
        spark = DatabricksSession.builder.serverless().getOrCreate()
        
        # Convertir Pandas DF a Spark DF
        sdf = spark.createDataFrame(pdf)
        
        print("\n📊 Muestra de los datos procesados en Spark:")
        sdf.show(5)
        
        # Mostrar conteo total y métricas
        print("Resumen de métricas distribuidas calculadas en Databricks:")
        sdf.describe("popularidad").show()

except Exception as e:
    print(f"Se produjo un error al extraer los datos: {str(e)}")
