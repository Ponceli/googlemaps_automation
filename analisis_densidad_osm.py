import os
import sys
import requests
import pandas as pd
from databricks.connect import DatabricksSession

# Forzar utf-8 para salida directa
sys.stdout = open('output_osm.txt', 'w', encoding='utf-8')
sys.stderr = sys.stdout

# --- CONFIGURACIÓN ---
DIRECCION = "Avenida Eva Peron, Parque Chacabuco, Buenos Aires, Argentina"
RADIO_METROS = 500

print(f"🔍 Geocodificando dirección: {DIRECCION} ...")
# Geocodificación Gratuita con Nominatim de OSM
headers = {'User-Agent': 'DatabricksAgent/1.0'}
geocode_url = f"https://nominatim.openstreetmap.org/search?q={requests.utils.quote(DIRECCION)}&format=json&limit=1"
r = requests.get(geocode_url, headers=headers)
if r.status_code == 200 and len(r.json()) > 0:
    location = r.json()[0]
    LAT, LON = float(location['lat']), float(location['lon'])
else:
    print("❌ No se pudo encontrar la coordenada. Usando default de Parque Chacabuco.")
    LAT, LON = -34.6339, -58.4358 # Aproximación manual a Parque Chacabuco

print(f"🌍 Analizando zona en ({LAT}, {LON}) con radio de {RADIO_METROS}m...")

# ==========================================
# 1. OPENSTREETMAP API (CÁLCULO DE DENSIDAD)
# ==========================================
print("\n📡 Consultando Overpass API (OpenStreetMap)...")
overpass_url = "https://overpass-api.de/api/interpreter"
overpass_query = f"""
[out:json];
node
  ["amenity"="restaurant"]
  (around:{RADIO_METROS},{LAT},{LON});
out;
"""

response = requests.post(overpass_url, data={'data': overpass_query})
if response.status_code == 200:
    osm_data = response.json()
    elementos = osm_data.get('elements', [])
    print(f"✅ Se encontraron {len(elementos)} restaurantes en la zona.")
    
    # Extraer datos en una lista para pandas
    negocios = []
    for el in elementos:
        tags = el.get('tags', {})
        negocios.append({
            "id_osm": el.get('id'),
            "nombre": tags.get('name', 'Desconocido'),
            "tipo": tags.get('amenity', 'restaurant'),
            "lat": el.get('lat'),
            "lon": el.get('lon'),
            "calle": tags.get('addr:street', 'Sin dirección'),
        })
    
    # Crear DataFrame local
    pdf_negocios = pd.DataFrame(negocios)
    
    print("\n📊 Densidad de negocios por calle (Top 5):")
    # Agrupamos los negocios que SÍ tienen nombre de calle registrado
    densidad_calles = pdf_negocios[pdf_negocios['calle'] != 'Sin dirección']['calle'].value_counts().head(5)
    print(densidad_calles)
    
    # ==========================================
    # 2. PROCESAMIENTO DISTRIBUIDO EN DATABRICKS
    # ==========================================
    sys_stdout_bak = sys.stdout # backup
    print("\n☁️ Enviando datos de densidad a Databricks Serverless para guardar...")
    try:
        spark = DatabricksSession.builder.serverless().getOrCreate()
        sdf_negocios = spark.createDataFrame(pdf_negocios)
        
        print("Registros procesados en Spark:")
        sdf_negocios.select("nombre", "calle", "lat", "lon").show(5, truncate=False)
        
        # Opcional: Aquí se podría grabar en Unity Catalog (ej. ds.write.saveAsTable("silver.trafico_peatonal"))
    except Exception as e:
        print(f"No se pudo completar la subida a Databricks: {str(e)}")

else:
    print(f"❌ Error en Overpass API: HTTP {response.status_code}")

print("\n==========================================")
print("  DATO ADICIONAL: GOOGLE POPULAR TIMES    ")
print("==========================================")
print("Para calcular los picos de visitas exactos de cada negocio (Google Popularity),")
print("debemos usar el método que vimos cruzando los IDs:\n")
print('import populartimes')
print('data = populartimes.get_id(tu_api_key_de_google, "place_id_del_negocio")')
print('print(data["populartimes"])')
print("\nNOTA: Este paso requiere contar con una API Key de Google Maps válida en tú archivo .env o usar text-search.")
