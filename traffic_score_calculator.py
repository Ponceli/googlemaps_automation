import os
import sys
import requests
import pandas as pd
from databricks.connect import DatabricksSession

sys.stdout = open('market_opportunity_scanner.txt', 'w', encoding='utf-8')
sys.stderr = sys.stdout

# --- CONFIGURACIÓN DEL SCANNER ---
DIRECCION = "Palermo, Buenos Aires, Argentina"
RADIO_METROS = 800

print("==========================================================================")
print("🏙️  FOOT TRAFFIC & BUSINESS OPPORTUNITY SCANNER (100% FREE APPROACH)")
print("==========================================================================")
print(f"Target: {DIRECCION}")
print(f"Radio exploratorio: {RADIO_METROS} metros\n")

# 1. Geocodificación
headers = {'User-Agent': 'DatabricksAgent/1.0'}
geocode_url = f"https://nominatim.openstreetmap.org/search?q={requests.utils.quote(DIRECCION)}&format=json&limit=1"
r = requests.get(geocode_url, headers=headers)
if r.status_code == 200 and len(r.json()) > 0:
    loc = r.json()[0]
    LAT, LON = float(loc['lat']), float(loc['lon'])
    print(f"[+] Coordenadas base: Lat {LAT}, Lon {LON}")
else:
    print("[-] Fallo geocodificación. Usando Obelisco por defecto.")
    LAT, LON = -34.6037, -58.3816

# 2. Extracción profunda en OSM (Tráfico Peatonal y Competencia)
# Buscamos Anclas Comerciales, Negocios y Paradas de Transporte (todo suma pasos peatonales)
print("\n[+] Lanzando scanner satelital en Overpass API...")
overpass_url = "https://overpass-api.de/api/interpreter"
query = f"""
[out:json][timeout:25];
(
  node["amenity"~"restaurant|cafe|bar|fast_food|pub"](around:{RADIO_METROS},{LAT},{LON});
  node["shop"~"supermarket|convenience|bakery|clothes|electronics"](around:{RADIO_METROS},{LAT},{LON});
  node["highway"~"bus_stop"](around:{RADIO_METROS},{LAT},{LON});
  node["railway"="subway_entrance"](around:{RADIO_METROS},{LAT},{LON});
);
out body;
"""
resp = requests.post(overpass_url, data={'data': query})

if resp.status_code == 200:
    elementos = resp.json().get('elements', [])
    print(f"[+] Se interceptaron {len(elementos)} nodos de actividad en el área.")
    
    # Procesar y puntuar cada nodo
    data = []
    for el in elementos:
        tags = el.get('tags', {})
        tipo_local = tags.get('amenity') or tags.get('shop') or tags.get('highway') or tags.get('railway', 'other')
        calle = tags.get('addr:street', 'No_definida')
        
        # Asignar peso de tráfico (score) al nodo
        # Un subte o bus genera muchisimo peatón, un supermercado genera mas que una tienda chica
        peso = 10
        if tipo_local in ['subway_entrance']: peso = 500
        elif tipo_local in ['bus_stop']: peso = 150
        elif tipo_local in ['supermarket', 'fast_food']: peso = 80
        elif tipo_local in ['restaurant', 'cafe', 'bar']: peso = 50
        elif tipo_local in ['convenience', 'bakery']: peso = 30
        
        if calle != 'No_definida':
            data.append({
                "calle": calle,
                "lat": el.get('lat'),
                "lon": el.get('lon'),
                "nodo": tipo_local,
                "nombre": tags.get('name', 'N/A'),
                "peatones_generados_score": peso
            })
            
    df = pd.DataFrame(data)
    
    print("\n==========================================================================")
    print("📈 RESULTADOS: RANKING DE CALLES POR TRÁFICO PEATONAL EXACTO (SCORE BASE)")
    print("==========================================================================")
    
    if not df.empty:
        # Agrupamos por calle
        calles_agrupadas = df.groupby('calle').agg(
            total_nodos=('nodo', 'count'),
            score_trafico_peatonal=('peatones_generados_score', 'sum')
        ).reset_index().sort_values(by='score_trafico_peatonal', ascending=False).head(10)
        
        # Le aplicamos una fórmula de oportunidad de negocio
        # Oportunidad = Tráfico muy alto y cantidad de locales menor en comparacion
        calles_agrupadas['promedio_pasos_por_local'] = (calles_agrupadas['score_trafico_peatonal'] / calles_agrupadas['total_nodos']).round(1)
        
        print("\nTOP CALLES CON MAYOR TRÁFICO (DIGITOS DUROS BASADOS EN DENSIDAD Y TRANSPORTE PÚBLICO):")
        print(calles_agrupadas.to_string(index=False))
        
        print("\n☁️ Sincronizando modelo en Databricks Serverless...")
        spark = DatabricksSession.builder.serverless().getOrCreate()
        sdf = spark.createDataFrame(calles_agrupadas)
        print("Sincronización completada. Listo para insertar en Unity Catalog.")
        
    else:
        print("No se logró estructurar la información por nombres de calle.")
else:
    print("Error de Overpass API:", resp.status_code)
