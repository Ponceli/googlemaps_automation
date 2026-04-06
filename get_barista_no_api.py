import sys
# Forzar utf-8
sys.stdout = open('output_barista_no_api.txt', 'w', encoding='utf-8')
sys.stderr = sys.stdout

import populartimes.crawler as crawler
import pandas as pd
from databricks.connect import DatabricksSession

# Usar el crawler interno que NO usa la API Key, sino scraping directo del Knowledge Graph de Google Search.
# Le pasamos el nombre del local y un indicio de dirección para que la Búsqueda de Google lo encuentre.
nombre = "I am barista"
address = "Caballito, Buenos Aires"

print(f"🕵️ Intentando scraping directo 100% gratuito sin API Key para: {nombre}...")

# Llama a la función oculta de scraping
try:
    rating, rating_n, pop_times, raw_current_popularity, time_spent = crawler.get_populartimes_from_search(nombre, address)
    
    if not pop_times:
        print("❌ No se encontraron horarios populares en la gráfica de Google. Quizá el local es muy nuevo.")
    else:
        # Formatear a Pandas DF 
        print(f"✅ Scraping exitoso! {nombre} | Rating: {rating} ({rating_n} reseñas)")
        print("\nFormateando matriz de tráfico...")
        
        # get_populartimes_from_search retorna un formato ligeramente distinto que hay que limpiar
        # pop_times contiene una lista de los 7 días.
        import calendar
        dias_semana = list(calendar.day_name)
        
        data = []
        for d in range(7):
            try:
                horas_del_dia = pop_times[d]
                dia_nombre = dias_semana[d]
            except:
                continue
                
            if horas_del_dia:
                for tuple_info in horas_del_dia:
                    try:
                        hora = tuple_info[0]
                        popularidad = tuple_info[1]
                        data.append({
                            "negocio": nombre,
                            "dia": dia_nombre,
                            "hora": hora,
                            "popularidad": popularidad
                        })
                    except:
                        pass
        
        pdf = pd.DataFrame(data)
        
        print("\n☁️ Enviando tráfico a Databricks Serverless...")
        spark = DatabricksSession.builder.serverless().getOrCreate()
        sdf = spark.createDataFrame(pdf)
        
        print("\n📊 Muestra de Picos de Tráfico (Spark DataFrame):")
        sdf.filter(sdf.popularidad > 0).orderBy("popularidad", ascending=False).show(10)
        
except Exception as e:
    print(f"Error en el scraper: {str(e)}")
