import os
import sys
from dotenv import load_dotenv

# Redirigir stdout a utf-8 nativo de python
sys.stdout = open('output_results.txt', 'w', encoding='utf-8')
sys.stderr = sys.stdout

load_dotenv()
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

print("==== INIT DATABRICKS CONNECT ====")
try:
    spark = DatabricksSession.builder.serverless().getOrCreate()
    print("--- DATABASES ---")
    df = spark.sql("SHOW DATABASES")
    
    # Escribir cada row
    for row in df.collect():
        print(f"DATABASE: {row['databaseName']}")
        
    print("--- FILES IN / ---")
    w = WorkspaceClient()
    archivos = w.dbfs.list("/")
    for archivo in archivos:
        print(f"FILE: {archivo.path} | SIZE: {archivo.file_size}")
except Exception as e:
    print(f"ERROR: {str(e)}")
