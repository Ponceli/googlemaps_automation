import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Inicializar cliente del workspace de Databricks
try:
    # Esto tomará automáticamente DATABRICKS_HOST y DATABRICKS_TOKEN
    w = WorkspaceClient()
    
    # Listar los clusters disponibles para verificar conexión
    print("Conexión exitosa. Mostrando algunos clusters:")
    for c in w.clusters.list():
        print(f" - {c.cluster_name} (ID: {c.cluster_id})")
        break
except Exception as e:
    print(f"Error al conectar con Databricks: {e}")
