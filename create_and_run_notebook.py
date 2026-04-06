import os
import time
import base64
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.jobs import (
    NotebookTask,
    RunLifeCycleState,
    Source,
)

load_dotenv()

w = WorkspaceClient()

# ---- 1. Crear la notebook en el workspace ----
notebook_path = "/Users/" + w.current_user.me().user_name + "/test_serverless_notebook"

# Contenido de la notebook: un comando simple %fs ls /
notebook_content = """# Databricks notebook source
# MAGIC %md
# MAGIC # Test Notebook - Serverless
# MAGIC Este notebook fue creado automáticamente para verificar la conexión.

# COMMAND ----------

# Listar archivos en el root del DBFS
dbutils.fs.ls("/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Conexión exitosa desde serverless!' AS mensaje, current_timestamp() AS timestamp
"""

# Codificar contenido en base64
content_b64 = base64.b64encode(notebook_content.encode("utf-8")).decode("utf-8")

print(f"Creando notebook en: {notebook_path}")
w.workspace.import_(
    path=notebook_path,
    content=content_b64,
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True,
)
print("Notebook creada exitosamente.")

# ---- 2. Ejecutar la notebook con serverless ----
print("\nEjecutando notebook con serverless compute...")
from databricks.sdk.service.jobs import SubmitTask

run = w.jobs.submit(
    run_name="test_serverless_run",
    tasks=[
        SubmitTask(
            task_key="test_task",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                source=Source.WORKSPACE,
            ),
            # No especificamos cluster -> usa serverless automáticamente
        )
    ],
)

run_id = run.run_id
print(f"Run iniciado con ID: {run_id}")

# ---- 3. Esperar a que termine ----
print("Esperando a que termine la ejecución...")
while True:
    run_status = w.jobs.get_run(run_id=run_id)
    state = run_status.state.life_cycle_state
    print(f"  Estado: {state}")

    if state in [
        RunLifeCycleState.TERMINATED,
        RunLifeCycleState.INTERNAL_ERROR,
        RunLifeCycleState.SKIPPED,
    ]:
        break
    time.sleep(10)

# ---- 4. Mostrar resultado ----
result_state = run_status.state.result_state
print(f"\nResultado final: {result_state}")

if str(result_state) == "RunResultState.SUCCESS":
    print("La notebook se ejecutó correctamente en serverless.")
    print(f"\nPuedes verla en Databricks:")
    print(f"  Notebook: {notebook_path}")
    print(f"  Run URL:  {run_status.run_page_url}")
else:
    print(f"La ejecución falló. Estado: {result_state}")
    if run_status.state.state_message:
        print(f"Mensaje: {run_status.state.state_message}")
    print(f"Run URL: {run_status.run_page_url}")
