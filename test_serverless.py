# Databricks notebook source
# COMMAND ----------
print("=== Ejecutando Serverless en Python ===")

# COMMAND ----------
# Listar archivos en la raíz usando dbutils de Python
print("Archivos en la raíz del DBFS:")
archivos = dbutils.fs.ls("/")
display(archivos)

# COMMAND ----------
# Ejecutar consulta SQL puro usando Spark
print("Mostrando bases de datos disponibles:")
databases = spark.sql("SHOW DATABASES")
display(databases)
