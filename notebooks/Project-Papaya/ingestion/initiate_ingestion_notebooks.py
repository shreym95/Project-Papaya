# Databricks notebook source
# MAGIC %md
# MAGIC ## Single notebook to run all others for ingestion

# COMMAND ----------

exit_code = dbutils.notebook.run("ingest_circuits_file", 0, {"data_source": "Ergast API"})
print(exit_code)