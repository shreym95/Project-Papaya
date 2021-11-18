# Databricks notebook source
v_result = dbutils.notebook.run("ingest_circuits_file", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_races_file", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_constructors_file", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_drivers_file", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_results_file", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_pit_stops_file", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_lap_times_file", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_qualifying_file", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result