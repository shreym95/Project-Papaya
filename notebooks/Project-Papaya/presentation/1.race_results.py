# Databricks notebook source
# MAGIC %run ../includes/configs

# COMMAND ----------

dbutils.notebook.run("../includes/utility_functions", 0)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")
display(races_df)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder}/circuits") \
.withColumnRenamed("name", "circuit_name") \
.withColumnRenamed("location", "circuit_location")
display(circuits_df)

# COMMAND ----------

#Adding circuit name column to races_df using circuits_df
races_df = races_df.join(circuits_df, "circuit_id") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_name, circuits_df.circuit_location)
display(races_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder}/drivers") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("nationality", "driver_nationality")
display(drivers_df)

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder}/constructors") \
.withColumnRenamed("name", "team")
display(constructors_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder}/results") \
.withColumnRenamed("time", "race_time")
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining the dataframes to get the data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = results_df.join(races_df, "race_id") \
.join(drivers_df, "driver_id") \
.join(constructors_df, "constructor_id") \
.select(races_df.race_year, races_df.race_name, races_df.race_date, races_df.circuit_name, races_df.circuit_location, drivers_df.driver_name, drivers_df.driver_number, drivers_df.driver_nationality, constructors_df.team, results_df.grid, results_df.fastest_lap, results_df.race_time, results_df.points) \
.withColumn("creation_date", current_timestamp())

# COMMAND ----------

display(final_df.filter((final_df.race_year == 2020) & (final_df.race_name == "Abu Dhabi Grand Prix")).orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder}/race_results")