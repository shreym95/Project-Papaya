# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce driver standings

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-28")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %run ../includes/configs

# COMMAND ----------

#Filter the race_results data as per the input data
race_results_df = spark.read.parquet(f"{presentation_folder}/race_results") \
.filter(f"result_file_date = '{file_date}'")

# COMMAND ----------

#Get race years for which the data needs to be reprocessed, as per the data received on given file_date
race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

#Get data for only those years for which data need to be reprocessed
from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM F1_PRESENTATION.DRIVER_STANDINGS

# COMMAND ----------

