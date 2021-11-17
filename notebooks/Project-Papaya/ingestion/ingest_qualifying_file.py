# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the json files using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), False),
                                      StructField("number", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                  ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json("/mnt/wtf1dl/raw/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("qualifyingId", "qualifying_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM F1_PROCESSED.QUALIFYING

# COMMAND ----------

display(spark.read.parquet("/mnt/wtf1dl/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("200")