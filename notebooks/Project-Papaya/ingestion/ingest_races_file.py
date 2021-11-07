# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file
# MAGIC 
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

#Importing necessary modules

#1. Importing data types from pyspark.sql.types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

#2. Importing col function from pyspark.sql.functions to select specific columns
from pyspark.sql.functions import col

#3. Importing current_timestamp from pyspark.sql.functions to add a timestamp column
from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

#Defining the schema before file is read (similar to SQL), as opposed to inferring schema directly through the file
race_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", StringType(), True),
                                     StructField("time", StringType(), True),                     
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

#Reading the file as per defined schema, using spark.read.csv, and using options() to define read parameters
races_df = spark.read \
.option("header", True) \
.schema(race_schema) \
.csv("/mnt/wtf1dl/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required, add ingestion date, and create race_timestamp column

# COMMAND ----------

#Adding a race timestamp column from date and time, and an ingestion date column for auditing
races_final_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
.drop("date", "time")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("/mnt/wtf1dl/processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/wtf1dl/processed/races"))