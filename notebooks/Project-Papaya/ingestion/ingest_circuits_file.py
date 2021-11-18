# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file
# MAGIC 
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

#Adding widgets to pass data source at runtime
dbutils.widgets.text("file_date", "")
file_date = dbutils.widgets.get("file_date")
dbutils.widgets.text("data_source", "")
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

# MAGIC %run ../includes/configs

# COMMAND ----------

# MAGIC %run ../includes/utility_functions

# COMMAND ----------

#Importing necessary modules

#1. Importing data types from pyspark.sql.types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

#2. Importing col function from pyspark.sql.functions to select specific columns
from pyspark.sql.functions import col

#3. Importing current_timestamp from pyspark.sql.functions to add a timestamp column
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#Defining the schema before file is read (similar to SQL), as opposed to inferring schema directly through the file
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

#Reading the file as per defined schema, using spark.read.csv, and using options() to define read parameters
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder}/{file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuits_df_temp = add_ingestion_date(circuits_renamed_df)#.withColumn("ingestion_date", current_timestamp()) 
circuits_final_df = add_data_source(circuits_df_temp, data_source)
circuits_final_df = circuits_final_df.withColumn("file_date", lit(file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(spark.read.parquet("/mnt/wtf1dl/processed/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM f1_processed.circuits;

# COMMAND ----------

#Adding exit code for successful run of the notebook
dbutils.notebook.exit("200")