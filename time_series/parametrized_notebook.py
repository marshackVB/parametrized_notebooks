# Databricks notebook source
# MAGIC %md ## Production parametrized notebook
# MAGIC This notebook performs simple data processing and returns the results as a Global Temporary View
# MAGIC 
# MAGIC #### Arguments:
# MAGIC **database** (string): the database containing the data to process  
# MAGIC **table_name** (string): the table containing the data to process  
# MAGIC **group_by_cols** (list(string): a pythong list of column names as strings  
# MAGIC **aggregations_type** (string): the type of aggregation to apply to each group  
# MAGIC **value_col** (string): the colum on which to apply the aggregation  
# MAGIC **results_table** (string): Then name of the global temporary table in which to store the notebook results

# COMMAND ----------

import pyspark.sql.functions as func
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md #### Create widgets

# COMMAND ----------

# To clear widgets
#dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("database", "global_temp_db")

dbutils.widgets.text("table", "")

dbutils.widgets.text("group_by_cols", "")

dbutils.widgets.text("value_col", "")

dbutils.widgets.dropdown("aggregation_type", "sum", ["sum", "count", "max", "min"])

dbutils.widgets.text("results_table", "")

# COMMAND ----------

# MAGIC %md #### Get widget values

# COMMAND ----------

database = dbutils.widgets.get("database")

table = dbutils.widgets.get("table")

group_by_cols = eval(dbutils.widgets.get("group_by_cols"))

value_col = dbutils.widgets.get("value_col")

aggregation_type = dbutils.widgets.get("aggregation_type")

results_table = dbutils.widgets.get("results_table")

# COMMAND ----------

# MAGIC %md #### Perform calculations

# COMMAND ----------

calc_mapping = {"sum":   func.sum,
                "count": func.count,
                "max":   func.max,
                "min":   func.min}

pyspark_aggregation = calc_mapping[aggregation_type]

df = spark.table(f"{database}.{table}")

transformation = (df.groupby(group_by_cols).agg(func.sum(f"{value_col}").alias(aggregation_type)))

# COMMAND ----------

# MAGIC %md #### Store results in temporary table

# COMMAND ----------

transformation.createOrReplaceGlobalTempView(results_table)
dbutils.notebook.exit(results_table)