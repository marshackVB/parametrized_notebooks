# Databricks notebook source
# MAGIC %md # Tests for parametrized notebook
# MAGIC A collection of tests cases that must pass for a parametrized notebook

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField

# COMMAND ----------

global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")

# COMMAND ----------

# MAGIC %md ### Create test datasets

# COMMAND ----------

# DataFrame schema
df_1_schema = [StructField('account',      IntegerType(), True),
               StructField('company_name', StringType(),  True),
               StructField('account_type', StringType(),  True),
               StructField('balance',      StringType(),  True)]

# Sample data
df_data = [[100, 200, 300],
           ['company_a', 'company_b', 'company_c'],
           ['a', 'b', 'c'],
           [1, 2, 3]]

# Create DataFrame
df_1 = spark.createDataFrame(zip(*df_data), schema = StructType(df_1_schema))

display(df_1)

# COMMAND ----------

# MAGIC %md ### Register test datasets as Global Temporary Views
# MAGIC This ensures that they are accesible from child notebooks that are called by this notebook

# COMMAND ----------

df_1.createOrReplaceGlobalTempView("df_1_table")

# COMMAND ----------

# MAGIC %md ## Test 1
# MAGIC Sum function tests

# COMMAND ----------

# MAGIC %md #### Create expected output DataFrame

# COMMAND ----------

# DataFrame schema
test_1_schema= [StructField('company_name', StringType(),  True),
                StructField('sum',          IntegerType(),  True)]

# Sample data
test_1_data = [['company_a', 'company_b', 'company_c'],
               [1, 2, 4]]

# Create DataFrame
df_test_1 = spark.createDataFrame(zip(*test_1_data), schema = StructType(test_1_schema))

display(df_test_1)

# COMMAND ----------

# MAGIC %md #### Call production parametrized notebook

# COMMAND ----------

test_1_parameters = {"database":         global_temp_db,
                     "table":            "df_1_table",
                     "group_by_cols":    "['company_name']",
                     "value_col":        "balance",
                     "aggregation_type": "sum",
                     "results_table":    "test_1_results"}

test_1_result = dbutils.notebook.run("../parametrized_notebook", 200, test_1_parameters)

test_1_result_table = (spark.table(f"{global_temp_db}.{test_1_result}")
                            .orderBy(col('company_name').asc()))

display(test_1_result_table)

# COMMAND ----------

differences = test_1_result_table.subtract(df_test_1).count()

if differences != 0:
  dbutils.notebook.exit("Test failure: Test 1 failed")

# COMMAND ----------

dbutils.notebook.exit("All tests passed")