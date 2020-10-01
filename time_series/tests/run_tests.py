# Databricks notebook source
# MAGIC %md ## Run all tests

# COMMAND ----------

tests = ['test_transformation']

for test in tests:
  result = dbutils.notebook.run(f"./{test}", 120)
  if result != "All tests passed":
    dbutils.notebook.exit(result)

# COMMAND ----------

dbutils.notebook.exit("All tests passed")