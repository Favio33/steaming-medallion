# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

# MAGIC %md
# MAGIC # Set Up

# COMMAND ----------

# MAGIC %run ./configurations/setup

# COMMAND ----------

setup = SetupHelper(env)
setup.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC # Loader

# COMMAND ----------

# MAGIC %run ./resources/historyc-loader

# COMMAND ----------

setup = SetupHelper(env, True)
h_loader = HistoryLoader("develop")

# COMMAND ----------

# MAGIC %md
# MAGIC # Produce 1

# COMMAND ----------

# MAGIC %run ./configurations/main

# COMMAND ----------

producer = Producer()
producer.produce(1)
producer.validate(1)
dbutils.notebook.run("./main", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Validation

# COMMAND ----------

# MAGIC
# MAGIC %run ./bronze_ingestion

# COMMAND ----------

bronze = Bronze(env)
bronze.validate(1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Validation

# COMMAND ----------

# MAGIC %run ./silver_ingestion

# COMMAND ----------

silver = Silver(env)
silver.validate(1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Validation

# COMMAND ----------

# MAGIC %run ./gold_ingestion

# COMMAND ----------

gold = Gold(env)
gold.validate(1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Produce 2

# COMMAND ----------

producer.produce(2)
producer.validate(2)
dbutils.notebook.run("./main", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

bronze.validate(2)
silver.validate(2)
gold.validate(2)

# COMMAND ----------

cleanup.cleanup()
