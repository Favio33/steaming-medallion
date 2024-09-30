# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
dbutils.widgets.text("RunType", "once", "Set once to run as a batch")
dbutils.widgets.text("ProcessingTime", "5 seconds", "Set the microbatch interval")
dbutils.widgets.text("landing-path", "/Volumes/develop/kamino/jangofett/customerio/", "")

# COMMAND ----------

env = dbutils.widgets.get("Environment")
once = True if dbutils.widgets.get("RunType") == "once" else False
processing_time = dbutils.widgets.get("ProcessingTime")
if once:
    print("Starting sbit in batch mode.")
else:
    print(f"Starting sbit in stream mode with {processing_time} microbatch.")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Set Up

# COMMAND ----------

# MAGIC %run ./configurations/setup

# COMMAND ----------

# MAGIC %run ./resources/historyc-loader

# COMMAND ----------

setup = SetupHelper(env, True)
h_loader = HistoryLoader("develop")

# COMMAND ----------

setup_required = spark.sql(f"SHOW TABLES IN {env}.dummy").where(~col("tableName").isin("h_payment_demo", "promotions", "promotions_v2", "user_item_data")).count()

# COMMAND ----------

if setup_required != 1:
    setup.setup()
    setup.validate()
    h_loader.load_history()
    h_loader.validate()
else:
    spark.sql(f"USE {h_loader.catalog}.{h_loader.db_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Bronze Ingestion

# COMMAND ----------

# MAGIC %run ./bronze_ingestion

# COMMAND ----------

bronze = Bronze(env)

# COMMAND ----------

bronze.consume(once, processing_time)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Silver Ingestion

# COMMAND ----------

# MAGIC %run ./silver_ingestion

# COMMAND ----------

silver = Silver(env)

# COMMAND ----------

silver.upsert(once, processing_time)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Gold Ingestion

# COMMAND ----------

# MAGIC %run ./gold_ingestion

# COMMAND ----------

gold = Gold(env)

# COMMAND ----------

gold.upsert(once, processing_time)
