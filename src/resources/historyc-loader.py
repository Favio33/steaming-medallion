# Databricks notebook source
# MAGIC %run ../configurations/config

# COMMAND ----------

class HistoryLoader():
    def __init__(self, env):
        Conf = Config()
        self.landing_zone = Conf.base_dir_data + "/landing"
        self.catalog = env
        self.db_name = Conf.db_name

    def load_date_lookup(self):        
        print("Loading date_lookup table...", end='')        
        spark.sql(f"""INSERT OVERWRITE TABLE {self.catalog}.{self.db_name}.date_lookup 
                SELECT date, week, year, month, dayofweek, dayofmonth, dayofyear, week_part 
                FROM json.`{self.landing_zone}/date_lookup/6-date-lookup.json/`""")
        print("Done")

    def load_history(self):
        import time
        start = int(time.time())
        print("\nStarting historical data load ...")
        self.load_date_lookup()
        print(f"Historical data load completed in {int(time.time()) - start} seconds")

    def assert_count(self, table_name, expected_count):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name}"
        print(f"Found {actual_count:,} / Expected {expected_count:,} records: Success")        

    def validate(self):
        import time
        start = int(time.time())
        print("\nStarting historical data load validation...")
        self.assert_count("date_lookup", 365)
        print(f"Historical data load validation completed in {int(time.time()) - start} seconds")
