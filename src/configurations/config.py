# Databricks notebook source
import os

# COMMAND ----------

class Config():    
    def __init__(self):      
        self.base_dir_data = os.getenv("ENV_URL_DEVELOP_BRZ")
        self.base_dir_checkpoint = os.getenv("ENV_URL_DEVELOP_CHK_BRZ")
        self.db_name = "dummy"
        self.maxFilesPerTrigger = 1000
