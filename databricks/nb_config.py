# Databricks notebook source
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    col,
    to_timestamp,
    row_number,
    expr
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging, time
from delta.tables import DeltaTable

# COMMAND ----------

MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 10

# COMMAND ----------

WATERMARKING = 1

# COMMAND ----------

BASE_MNT_PATH = "/mnt"
DELTA_PATH = f"{BASE_MNT_PATH}/delta"
CHECKPOINT_PATH = f"{BASE_MNT_PATH}/checkpoints"

# Raw data paths
RAW_SOURCE_PATH = f"{BASE_MNT_PATH}/raw/customer_updates/"
SCHEMA_LOCATION = f"{CHECKPOINT_PATH}/bronze_schema"

# Bronze table paths
BRONZE_TABLE_PATH = f"{DELTA_PATH}/bronze/customers"
BRONZE_CHECKPOINT = f"{CHECKPOINT_PATH}/bronze_customers"

# Silver table paths
SILVER_TABLE_PATH = f"{DELTA_PATH}/silver/customers"
SILVER_CHECKPOINT = f"{CHECKPOINT_PATH}/silver_customers"

# Gold table paths & configs
GOLD_TABLE_PATH = f"{DELTA_PATH}/gold/customers"
GOLD_CHECKPOINT = f"{CHECKPOINT_PATH}/gold_customers"
GOLD_SCHEMA = "gold"
GOLD_DIM_TABLE = f"{GOLD_SCHEMA}.gold_customer_dimension"
BUSINESS_COLUMNS = [
    "customer_id",
    "name",
    "email",
    "city",
    "status"
]
PRIMARY_KEY = "customer_id"
CURRENT_FLAG = "is_current"