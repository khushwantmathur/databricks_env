# Databricks notebook source
# MAGIC %run "./nb_config"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.{GOLD_DIM_TABLE} (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id STRING,
    name STRING,
    email STRING,
    city STRING,
    status STRING,
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN
)
USING DELTA
TBLPROPERTIES (
delta.autoOptimize.optimizeWrite = true,
delta.autoOptimize.autoCompact = true
)
""")

# COMMAND ----------

logger = logging.getLogger("scd2_pipeline")


silver_stream = (
    spark.readStream
    .format("delta")
    .load(SILVER_TABLE_PATH)
)

dim_table = DeltaTable.forName(spark, GOLD_DIM_TABLE)

# Helper functions for inserts and updates while MERGE


def build_update_condition(columns):
    return " OR ".join(
        [f"target.{c} <> source.{c}" for c in columns if c != PRIMARY_KEY]
    )


def build_insert_mapping(columns):
    mapping = {c: f"source.{c}" for c in columns}

    mapping.update({
        "start_date": "source.start_date",
        "end_date": "source.end_date",
        "is_current": "source.is_current"
    })
    return mapping


# SCD2 MERGE

def scd2_merge(df_batch, batch_id):

    try:

        if df_batch.rdd.isEmpty():
            logger.info(f"Batch {batch_id} empty. Skipping.....")
            return

        logger.info(f"Processing batch {batch_id}")

        updates_df = (
            df_batch
            .select(
                *BUSINESS_COLUMNS,
                col("update_ts").alias("start_date"),
                "end_date",
                "is_current"
            )
        )

        update_condition = build_update_condition(BUSINESS_COLUMNS)
        insert_mapping = build_insert_mapping(BUSINESS_COLUMNS)

        (
            dim_table.alias("target")
            .merge(
                updates_df.alias("source"),
                f"""
                target.{PRIMARY_KEY} = source.{PRIMARY_KEY}
                AND target.{CURRENT_FLAG} = true
                """
            )
            .whenMatchedUpdate(
                condition=update_condition,
                set={
                    "end_date": "source.start_date",
                    "is_current": "false"
                }
            )
            .whenNotMatchedInsert(values=insert_mapping)
            .execute()
        )

        logger.info(f"Batch {batch_id} successfully merged")

    except Exception as e:

        logger.error(f"SCD2 merge failed for batch {batch_id}")
        raise e

try:

    merge_query = (
        silver_stream.writeStream
        .foreachBatch(scd2_merge)
        .option("checkpointLocation", GOLD_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )
    merge_query.awaitTermination()

except Exception as e:

    logger.error("Gold layer streaming job failed.....")
    raise e