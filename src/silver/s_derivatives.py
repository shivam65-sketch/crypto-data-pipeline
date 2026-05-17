from pyspark.sql.functions import *

def silver_upload_derivatives(spark):
    bronze_table = "b_derivatives"
    if spark.catalog.tableExists(f'prod.bronze.{bronze_table}'):
        df_bronze = spark.table(f'prod.bronze.{bronze_table}')
        if df_bronze.count() == 0:
            raise Exception("Table has no data")
    else:
        raise Exception("Table doesn't exist")

    #filtering out nulls and numeric values more than 0 for required columns
    df_bronze = df_bronze.filter(

        col("symbol").isNotNull() &

        col("price").isNotNull() &
        (col("price").cast("double") > 0) &

        col("open_interest").isNotNull() &
        # (col("open_interest").cast("double") > 0) &

        col("volume_24h").isNotNull() &
        (col("volume_24h").cast("double") > 0)

    )

    #drop duplicates -- based on the same symbol and snapshot timestamp
    df_bronze = df_bronze.dropDuplicates(["symbol","bronze_create_timestamp"])

    df_silver = (

        df_bronze

        .withColumn(
            "price",
            col("price").cast("double")
        )

        .withColumn(
            "price_percentage_change_24h",
            col("price_percentage_change_24h").cast("double")
        )

        .withColumn(
            "index_price",
            col("index").cast("double")
        )

        .withColumn(
            "basis",
            col("basis").cast("double")
        )

        .withColumn(
            "spread",
            col("spread").cast("double")
        )

        .withColumn(
            "funding_rate",
            col("funding_rate").cast("double")
        )

        .withColumn(
            "open_interest",
            col("open_interest").cast("double")
        )

        .withColumn(
            "volume_24h",
            col("volume_24h").cast("double")
        )

        .withColumn(
            "last_traded_at",
            from_unixtime(col("last_traded_at").cast("double").cast("long"))
        )

        # Derived metrics
        .withColumn(
            "oi_volume_ratio",
            col("open_interest") / col("volume_24h")
        )

        .withColumn(
            "premium_pct",
            (
                col("price") - col("index_price")
            ) / col("index_price")
        )

        .withColumn(
            "notional_open_interest",
            col("open_interest") * col("price")
        )

        .withColumn(
            "silver_create_timestamp",
            current_timestamp()
        )


    )

    df_silver = df_silver.select(

        col("market"),
        col("symbol"),
        col("index_id"),
        col("contract_type"),
        col("price"),
        col("price_percentage_change_24h"),
        col("index_price"),
        col("basis"),
        col("spread"),
        col("funding_rate"),
        col("open_interest"),
        col("volume_24h"),
        col("last_traded_at"),
        col("oi_volume_ratio"),
        col("premium_pct"),
        col("notional_open_interest"),
        col("bronze_create_timestamp").alias("snapshot_timestamp"),
        col("created_date"),
        col("created_hour"),
        col("silver_create_timestamp")

    )
    try:
        df_silver.write\
            .format("delta")\
            .mode("append")\
            .partitionBy("created_date")\
            .saveAsTable("prod.silver.s_derivatives")
        print("Table prod.silver.derivatives updated")
    except Exception as e:
        print(f"silver upload failed: {e}")



