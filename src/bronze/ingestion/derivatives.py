from fetcher import fetch
from pyspark.sql.types import *
from pyspark.sql.functions import *

def upload_derivatives(spark):
    table = 'derivatives'
    schema = StructType([
    StructField('market',StringType(), True),
    StructField('symbol',StringType(), True),
    StructField('index_id',StringType(), True),
    StructField('price',StringType(), True),
    StructField('price_percentage_change_24h',StringType(), True),
    StructField('contract_type',StringType(), True),
    StructField('index',StringType(), True),
    StructField('basis',StringType(), True),
    StructField('spread',StringType(), True),
    StructField('funding_rate',StringType(), True),
    StructField('open_interest',StringType(), True),   
    StructField('volume_24h',StringType(), True),
    StructField('last_traded_at',StringType(), True),
    StructField('expired_at',StringType(), True)
])

    url = 'https://api.coingecko.com/api/v3/derivatives'
    params = {

    }

    df_bronze = spark.createDataFrame(fetch(url,params),schema)
    df_bronze = df_bronze.withColumn('bronze_create_timestamp',current_timestamp())
    df_bronze = df_bronze.withColumn('created_date',to_date(current_timestamp()))
    try:
        df_bronze.write.mode('append')\
            .partitionBy('created_date')\
            .saveAsTable(f'prod.bronze.{table}')
    except Exception as e:
        print(f'upload failed: {e}')
        raise
    if spark.catalog.tableExists(f'prod.bronze.{table}'):
        print(f'{table} created!')
    else:
        print("Table creation failed!")