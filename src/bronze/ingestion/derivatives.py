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

    data = fetch(url,params)
    if not data or len(data) == 0:
        raise Exception('Empty API response')

    if not isinstance(data,list):
        raise Exception("Unexpected API format")

    df_bronze = spark.createDataFrame(data,schema)
    print("Raw rows:", df_bronze.count())
    df_bronze = df_bronze.filter(
        (col('market').isNotNull())\
        & (col('contract_type').isNotNull())\
        & (lower(col('market')) == 'binance (futures)')\
        & (lower(col('contract_type')).contains('perpetual'))
        & (lower(col('symbol')).contains('usdt'))
    )
    print("Filtered rows:", df_bronze.count())
    df_bronze = df_bronze.withColumn('bronze_create_timestamp',current_timestamp())
    df_bronze = df_bronze.withColumn('created_date',to_date(col('bronze_create_timestamp')))
    df_bronze = df_bronze.withColumn('created_hour',hour(col('bronze_create_timestamp')))
    try:
        df_bronze.write.mode('append')\
            .partitionBy('created_date','created_hour')\
            .saveAsTable(f'prod.bronze.{table}')
    except Exception as e:
        print(f'upload failed: {e}')
        raise
    if spark.catalog.tableExists(f'prod.bronze.{table}'):
        print(f'{table} created!')
    else:
        print("Table creation failed!")
