from fetcher import fetch_pages
from pyspark.sql.types import *
from pyspark.sql.functions import *

def upload_exchanges_list(spark):       #top 1k coins by market cap
    table= 'exchanges'

    schema = StructType([
    StructField('id',StringType(), True),
    StructField('name',StringType(), True),
    StructField('year_established',StringType(), True),
    StructField('country',StringType(), True),
    StructField('description',StringType(), True),
    StructField('url',StringType(), True),
    StructField('image',StringType(), True),
    StructField('has_trading_incentive',StringType(), True),
    StructField('trust_score',StringType(), True),
    StructField('trust_score_rank',StringType(), True),
    StructField('trade_volume_24h_btc',StringType(), True)
])
    
    url = 'https://api.coingecko.com/api/v3/exchanges'
    params = {
    'per_page': 250,
}
    
    df_bronze = spark.createDataFrame(fetch_pages(url,params),schema)
    df_bronze = df_bronze.withColumn('bronze_create_timestamp',current_timestamp())
    try:
        df_bronze.write.mode('overwrite')\
            .saveAsTable(f'prod.bronze.{table}')
    except Exception as e:
        print(f'upload failed: {e}')
        raise
    if spark.catalog.tableExists(f'prod.bronze.{table}'):
        print(f'{table} created!')
    else:
        print("Table creation failed!")