# SparkSession is provided by Databricks runtime
# For local execution, initialize manually

from fetcher import fetch
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([
    StructField('id',StringType(), True),
    StructField('symbol',StringType(), True),
    StructField('name',StringType(), True),
    StructField('image',StringType(), True),
    StructField('current_price',StringType(), True),
    StructField('market_cap',StringType(), True),
    StructField('market_cap_rank',StringType(), True),
    StructField('fully_diluted_valuation',StringType(), True),
    StructField('total_volume',StringType(), True),
    StructField('high_24h',StringType(), True),
    StructField('low_24h',StringType(), True),
    StructField('price_change_24h',StringType(), True),
    StructField('price_change_percentage_24h',StringType(), True),
    StructField('market_cap_change_24h',StringType(), True),
    StructField('market_cap_change_percentage_24h',StringType(), True),
    StructField('circulating_supply',StringType(), True),
    StructField('total_supply',StringType(), True),
    StructField('max_supply',StringType(), True),
    StructField('ath',StringType(), True),
    StructField('ath_change_percentage',StringType(), True),
    StructField('ath_date',StringType(), True),
    StructField('atl',StringType(), True),
    StructField('atl_change_percentage',StringType(), True),
    StructField('atl_date',StringType(), True),
    StructField('roi',StringType(), True),
    StructField('last_updated',StringType(), True),
])

def upload_coin_market_top_by_market_cap(schema):
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
    'vs_currency':'USD',
    'order':'market_cap_desc',
    'per_page': 250,
}
    
    df_bronze = spark.createDataFrame(fetch(url,params),schema)
    df_bronze = df_bronze.withColumn('bronze_create_timestamp',current_timestamp())
    try:
        df_bronze.write.mode('overwrite')\
            .saveAsTable('prod.bronze.coin_market_top_by_market_cap')
    except Exception as e:
        print(f'upload failed: {e}')
        raise

def upload_coin_market_top_by_volume(schema):
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency':'USD',
        'order':'volume_desc',
        'per_page': 250
    }

    df_bronze = spark.createDataFrame(fetch(url,params),schema)
    df_bronze = df_bronze.withColumn('bronze_create_timestamp',current_timestamp())
    try:
        df_bronze.write.mode('overwrite')\
            .saveAsTable('prod.bronze.coin_market_top_by_volume')
    except Exception as e:
        print(f'upload failed: {e}')
        raise


upload_coin_market_top_by_market_cap(schema)
upload_coin_market_top_by_volume(schema)