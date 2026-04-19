from fetcher import fetch
from pyspark.sql.types import *
from pyspark.sql.functions import *

def upload_coins_list(spark):
    table = 'coins_list'
    schema = StructType([
    StructField('id',StringType(), True),
    StructField('symbol',StringType(), True),
    StructField('name',StringType(), True),
    StructField('platforms',StringType(), True)    
])

    url = 'https://api.coingecko.com/api/v3/coins/list'
    params = {
        'include_platform': 'true'
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