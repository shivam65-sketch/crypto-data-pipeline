from pyspark.sql import SparkSession
# from coin_market import upload_coin_market_top_by_market_cap, upload_coin_market_top_by_volume
# from coin_list import upload_coins_list 
# from exchanges import upload_exchanges_list
from bronze.b_derivatives import upload_derivatives
from silver.s_derivatives import silver_upload_derivatives

spark = SparkSession.builder.appName("Crypto").getOrCreate()

# upload_coin_market_top_by_market_cap(spark)
# # upload_coin_market_top_by_volume(spark)  
# upload_coins_list(spark)
# upload_exchanges_list(spark)
upload_derivatives(spark)
silver_upload_derivatives(spark)

