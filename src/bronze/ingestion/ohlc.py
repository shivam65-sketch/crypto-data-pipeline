from fetcher import fetch
from pyspark.sql.types import *
from pyspark.sql.functions import *

url = 'https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments'
params = {

}
dcxdata = fetch(url,params)
dcxsymbol = [i.split('-')[1].split('_')[0] for i in dcxdata]
df = spark.table('prod.bronze.derivatives')

df = df.filter(col('index_id').isin(dcxsymbol))
df.display()

