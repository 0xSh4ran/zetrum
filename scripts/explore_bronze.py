from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
	.appName("explore-bronze") \
	.getOrCreate()
	
df = spark.table("zetrum.clickstream_events")

print("Total events : ", df.count())

df.groupBy("event_type")
df.count()
df.orderBy("count", ascending = False)
df.show()

df.groupBy("city")
df.count()
df.orderBy("count", ascending = False)
df.limit(10)
df.show

spark.stop()
