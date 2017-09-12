from pyspark.sql.types import *
from spark_session_config import spark
from pyspark.sql import Row

''' load and save function '''
#df  = spark.read.load("/p01/sample_data/people.json", format="json")
#df.select("name", "age").write.save("/p01/spark_output/namesAndAges_none.parquet", format="parquet")
#df.select("name", "age").write.save("/p01/spark_output/namesAndAges_none.parquet", format=None, mode="overwrite")


'''

run sql querries on files

'''
'''#
df =  spark.sql("SELECT * FROM parquet. /p01/sample_data/users.parquet")

df.show()

names = df.map(lambda p: "Name: "+p.name).collect()

for name in names:
	print name
'''



# Saving to Persistent Tables

#df.select("name", "age").write.option("path", "/user/hive2/warehouse/p01.db/Name_Age" ).saveAsTable("Name_Age")

#df.select("name", "age").write.saveAsTable("Name_Age")




# Bucketing, Sorting and Partitioning

df =  spark.sql("SELECT * FROM parquet. /p01/sample_data/users.parquet")
df.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
df.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")



