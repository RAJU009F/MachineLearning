from pyspark.sql.types import *
from spark_session_config import spark
from pyspark.sql import Row

sc =  spark.sparkContext

lines = sc.textFile("/p01/sample_data/people.txt")

parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: (p[0], p[1].strip()))

schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

schemaPeople = spark.createDataFrame(people, schema)
schemaPeople.createOrReplaceTempView("people")


results = spark.sql("SELECT name FROM people")

results.show()

'''
+----------+
|      name|
+----------+
|   Michael|
|      Andy|
|    Justin|
|    Hitler|
|     Adolf|
|Cheguevera|
+----------+
'''
