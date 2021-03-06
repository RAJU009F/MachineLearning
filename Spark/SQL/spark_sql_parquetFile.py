from spark_session_config import spark

peopleDF  = spark.read.json("/p01/sample_data/people.json")


peopleDF.write.parquet("/p01/spark_output/people.parquet")

# save as parquetfile

parquetFile  =  spark.read.parquet("/p01/spark_output/people.parquet")

parquetFile.createOrReplaceTempView("parquetFile")
teenagers = spark.sql("SELECT name FROM parquetFile WHERE age>=13 AND age <=19")
teenagers.show()


''' OUTPUT

+------+
|  name|
+------+
|Justin|
|Hitler|
| Adolf|
+------+



'''
