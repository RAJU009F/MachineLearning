from spark_session_config import spark

df = spark.read.json("/p01/sample_data/people.json")
#df.show()

'''
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+


'''

#df.printSchema()
'''
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)



'''

#df.select("name").show()
'''
+-------+
|   name|
+-------+
|Michael|
|   Andy|
|Micheal|
| Justin|
| Hitler|
|  Adolf|
+-------+
'''

#df.select(df["name"], df['age']+1).show()
'''
+-------+---------+
|   name|(age + 1)|
+-------+---------+
|Michael|     null|
|   Andy|       31|
|Micheal|       31|
| Justin|       20|
| Hitler|       20|
|  Adolf|       20|
+-------+---------+
'''

#df.filter(df['age']>19).show()
'''

+---+-------+
|age|   name|
+---+-------+
| 30|   Andy|
| 30|Micheal|
+---+-------+

'''


#df.groupBy("age").count().show()

'''
+----+-----+
| age|count|
+----+-----+
|  19|    3|
|null|    1|
|  30|    2|
+----+-----+

'''

# Running SQL Queries Programmatically

#df.createOrReplaceTempView("people")


#sqlDF = spark.sql("SELECT * from people WHERE people.age >19")
#sqlDF.show()
'''
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  30|Micheal|
|  19| Justin|
|  19| Hitler|
|  19|  Adolf|
+----+-------+

+---+-------+
|age|   name|
+---+-------+
| 30|   Andy|
| 30|Micheal|
+---+-------+




'''

# Global Temporary View
#createGlobalTempView
df.createGlobalTempView("people")
spark.sql("SELECT * FROM gloabl_temp.people").show()


# create a new session and access the global temp view
spark.newSession().sql("SELECT* FROM global_temp.people").show()







