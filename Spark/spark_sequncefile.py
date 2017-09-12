from pyspark import SparkContext, SparkConf

conf  = SparkConf().setMaster("local").setAppName("My APP")
sc =  SparkContext(conf=conf)

rdd =  sc.parallelize(range(1,4)).map(lambda x: (x, "a"*x))
rdd.saveAsSequenceFile("/p01/spark_data/sequence_file")
#rdd.saveAsTextFile("/p01/spark_data/sequence_file")
list = sorted(sc.sequenceFile("/p01/spark_data/sequence_file").collect())
for item in rdd.collect():#list:
	print item
