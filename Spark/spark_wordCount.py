from pyspark import SparkContext, SparkConf

conf  = SparkConf().setMaster("local").setAppName("My APP")
sc =  SparkContext(conf=conf)


'''
word count Example
'''

fileRDD =  sc.textFile('/p01/sample_data/text_data.txt')
wordRDD = fileRDD.flatMap(lambda line: line.split()).map(lambda  y: (y,1))
wordCountRDD = wordRDD.reduceByKey(lambda x,y:x+y)
for tup in wordCountRDD.take(10):
	print tup
print "total entries:"+format(wordCountRDD.count())

wordCountRDD.saveAsTextFile("/p01/spark_data/wordcount_out.txt")

