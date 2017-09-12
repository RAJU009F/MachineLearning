from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD

from pyspark import SparkContext , SparkConf

conf  = SparkConf().setMaster("local").setAppName("spam email ")
sc =  SparkContext(conf=conf)

spam  =  sc.textFile("/p01/sample_data/spam.txt")
normal = sc.textFile("/p01/sample_data/normal.txt")

# create HashingTF instance to amp text to vectors of 100000 features
tf = HashingTF(numFeatures=10000)

# each eamil is split into words , and each word is mapped to one feature

spamFeatures = spam.map(lambda email: tf.transform(email.split(" ")))
normalFeatures = normal.map(lambda email: tf.transform(email.split(" ")))


# Create LabeledPoint datasets for +ve and -ve examples

positiveExamples = spamFeatures.map(lambda features: LabeledPoint(1, features))
negativeExamples = normalFeatures.map(lambda features: LabeledPoint(0, features))
trainingData = positiveExamples.union(negativeExamples)
trainingData.cache()

# Run Logistic rgression using SGD algorithm

model = LogisticRegressionWithSGD.train(trainingData)

posTest = tf.transform(" O M G GET cheap stuff by sending money to ".split())
negTest = tf.transform(" Hi Dad, I started studying Spark the other".split())

print " Prediction for positive test example: %g"%model.predict(posTest)
print "Prediction for negative test example: %g"%model.predict(negTest)







