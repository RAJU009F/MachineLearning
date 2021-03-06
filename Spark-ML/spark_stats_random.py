from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import numpy as np

from pyspark.mllib.stat import Statistics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Matrices, Vectors
from pyspark.mllib.random import RandomRDDs

conf = SparkConf().setMaster("local").setAppName("MyAPP")
sc = SparkContext(conf=conf)

u = RandomRDDs.normalRDD(sc, 1000000L, 10)

v = u.map(lambda x: 1.0+2.0*x)

print v
