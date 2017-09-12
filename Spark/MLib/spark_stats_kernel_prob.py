from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import numpy as np

from pyspark.mllib.stat import Statistics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Matrices, Vectors
from pyspark.mllib.stat import KernelDensity

conf = SparkConf().setMaster("local").setAppName("MyAPP")
sc = SparkContext(conf=conf)


data = sc.parallelize([1.0, 1.0, 1.0, 2.0,3.0, 4.0, 5.0, 5.0, 6.0, 7.0, 8.0, 9.0, 9.0])

kd = KernelDensity()
kd.setSample(data)
kd.setBandwidth(3.0)

densities = kd.estimate([-1.0, 2.0, 5.0])

print densities


