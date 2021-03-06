from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint


# postive Labled point of Dense vector
pos = LabeledPoint(1.0, [1.0,0.0,2.0])

print pos
# negative Labled Point of Sparse Vector

neg =  LabeledPoint(0.0, SparseVector(3, [0, 2], [1.0, 3.0]))
print neg

# LIBSVM 

conf  =  SparkConf().setMaster("local").setAppName("MyAPP")
sc =  SparkContext(conf=conf)
from pyspark.mllib.util import MLUtils
examples =  MLUtils.loadLibSVMFile(sc, "/p01/sample_data/sample_libsvm_data.txt")

print example
