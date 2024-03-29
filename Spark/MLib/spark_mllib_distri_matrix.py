from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint


from pyspark.mllib.linalg.distributed import RowMatrix

conf =  SparkConf().setMaster("local").setAppName("MyAPP")
sc = SparkContext(conf = conf)

# distributed Row matrix

rows =  sc.parallelize([[1,2,3], [4,5,6],[7,8,9], [10,11,12]])
mat = RowMatrix(rows)

m = mat.numRows()
n = mat.numCols()

rowsRDD = mat.rows


print m
print n
print rowsRDD



