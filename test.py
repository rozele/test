from __future__ import print_function

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonTest")\
        .getOrCreate()

    count = spark.sparkContext.textFile('wasb:///pg100.txt.utf8').count()
    print("Output is %f" % count)

    spark.stop()