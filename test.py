from pyspark import SparkConf, SparkContext

def main(sc):
  textFile = sc.textFile('wasb://hdfs@u07rlumajqacgvoagnt1.blob.core.windows.net/pg100.txt.utf8')
  print textFile.count()

if __name__ == "__main__":
  conf = SparkConf()
  sc = SparkContext(conf)
  main(sc)
