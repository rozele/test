textFile = sc.textFile('wasb://hdfs@u07rlumajqacgvoagnt1.blob.core.windows.net/pg100.txt.utf8')
print textFile.count()