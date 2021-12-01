from pyspark import SparkConf,SparkContext
conf = SparkConf ()
sc = SparkContext.getOrCreate(conf = conf)

stop_word = ["they", "she", "he", "it", "the", "as", "is", "and"]


search_rdd = sc.wholeTextFiles("/home/vikaskashyap307/HW5/Data/*/*")\
        .flatMap(lambda x: [(word, x[0]) for word in x[1].lower().split()])\
        .filter(lambda x: x[0] not in stop_word).map(lambda x: (x, 1))\
        .reduceByKey(lambda a, b: a + b).map(lambda x: (x[0][0], [(x[0][1], x[1])])).reduceByKey(lambda a, b: a + b)
     

search_rdd.coalesce(1).saveAsTextFile('/home/vikaskashyap307/HW5/result')

