from pyspark import SparkConf, SparkContext
from math import sqrt

conf = (SparkConf()
	.set("spark.master", "yarn-client")
	.set("spark.driver.memory", "1g")
	.set("spark.executor.cores", "1")
	.set("spark.executor.memory", "1g"))

sc = SparkContext(conf = conf);

minCosine = 0.97
minDimensions = 50

def extractNames(line):
	fields = line.split('|')
	return (fields[0], fields[1])

def extractRatings(line):
	fields = line.split()
	return (fields[0], (fields[1], float(fields[2])))

def filterDuplicates((user, ((movie1, rating1), (movie2, rating2)))):
	return movie1 < movie2

def processPairs((user, ((movie1, rating1), (movie2, rating2)))):
	return ((movie1, movie2), (rating1, rating2))

def expandPairs(((movie1, movie2), ratingPairs)):
	dimensions = sumAB = sumAA = sumBB = 0
	for A, B in ratingPairs:
		sumAB += A * B
		sumAA += A * A
		sumBB += B * B
		dimensions += 1
	cosine = sumAB / (sqrt(sumAA) * sqrt(sumBB))
	name1 = movieMap[movie1]
	name2 = movieMap[movie2]
	if cosine > minCosine and dimensions > minDimensions:
		return [(name1, [name2]), (name2, [name1])]
	else:
		return [(name1, []), (name2, [])]

names = sc.textFile("hdfs://nameservice1/user/nsteyer/data/ml-100k/u.item").map(extractNames)
movieMap = names.collectAsMap()

ratings = sc.textFile("hdfs://nameservice1/user/nsteyer/data/ml-100k/u.data").map(extractRatings)
pairs = ratings.join(ratings).filter(filterDuplicates).map(processPairs).groupByKey()
similarMovies = pairs.flatMap(expandPairs).reduceByKey(lambda x, y: x + y)

similarMovies.saveAsTextFile("hdfs://nameservice1/user/nsteyer/data/similar-movies")
