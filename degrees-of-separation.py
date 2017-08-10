from pyspark import SparkConf, SparkContext

conf = (SparkConf()
	.set("spark.master", "yarn-client")
	.set("spark.driver.memory", "1g")
	.set("spark.executor.cores", "1")
	.set("spark.executor.memory", "1g"))

sc = SparkContext(conf = conf);

startID = "5306"
targetID = "14"

hits = sc.accumulator(0)

def extractConnections(line):
	fields = line.split()
	return (fields[0], fields[1:])

def createNode((nodeID, edges)):
	if nodeID == startID:
		return (nodeID, (edges, 1, 0))
	else:
		return (nodeID, (edges, 0, ""))

def searchBreadth((nodeID, (edges, state, distance))):
	updatedNodes = []
	if state == 1:
		for edge in edges:
			updatedNodes.append((edge, ([], 1, distance + 1)))
			if edge == targetID:
				hits.add(1)
		state = 2
	updatedNodes.append((nodeID, (edges, state, distance)))
	return updatedNodes

def reduceNodes((edges1, state1, distance1), (edges2, state2, distance2)):
	edges = edges1 if len(edges1) > 0 else edges2
	state = state1 if state1 > state2 else state2
	distance = distance1 if distance1 < distance2 else distance2
	return (edges, distance, state)

rdd = sc.textFile("hdfs://nameservice1/user/nsteyer/data/marvel/marvel-graph.txt")
nodes = rdd.map(extractConnections).reduceByKey(lambda x, y: x + y).map(createNode)
degrees = 1

while hits.value < 1:
	nodes = nodes.flatMap(searchBreadth)
	print("Processing " + str(nodes.count()) + " nodes")
	if hits.value > 0:
		print("Connected by " + str(degrees) + " degrees in " + str(hits.value) + " way(s)")
		break
	nodes = nodes.reduceByKey(reduceNodes)
	degrees += 1
