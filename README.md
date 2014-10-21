flink-graph
===========

Graph API for Apache Flink

##Implemented Operations

###Graph Class
* getVertices()
* getEdges()
* mapVertices()
* subGraph()
* outDegrees()
* pga()
* getUndirected()
* reverse()
* create(vertices, edges)
* readTuple2CsvFile
* readEdgesCsvFile
* readGraphFromCsvFile
* numberOfVertices()
* numberOfEdges()
* getVertexIds()
* getEdgeIds()
* isWeaklyConnected()
* addVertex()
* removeVertex()
* addEdge()
* union()
* passMessages()


###Vertex Class

###Edge Class
* reverse()

##Tested Operations

##Wishlist

###Graph Class
* fromCollection(vertices, edges)
* fromCollection(vertices)
* mapEdges()
* getNeighborhoodGraph(Vertex src, int distance)
* vertexCentricComputation()
* edgeCentricComputation()
* partitionCentricComputation()

###Vertex Class
* getDegree()
* inDegree()
* outDegree()
* getInNeighbors()
* getOutNeighbors()
* getAllNeighbors()


###Edge Class

##Other (low priority)
* partitionBy
* sample
* centrality
* pagerank
* distance
* clusteringCoefficient
* dfs
* bfs
* sssp
* isIsomorphic
* isSubgraphOf
