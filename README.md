flink-graph
===========

Graph API for Apache Flink

##Implemented Operations

###Graph Class

* readTuple2CsvFile
* readEdgesCsvFile
* readGraphFromCsvFile

###Vertex Class

###Edge Class
* reverse()

##Tested Operations
* mapVertices()
* mapEdges()
* subgraph()
* filterOnVertices(vertexFilter)
* filterOnEdges(edgeFilter)
* outDegrees()
* inDegrees()
* getDegrees()
* getUndirected()
* reverse()
* addVertex()
* removeVertex()
* addEdge()
* removeEdge()
* union()
* isWeaklyConnected()
* runVertexCentricIteration()
* fromCollection(vertices, edges)
* getVertices()
* getEdges()
* create(vertices, edges)
* numberOfVertices()
* numberOfEdges()
* getVertexIds()
* getEdgeIds()

##Wishlist

###Graph Class
* fromCollection(edges)
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
