Gelly
===========

Graph API for Apache Flink

##Implemented Methods

###Graph Creation

* create
* fromCollection

###Graph Properties and Metrics

* getVertices
* getEdges
* getVertexIds
* getEdgeIds
* numberOfVertices
* numberOfEdges
* getDegrees
* inDegrees
* outDegrees
* isWeaklyConnected

###Graph Mutations

* addVertex
* addEdge
* removeVertex
* removeEdge

###Graph Transformations

* mapVertices
* mapEdges
* union
* filterOnVertices
* filterOnEdges
* subgraph
* reverse
* getUndirected
* joinWithVertices
* joinWithEdges
* joinWithEdgesOnSource
* joinWithEdgesOnTarget


### Neighborhood Methods

* reduceOnEdges
* reduceOnNeighbors

### Graph Validation

* validate
* InvalidVertexIdsValidator

## Graph Algorithms

* PageRank
* SingleSourceShortestPaths

## Examples

* GraphMetrics
* PageRank
* SingleSourceShortestPaths
