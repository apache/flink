---
title: Library Methods
weight: 4
type: docs
aliases:
  - /zh/dev/libs/gelly/library_methods.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Library Methods

Gelly has a growing collection of graph algorithms for easily analyzing large-scale Graphs.



Gelly's library methods can be used by simply calling the `run()` method on the input graph:

{{< tabs "3e609cf5-6632-413e-be14-5fcfa0f10ff3" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

Graph<Long, Long, NullValue> graph = ...

// run Label Propagation for 30 iterations to detect communities on the input graph
DataSet<Vertex<Long, Long>> verticesWithCommunity = graph.run(new LabelPropagation<Long>(30));

// print the result
verticesWithCommunity.print();

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = ExecutionEnvironment.getExecutionEnvironment

val graph: Graph[java.lang.Long, java.lang.Long, NullValue] = ...

// run Label Propagation for 30 iterations to detect communities on the input graph
val verticesWithCommunity = graph.run(new LabelPropagation[java.lang.Long, java.lang.Long, NullValue](30))

// print the result
verticesWithCommunity.print()

```
{{< /tab >}}
{{< /tabs >}}

## Community Detection

#### Overview
In graph theory, communities refer to groups of nodes that are well connected internally, but sparsely connected to other groups.
This library method is an implementation of the community detection algorithm described in the paper [Towards real-time community detection in large networks](http://arxiv.org/pdf/0808.2633.pdf).

#### Details
The algorithm is implemented using [scatter-gather iterations](#scatter-gather-iterations).
Initially, each vertex is assigned a `Tuple2` containing its initial value along with a score equal to 1.0.
In each iteration, vertices send their labels and scores to their neighbors. Upon receiving messages from its neighbors,
a vertex chooses the label with the highest score and subsequently re-scores it using the edge values,
a user-defined hop attenuation parameter, `delta`, and the superstep number.
The algorithm converges when vertices no longer update their value or when the maximum number of iterations
is reached.

#### Usage
The algorithm takes as input a `Graph` with any vertex type, `Long` vertex values, and `Double` edge values. It returns a `Graph` of the same type as the input,
where the vertex values correspond to the community labels, i.e. two vertices belong to the same community if they have the same vertex value.
The constructor takes two parameters:

* `maxIterations`: the maximum number of iterations to run.
* `delta`: the hop attenuation parameter, with default value 0.5.

## Label Propagation

#### Overview
This is an implementation of the well-known Label Propagation algorithm described in [this paper](http://journals.aps.org/pre/abstract/10.1103/PhysRevE.76.036106). The algorithm discovers communities in a graph, by iteratively propagating labels between neighbors. Unlike the [Community Detection library method](#community-detection), this implementation does not use scores associated with the labels.

#### Details
The algorithm is implemented using [scatter-gather iterations](#scatter-gather-iterations).
Labels are expected to be of type `Comparable` and are initialized using the vertex values of the input `Graph`.
The algorithm iteratively refines discovered communities by propagating labels. In each iteration, a vertex adopts
the label that is most frequent among its neighbors' labels. In case of a tie (i.e. two or more labels appear with the
same frequency), the algorithm picks the greater label. The algorithm converges when no vertex changes its value or
the maximum number of iterations has been reached. Note that different initializations might lead to different results.

#### Usage
The algorithm takes as input a `Graph` with a `Comparable` vertex type, a `Comparable` vertex value type and an arbitrary edge value type.
It returns a `DataSet` of vertices, where the vertex value corresponds to the community in which this vertex belongs after convergence.
The constructor takes one parameter:

* `maxIterations`: the maximum number of iterations to run.

## Connected Components

#### Overview
This is an implementation of the Weakly Connected Components algorithm. Upon convergence, two vertices belong to the
same component, if there is a path from one to the other, without taking edge direction into account.

#### Details
The algorithm is implemented using [scatter-gather iterations](#scatter-gather-iterations).
This implementation uses a comparable vertex value as initial component identifier (ID). Vertices propagate their
current value in each iteration. Upon receiving component IDs from its neighbors, a vertex adopts a new component ID if
its value is lower than its current component ID. The algorithm converges when vertices no longer update their component
ID value or when the maximum number of iterations has been reached.

#### Usage
The result is a `DataSet` of vertices, where the vertex value corresponds to the assigned component.
The constructor takes one parameter:

* `maxIterations`: the maximum number of iterations to run.

## GSA Connected Components

#### Overview
This is an implementation of the Weakly Connected Components algorithm. Upon convergence, two vertices belong to the
same component, if there is a path from one to the other, without taking edge direction into account.

#### Details
The algorithm is implemented using [gather-sum-apply iterations](#gather-sum-apply-iterations).
This implementation uses a comparable vertex value as initial component identifier (ID). In the gather phase, each
vertex collects the vertex value of their adjacent vertices. In the sum phase, the minimum among those values is
selected. In the apply phase, the algorithm sets the minimum value as the new vertex value if it is smaller than
the current value. The algorithm converges when vertices no longer update their component ID value or when the
maximum number of iterations has been reached.

#### Usage
The result is a `DataSet` of vertices, where the vertex value corresponds to the assigned component.
The constructor takes one parameter:

* `maxIterations`: the maximum number of iterations to run.

## Single Source Shortest Paths

#### Overview
An implementation of the Single-Source-Shortest-Paths algorithm for weighted graphs. Given a source vertex, the algorithm computes the shortest paths from this source to all other nodes in the graph.

#### Details
The algorithm is implemented using [scatter-gather iterations](#scatter-gather-iterations).
In each iteration, a vertex sends to its neighbors a message containing the sum its current distance and the edge weight connecting this vertex with the neighbor. Upon receiving candidate distance messages, a vertex calculates the minimum distance and, if a shorter path has been discovered, it updates its value. If a vertex does not change its value during a superstep, then it does not produce messages for its neighbors for the next superstep. The computation terminates after the specified maximum number of supersteps or when there are no value updates.

#### Usage
The algorithm takes as input a `Graph` with any vertex type and `Double` edge values. The vertex values can be any type and are not used by this algorithm. The vertex type must implement `equals()`.
The output is a `DataSet` of vertices where the vertex values correspond to the minimum distances from the given source vertex.
The constructor takes two parameters:

* `srcVertexId` The vertex ID of the source vertex.
* `maxIterations`: the maximum number of iterations to run.

## GSA Single Source Shortest Paths

The algorithm is implemented using [gather-sum-apply iterations](#gather-sum-apply-iterations).

See the [Single Source Shortest Paths](#single-source-shortest-paths) library method for implementation details and usage information.

## Triangle Enumerator

#### Overview
This library method enumerates unique triangles present in the input graph. A triangle consists of three edges that connect three vertices with each other.
This implementation ignores edge directions.

#### Details
The basic triangle enumeration algorithm groups all edges that share a common vertex and builds triads, i.e., triples of vertices
that are connected by two edges. Then, all triads are filtered for which no third edge exists that closes the triangle.
For a group of <i>n</i> edges that share a common vertex, the number of built triads is quadratic <i>((n*(n-1))/2)</i>.
Therefore, an optimization of the algorithm is to group edges on the vertex with the smaller output degree to reduce the number of triads.
This implementation extends the basic algorithm by computing output degrees of edge vertices and grouping on edges on the vertex with the smaller degree.

#### Usage
The algorithm takes a directed graph as input and outputs a `DataSet` of `Tuple3`. The Vertex ID type has to be `Comparable`.
Each `Tuple3` corresponds to a triangle, with the fields containing the IDs of the vertices forming the triangle.

## Summarization

#### Overview
The summarization algorithm computes a condensed version of the input graph by grouping vertices and edges based on
their values. In doing so, the algorithm helps to uncover insights about patterns and distributions in the graph.
One possible use case is the visualization of communities where the whole graph is too large and needs to be summarized
based on the community identifier stored at a vertex.

#### Details
In the resulting graph, each vertex represents a group of vertices that share the same value. An edge, that connects a
vertex with itself, represents all edges with the same edge value that connect vertices from the same vertex group. An
edge between different vertices in the output graph represents all edges with the same edge value between members of
different vertex groups in the input graph.

The algorithm is implemented using Flink data operators. First, vertices are grouped by their value and a representative
is chosen from each group. For any edge, the source and target vertex identifiers are replaced with the corresponding
representative and grouped by source, target and edge value. Output vertices and edges are created from their
corresponding groupings.

#### Usage
The algorithm takes a directed, vertex (and possibly edge) attributed graph as input and outputs a new graph where each
vertex represents a group of vertices and each edge represents a group of edges from the input graph. Furthermore, each
vertex and edge in the output graph stores the common group value and the number of represented elements.

## Clustering

### Average Clustering Coefficient

#### Overview
The average clustering coefficient measures the mean connectedness of a graph. Scores range from 0.0 (no edges between
neighbors) to 1.0 (complete graph).

#### Details
See the [Local Clustering Coefficient](#local-clustering-coefficient) library method for a detailed explanation of
clustering coefficient. The Average Clustering Coefficient is the average of the Local Clustering Coefficient scores
over all vertices with at least two neighbors. Each vertex, independent of degree, has equal weight for this score.

#### Usage
Directed and undirected variants are provided. The analytics take a simple graph as input and output an `AnalyticResult`
containing the total number of vertices and average clustering coefficient of the graph. The graph ID type must be
`Comparable` and `Copyable`.

* `setParallelism`: override the parallelism of operators processing small amounts of data

### Global Clustering Coefficient

#### Overview
The global clustering coefficient measures the connectedness of a graph. Scores range from 0.0 (no edges between
neighbors) to 1.0 (complete graph).

#### Details
See the [Local Clustering Coefficient](#local-clustering-coefficient) library method for a detailed explanation of
clustering coefficient. The Global Clustering Coefficient is the ratio of connected neighbors over the entire graph.
Vertices with higher degrees have greater weight for this score because the count of neighbor pairs is quadratic in
degree.

#### Usage
Directed and undirected variants are provided. The analytics take a simple graph as input and output an `AnalyticResult`
containing the total number of triplets and triangles in the graph. The result class provides a method to compute the
global clustering coefficient score. The graph ID type must be `Comparable` and `Copyable`.

* `setParallelism`: override the parallelism of operators processing small amounts of data

### Local Clustering Coefficient

#### Overview
The local clustering coefficient measures the connectedness of each vertex's neighborhood. Scores range from 0.0 (no
edges between neighbors) to 1.0 (neighborhood is a clique).

#### Details
An edge between neighbors of a vertex is a triangle. Counting edges between neighbors is equivalent to counting the
number of triangles which include the vertex. The clustering coefficient score is the number of edges between neighbors
divided by the number of potential edges between neighbors.

See the [Triangle Listing](#triangle-listing) library method for a detailed explanation of triangle enumeration.

#### Usage
Directed and undirected variants are provided. The algorithms take a simple graph as input and output a `DataSet` of
`UnaryResult` containing the vertex ID, vertex degree, and number of triangles containing the vertex. The result class
provides a method to compute the local clustering coefficient score. The graph ID type must be `Comparable` and
`Copyable`.

* `setIncludeZeroDegreeVertices`: include results for vertices with a degree of zero
* `setParallelism`: override the parallelism of operators processing small amounts of data

### Triadic Census

#### Overview
A triad is formed by any three vertices in a graph. Each triad contains three pairs of vertices which may be connected
or unconnected. The [Triadic Census](http://vlado.fmf.uni-lj.si/pub/networks/doc/triads/triads.pdf) counts the
occurrences of each type of triad with the graph.

#### Details
This analytic counts the four undirected triad types (formed with 0, 1, 2, or 3 connecting edges) or 16 directed triad
types by counting the triangles from [Triangle Listing](#triangle-listing) and running [Vertex Metrics](#vertex-metrics)
to obtain the number of triplets and edges. Triangle counts are then deducted from triplet counts, and triangle and
triplet counts are removed from edge counts.

#### Usage
Directed and undirected variants are provided. The analytics take a simple graph as input and output an
`AnalyticResult` with accessor methods for querying the count of each triad type. The graph ID type must be
`Comparable` and `Copyable`.

* `setParallelism`: override the parallelism of operators processing small amounts of data

### Triangle Listing

#### Overview
Enumerates all triangles in the graph. A triangle is composed of three edges connecting three vertices into cliques of
size 3.

#### Details
Triangles are listed by joining open triplets (two edges with a common neighbor) against edges on the triplet endpoints.
This implementation uses optimizations from
[Schank's algorithm](http://i11www.iti.uni-karlsruhe.de/extra/publications/sw-fclt-05_t.pdf) to improve performance with
high-degree vertices. Triplets are generated from the lowest degree vertex since each triangle need only be listed once.
This greatly reduces the number of generated triplets which is quadratic in vertex degree.

#### Usage
Directed and undirected variants are provided. The algorithms take a simple graph as input and output a `DataSet` of
`TertiaryResult` containing the three triangle vertices and, for the directed algorithm, a bitmask marking each of the
six potential edges connecting the three vertices. The graph ID type must be `Comparable` and `Copyable`.

* `setParallelism`: override the parallelism of operators processing small amounts of data
* `setSortTriangleVertices`: normalize the triangle listing such that for each result (K0, K1, K2) the vertex IDs are sorted K0 < K1 < K2

## Link Analysis

### Hyperlink-Induced Topic Search

#### Overview
[Hyperlink-Induced Topic Search](http://www.cs.cornell.edu/home/kleinber/auth.pdf) (HITS, or "Hubs and Authorities")
computes two interdependent scores for every vertex in a directed graph. Good hubs are those which point to many
good authorities and good authorities are those pointed to by many good hubs.

#### Details
Every vertex is assigned the same initial hub and authority scores. The algorithm then iteratively updates the scores
until termination. During each iteration new hub scores are computed from the authority scores, then new authority
scores are computed from the new hub scores. The scores are then normalized and optionally tested for convergence.
HITS is similar to [PageRank](#pagerank) but vertex scores are emitted in full to each neighbor whereas in PageRank
the vertex score is first divided by the number of neighbors.

#### Usage
The algorithm takes a simple directed graph as input and outputs a `DataSet` of `UnaryResult` containing the vertex ID,
hub score, and authority score. Termination is configured by the number of iterations and/or a convergence threshold on
the iteration sum of the change in scores over all vertices.

* `setIncludeZeroDegreeVertices`: whether to include zero-degree vertices in the iterative computation
* `setParallelism`: override the operator parallelism

### PageRank

#### Overview
[PageRank](https://en.wikipedia.org/wiki/PageRank) is an algorithm that was first used to rank web search engine
results. Today, the algorithm and many variations are used in various graph application domains. The idea of PageRank is
that important or relevant vertices tend to link to other important vertices.

#### Details
The algorithm operates in iterations, where pages distribute their scores to their neighbors (pages they have links to)
and subsequently update their scores based on the sum of values they receive. In order to consider the importance of a
link from one page to another, scores are divided by the total number of out-links of the source page. Thus, a page with
10 links will distribute 1/10 of its score to each neighbor, while a page with 100 links will distribute 1/100 of its
score to each neighboring page.

#### Usage
The algorithm takes a directed graph as input and outputs a `DataSet` where each `Result` contains the vertex ID and
PageRank score. Termination is configured with a maximum number of iterations and/or a convergence threshold
on the sum of the change in score for each vertex between iterations.

* `setParallelism`: override the operator parallelism

## Metric

### Vertex Metrics

#### Overview
This graph analytic computes the following statistics for both directed and undirected graphs:
- number of vertices
- number of edges
- average degree
- number of triplets
- maximum degree
- maximum number of triplets

The following statistics are additionally computed for directed graphs:
- number of unidirectional edges
- number of bidirectional edges
- maximum out degree
- maximum in degree

#### Details
The statistics are computed over vertex degrees generated from `degree.annotate.directed.VertexDegrees` or
`degree.annotate.undirected.VertexDegree`.

#### Usage
Directed and undirected variants are provided. The analytics take a simple graph as input and output an `AnalyticResult`
with accessor methods for the computed statistics. The graph ID type must be `Comparable`.

* `setIncludeZeroDegreeVertices`: include results for vertices with a degree of zero
* `setParallelism`: override the operator parallelism
* `setReduceOnTargetId` (undirected only): the degree can be counted from either the edge source or target IDs. By default the source IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is sorted by target ID

### Edge Metrics

#### Overview
This graph analytic computes the following statistics:
- number of triangle triplets
- number of rectangle triplets
- maximum number of triangle triplets
- maximum number of rectangle triplets

#### Details
The statistics are computed over edge degrees generated from `degree.annotate.directed.EdgeDegreesPair` or
`degree.annotate.undirected.EdgeDegreePair` and grouped by vertex.

#### Usage
Directed and undirected variants are provided. The analytics take a simple graph as input and output an `AnalyticResult`
with accessor methods for the computed statistics. The graph ID type must be `Comparable`.

* `setParallelism`: override the operator parallelism
* `setReduceOnTargetId` (undirected only): the degree can be counted from either the edge source or target IDs. By default the source IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is sorted by target ID

## Similarity

### Adamic-Adar

#### Overview
Adamic-Adar measures the similarity between pairs of vertices as the sum of the inverse logarithm of degree over shared
neighbors. Scores are non-negative and unbounded. A vertex with higher degree has greater overall influence but is less
influential to each pair of neighbors.

#### Details
The algorithm first annotates each vertex with the inverse of the logarithm of the vertex degree then joins this score
onto edges by source vertex. Grouping on the source vertex, each pair of neighbors is emitted with the vertex score.
Grouping on vertex pairs, the Adamic-Adar score is summed.

See the [Jaccard Index](#jaccard-index) library method for a similar algorithm.

#### Usage
The algorithm takes a simple undirected graph as input and outputs a `DataSet` of `BinaryResult` containing two vertex
IDs and the Adamic-Adar similarity score. The graph ID type must be `Copyable`.

* `setMinimumRatio`: filter out Adamic-Adar scores less than the given ratio times the average score
* `setMinimumScore`: filter out Adamic-Adar scores less than the given minimum
* `setParallelism`: override the parallelism of operators processing small amounts of data

### Jaccard Index

#### Overview
The Jaccard Index measures the similarity between vertex neighborhoods and is computed as the number of shared neighbors
divided by the number of distinct neighbors. Scores range from 0.0 (no shared neighbors) to 1.0 (all neighbors are
shared).

#### Details
Counting shared neighbors for pairs of vertices is equivalent to counting connecting paths of length two. The number of
distinct neighbors is computed by storing the sum of degrees of the vertex pair and subtracting the count of shared
neighbors, which are double-counted in the sum of degrees.

The algorithm first annotates each edge with the target vertex's degree. Grouping on the source vertex, each pair of
neighbors is emitted with the degree sum. Grouping on vertex pairs, the shared neighbors are counted.

#### Usage
The algorithm takes a simple undirected graph as input and outputs a `DataSet` of tuples containing two vertex IDs,
the number of shared neighbors, and the number of distinct neighbors. The result class provides a method to compute the
Jaccard Index score. The graph ID type must be `Copyable`.

* `setMaximumScore`: filter out Jaccard Index scores greater than or equal to the given maximum fraction
* `setMinimumScore`: filter out Jaccard Index scores less than the given minimum fraction
* `setParallelism`: override the parallelism of operators processing small amounts of data

{{< top >}}
