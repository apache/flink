---
title: Library Methods
nav-parent_id: graphs
nav-pos: 3
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

Gelly has a growing collection of graph algorithms for easily analyzing large-scale Graphs.

* This will be replaced by the TOC
{:toc}

Gelly's library methods can be used by simply calling the `run()` method on the input graph:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

Graph<Long, Long, NullValue> graph = ...

// run Label Propagation for 30 iterations to detect communities on the input graph
DataSet<Vertex<Long, Long>> verticesWithCommunity = graph.run(new LabelPropagation<Long>(30));

// print the result
verticesWithCommunity.print();

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment

val graph: Graph[java.lang.Long, java.lang.Long, NullValue] = ...

// run Label Propagation for 30 iterations to detect communities on the input graph
val verticesWithCommunity = graph.run(new LabelPropagation[java.lang.Long, java.lang.Long, NullValue](30))

// print the result
verticesWithCommunity.print

{% endhighlight %}
</div>
</div>

## Community Detection

#### Overview
In graph theory, communities refer to groups of nodes that are well connected internally, but sparsely connected to other groups.
This library method is an implementation of the community detection algorithm described in the paper [Towards real-time community detection in large networks](http://arxiv.org/pdf/0808.2633.pdf%22%3Earticle%20explaining%20the%20algorithm%20in%20detail).

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

## PageRank

#### Overview
An implementation of a simple [PageRank algorithm](https://en.wikipedia.org/wiki/PageRank), using [scatter-gather iterations](#scatter-gather-iterations).
PageRank is an algorithm that was first used to rank web search engine results. Today, the algorithm and many variations, are used in various graph application domains. The idea of PageRank is that important or relevant pages tend to link to other important pages.

#### Details
The algorithm operates in iterations, where pages distribute their scores to their neighbors (pages they have links to) and subsequently update their scores based on the partial values they receive. The implementation assumes that each page has at least one incoming and one outgoing link.
In order to consider the importance of a link from one page to another, scores are divided by the total number of out-links of the source page. Thus, a page with 10 links will distribute 1/10 of its score to each neighbor, while a page with 100 links, will distribute 1/100 of its score to each neighboring page. This process computes what is often called the transition probablities, i.e. the probability that some page will lead to other page while surfing the web. To correctly compute the transition probabilities, this implementation expects the edge values to be initialised to 1.0.

#### Usage
The algorithm takes as input a `Graph` with any vertex type, `Double` vertex values, and `Double` edge values. Edges values should be initialized to 1.0, in order to correctly compute the transition probabilities. Otherwise, the transition probability for an Edge `(u, v)` will be set to the edge value divided by `u`'s out-degree. The algorithm returns a `DataSet` of vertices, where the vertex value corresponds to assigned rank after convergence (or maximum iterations).
The constructors take the following parameters:

* `beta`: the damping factor.
* `maxIterations`: the maximum number of iterations to run.

## GSA PageRank

The algorithm is implemented using [gather-sum-apply iterations](#gather-sum-apply-iterations).

See the [PageRank](#pagerank) library method for implementation details and usage information.

## Single Source Shortest Paths

#### Overview
An implementation of the Single-Source-Shortest-Paths algorithm for weighted graphs. Given a source vertex, the algorithm computes the shortest paths from this source to all other nodes in the graph.

#### Details
The algorithm is implemented using [scatter-gather iterations](#scatter-gather-iterations).
In each iteration, a vertex sends to its neighbors a message containing the sum its current distance and the edge weight connecting this vertex with the neighbor. Upon receiving candidate distance messages, a vertex calculates the minimum distance and, if a shorter path has been discovered, it updates its value. If a vertex does not change its value during a superstep, then it does not produce messages for its neighbors for the next superstep. The computation terminates after the specified maximum number of supersteps or when there are no value updates.

#### Usage
The algorithm takes as input a `Graph` with any vertex type, `Double` vertex values, and `Double` edge values. The output is a `DataSet` of vertices where the vertex values
correspond to the minimum distances from the given source vertex.
The constructor takes two parameters:

* `srcVertexId` The vertex ID of the source vertex.
* `maxIterations`: the maximum number of iterations to run.

## GSA Single Source Shortest Paths

The algorithm is implemented using [gather-sum-apply iterations](#gather-sum-apply-iterations).

See the [Single Source Shortest Paths](#single-source-shortest-paths) library method for implementation details and usage information.

## Triangle Count

#### Overview
An analytic for counting the number of unique triangles in a graph.

#### Details
Counts the triangles generated by [Triangle Listing](#triangle-listing).

#### Usage
The analytic takes an undirected graph as input and returns as a result a `Long` corresponding to the number of triangles
in the graph. The graph ID type must be `Comparable` and `Copyable`.

## Triangle Listing

This algorithm supports object reuse. The graph ID type must be `Comparable` and `Copyable`.

See the [Triangle Enumerator](#triangle-enumerator) library method for implementation details.

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

## Hyperlink-Induced Topic Search

#### Overview
[Hyperlink-Induced Topic Search](http://www.cs.cornell.edu/home/kleinber/auth.pdf) (HITS, or "Hubs and Authorities")
computes two interdependent scores for every vertex in a directed graph. Good hubs are those which point to many
good authorities and good authorities are those pointed to by many good hubs.

#### Details
Every vertex is assigned the same initial hub and authority scores. The algorithm then iteratively updates the scores
until termination. During each iteration new hub scores are computed from the authority scores, then new authority
scores are computed from the new hub scores. The scores are then normalized and optionally tested for convergence.

#### Usage
The algorithm takes a directed graph as input and outputs a `DataSet` of `Tuple3` containing the vertex ID, hub score,
and authority score.

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

## Adamic-Adar

#### Overview
Adamic-Adar measures the similarity between pairs of vertices as the sum of the inverse logarithm of degree over shared
neighbors. Scores are non-negative and unbounded. A vertex with higher degree has greater overall influence but is less
influential to each pair of neighbors.

#### Details
The algorithm first annotates each vertex with the inverse of the logarithm of the vertex degree then joins this score
onto edges by source vertex. Grouping on the source vertex, each pair of neighbors is emitted with the vertex score.
Grouping on two-paths, the Adamic-Adar score is summed.

See the [Jaccard Index](#jaccard-index) library method for a similar algorithm.

#### Usage
The algorithm takes a simple, undirected graph as input and outputs a `DataSet` of tuples containing two vertex IDs and
the Adamic-Adair similarity score. The graph ID type must be `Comparable` and `Copyable`.

* `setLittleParallelism`: override the parallelism of operators processing small amounts of data
* `setMinimumRatio`: filter out Adamic-Adar scores less than the given ratio times the average score
* `setMinimumScore`: filter out Adamic-Adar scores less than the given minimum

## Jaccard Index

#### Overview
The Jaccard Index measures the similarity between vertex neighborhoods and is computed as the number of shared neighbors
divided by the number of distinct neighbors. Scores range from 0.0 (no shared neighbors) to 1.0 (all neighbors are
shared).

#### Details
Counting shared neighbors for pairs of vertices is equivalent to counting connecting paths of length two. The number of
distinct neighbors is computed by storing the sum of degrees of the vertex pair and subtracting the count of shared
neighbors, which are double-counted in the sum of degrees.

The algorithm first annotates each edge with the target vertex's degree. Grouping on the source vertex, each pair of
neighbors is emitted with the degree sum. Grouping on two-paths, the shared neighbors are counted.

#### Usage
The algorithm takes a simple, undirected graph as input and outputs a `DataSet` of tuples containing two vertex IDs,
the number of shared neighbors, and the number of distinct neighbors. The result class provides a method to compute the
Jaccard Index score. The graph ID type must be `Comparable` and `Copyable`.

* `setLittleParallelism`: override the parallelism of operators processing small amounts of data
* `setMaximumScore`: filter out Jaccard Index scores greater than or equal to the given maximum fraction
* `setMinimumScore`: filter out Jaccard Index scores less than the given minimum fraction

## Local Clustering Coefficient

#### Overview
The local clustering coefficient measures the connectedness of each vertex's neighborhood. Scores range from 0.0 (no
edges between neighbors) to 1.0 (neighborhood is a clique).

#### Details
An edge between a vertex's neighbors is a triangle. Counting edges between neighbors is equivalent to counting the
number of triangles which include the vertex. The clustering coefficient score is the number of edges between neighbors
divided by the number of potential edges between neighbors.

See the [Triangle Enumeration](#triangle-enumeration) library method for a detailed explanation of triangle enumeration.

#### Usage
Directed and undirected variants are provided. The algorithms take a simple graph as input and output a `DataSet` of
tuples containing the vertex ID, vertex degree, and number of triangles containing the vertex. The graph ID type must be
`Comparable` and `Copyable`.

## Global Clustering Coefficient

#### Overview
The global clustering coefficient measures the connectedness of a graph. Scores range from 0.0 (no edges between
neighbors) to 1.0 (complete graph).

#### Details
See the [Local Clustering Coefficient](#local-clustering-coefficient) library method for a detailed explanation of
clustering coefficient.

#### Usage
Directed and undirected variants are provided. The algorithm takes a simple graph as input and outputs a result
containing the total number of triplets and triangles in the graph. The graph ID type must be `Comparable` and
`Copyable`.


{% top %}
