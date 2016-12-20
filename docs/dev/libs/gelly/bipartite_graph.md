---
title: Bipartite Graph
nav-parent_id: graphs
nav-pos: 6
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

<span class="label label-danger">Attention</span> Bipartite Graph currently only supported in Gelly Java API.

* This will be replaced by the TOC
{:toc}

Bipartite Graph
---------------

A bipartite graph (also called a two-mode graph) is a type of graph where vertices are separated into two disjoint sets. These sets are usually called top and bottom vertices. An edge in this graph can only connect vertices from opposite sets (i.e. bottom vertex to top vertex) and cannot connect two vertices in the same set.

These graphs have wide application in practice and can be a more natural choice for particular domains. For example to represent authorship of scientific papers top vertices can represent scientific papers while bottom nodes will represent authors. Naturally an edge between a top and a bottom nodes would represent an authorship of a particular scientific paper. Another common example for applications of bipartite graphs is relationships between actors and movies. In this case an edge represents that a particular actor played in a movie.

Bipartite graphs are used instead of regular graphs (one-mode) for the following practical [reasons](http://www.complexnetworks.fr/wp-content/uploads/2011/01/socnet07.pdf):
 * They preserve more information about a connection between vertices. For example instead of a single link between two researchers in a graph that represents that they authored a paper together a bipartite graph preserves the information about what papers they authored
 * Bipartite graphs can encode the same information more compactly than one-mode graphs
 


Graph Representation
--------------------

A `BipartiteGraph` is represented by:
 * A `DataSet` of top nodes
 * A `DataSet` of bottom nodes
 * A `DataSet` of edges between top and bottom nodes

As in the `Graph` class nodes are represented by the `Vertex` type and the same rules apply to its types and values.

The graph edges are represented by the `BipartiteEdge` type. A `BipartiteEdge` is defined by a top ID (the ID of the top `Vertex`), a bottom ID (the ID of the bottom `Vertex`) and an optional value. The main difference between the `Edge` and `BipartiteEdge` is that IDs of nodes it links can be of different types. Edges with no value have a `NullValue` value type.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BipartiteEdge<Long, String, Double> e = new BipartiteEdge<Long, String, Double>(1L, "id1", 0.5);

Double weight = e.getValue(); // weight = 0.5
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Scala API is not yet supported
{% endhighlight %}
</div>
</div>
{% top %}


Graph Creation
--------------

You can create a `BipartiteGraph` in the following ways:

* from a `DataSet` of top vertices, a `DataSet` of bottom vertices and a `DataSet` of edges:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

DataSet<Vertex<String, Long>> topVertices = ...

DataSet<Vertex<String, Long>> bottomVertices = ...

DataSet<Edge<String, String, Double>> edges = ...

Graph<String, String, Long, Long, Double> graph = BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, env);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Scala API is not yet supported
{% endhighlight %}
</div>
</div>


Graph Transformations
---------------------

* <strong>Map</strong>: Gelly provides specialized methods for applying a map transformation on the vertex values or edge values. `mapTopVertices`, `mapBottomVertices`, and `mapEdges` return a new `BipartiteGraph`, where the IDs of the vertices (or edges) remain unchanged, while the values are transformed according to the provided user-defined map function. The map functions also allow changing the type of the vertex or edge values.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BipartiteGraph<Long, Long, Long, Long, Long> graph = BipartiteGraph.fromDataSet(topVertices, bottomVertices edges, env);

// increment each vertex value by one
BipartiteGraph<Long, Long, Long, Long, Long> updatedGraph = graph.mapTopVertices(
				new MapFunction<Vertex<Long, Long>, Long>() {
					public Long map(Vertex<Long, Long> value) {
						return value.getValue() + 1;
					}
				});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Scala API is not yet supported
{% endhighlight %}
</div>
</div>

* <strong>Filter</strong>: A filter transformation applies a user-defined filter function on the vertices or edges of the `BipartiteGraph`. `filterOnEdges` will create a sub-graph of the original graph, keeping only the edges that satisfy the provided predicate. Note that the vertex dataset will not be modified. Respectively, `filterOnTopVertices` and `filterOnTopVertices` apply a filter on the vertices of the graph. Edges whose top or/and bottom IDs do not satisfy the vertex predicate are removed from the resulting edge dataset. The `subgraph` method can be used to apply a filter function to the vertices and the edges at the same time.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BipartiteGraph<Long, Long, Long, Long, Long> graph = ...

graph.subgraph(
		new FilterFunction<Vertex<Long, Long>>() {
			   	public boolean filter(Vertex<Long, Long> vertex) {
					// keep only top vertices with positive values
					return (vertex.getValue() > 0);
			   }
		   },
		new FilterFunction<Vertex<Long, Long>>() {
			   	public boolean filter(Vertex<Long, Long> vertex) {
					// keep only bottom vertices with positive values
					return (vertex.getValue() > 0);
			   }
		   },
		new FilterFunction<BipartiteEdge<Long, Long, Long>>() {
				public boolean filter(BipartiteEdge<Long, Long, Long> edge) {
					// keep only edges with negative values
					return (edge.getValue() < 0);
				}
		})
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Scala API is not yet supported
{% endhighlight %}
</div>
</div>

* <strong>Join</strong>: Gelly provides specialized methods for joining the vertex and edge datasets with other input datasets. `joinWithTopVertices` and `joinWithBottomVertices` join the vertices with a `Tuple2` input data set. The join is performed using the vertex ID and the first field of the `Tuple2` input as the join keys. The method returns a new `BipartiteGraph` where the vertex values have been updated according to a provided user-defined transformation function.
Similarly, an input dataset can be joined with the edges, using one of three methods. `joinWithEdges` expects an input `DataSet` of `Tuple3` and joins on the composite key of both top and bottom vertex IDs.
Note that if the input dataset contains a key multiple times, all Gelly join methods will only consider the first value encountered.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BipartiteGraph<Long, Long, Double, Double, Double> graph = ...

DataSet<Tuple2<Long, LongValue>> dataset = ...

BipartiteGraph<Long, Long, Double, Double, Double> networkWithWeights = graph.joinWithTopVertices(
				new VertexJoinFunction<Double, LongValue>() {
					public Double vertexJoin(Double vertexValue, LongValue inputValue) {
						return vertexValue / inputValue.getValue();
					}
				});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Scala API not yet supported
{% endhighlight %}
</div>
</div>

* <strong>Projection</strong>: Projection is a common operation for bipartite graphs that converts a bipartite graph into a regular graph. There are two types of projections: top and bottom projections. Top projection preserves only top nodes in the result graph and creates a link between them in a new graph only if there is an intermediate bottom node both top nodes connect to in the original graph. Bottom projection is the opposite to top projection, i.e. only preserves bottom nodes and connects a pair of nodes if they are connected in the original graph.

<p class="text-center">
    <img alt="Bipartite Graph Projections" width="80%" src="{{ site.baseurl }}/fig/bipartite_graph_projections.png"/>
</p>

Gelly supports two sub-types of projections: simple projections and full projections. The only difference between them is what data is associated with edges in the result graph.

In the case of a simple projection each node in the result graph contains a pair of values of bipartite edges that connect nodes in the original graph:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
// Vertices (1, "top1")
DataSet<Vertex<Long, String>> topVertices = ...

// Vertices (2, "bottom2"); (4, "bottom4")
DataSet<Vertex<Long, String>> bottomVertices = ...

// Edge that connect vertex 2 to vertex 1 and vertex 4 to vertex 1:
// (1, 2, "1-2-edge"); (1, 4, "1-4-edge")
DataSet<Edge<Long, Long, String>> edges = ...

BipartiteGraph<Long, Long, String, String, String> graph = BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, env);

// Result graph with two vertices:
// (2, "bottom2"); (4, "bottom4")
//
// and one edge that contains ids of bottom edges and a tuple with
// values of intermediate edges in the original bipartite graph:
// (2, 4, ("1-2-edge", "1-4-edge"))
Graph<Long, String, Tuple2<String, String>> graph bipartiteGraph.projectionBottomSimple();

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Scala API is not yet supported
{% endhighlight %}
</div>
</div>

Full projection preserves all the information about the connection between two vertices and stores it in `Projection` instances. This includes value and id of an intermediate vertex, source and target vertex values and source and target edge values:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
// Vertices (1, "top1")
DataSet<Vertex<Long, String>> topVertices = ...

// Vertices (2, "bottom2"); (4, "bottom4")
DataSet<Vertex<Long, String>> bottomVertices = ...

// Edge that connect vertex 2 to vertex 1 and vertex 4 to vertex 1:
// (1, 2, "1-2-edge"); (1, 4, "1-4-edge")
DataSet<Edge<Long, Long, String>> edges = ...

BipartiteGraph<Long, Long, String, String, String> graph = BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, env);

// Result graph with two vertices:
// (2, "bottom2"); (4, "bottom4")
// and one edge that contains ids of bottom edges and a tuple that 
// contains id and value of the intermediate edge, values of connected vertices
// and values of intermediate edges in the original bipartite graph:
// (2, 4, (1, "top1", "bottom2", "bottom4", "1-2-edge", "1-4-edge"))
Graph<String, String, Projection<Long, String, String, String>> graph bipartiteGraph.projectionBottomFull();

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Scala API is not yet supported
{% endhighlight %}
</div>
</div>
