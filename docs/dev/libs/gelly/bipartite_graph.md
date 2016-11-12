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

DataSet<BipartiteEdge<String, String, Double>> edges = ...

Graph<String, String, Long, Long, Double> graph = BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, env);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Scala API is not yet supported
{% endhighlight %}
</div>
</div>


* from a `DataSet` of `Tuple2` representing the edges. Gelly will convert each `Tuple2` to a `BipartiteEdge`, where the first field will be the top vertex ID, and the second field will be the bottom vertex ID. Both vertex values and edges values will be set to `NullValue`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

DataSet<Tuple2<String, String>> edges = ...

BipartiteGraph<String, String, NullValue, NullValue, NullValue> graph = BipartiteGraph.fromTuple2DataSet(edges, env);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// TODO: Should be added when Scala interface is implemented
{% endhighlight %}
</div>
</div>

* from a `DataSet` of `Tuple3` and an optional `DataSet`s of `Tuple2`. In this case, Gelly will convert each `Tuple3` to a `BipartiteEdge`, where the first field will be the top vertex ID, the second field will be the bottom vertex ID and the third field will be the edge value. Equivalently, each `Tuple2` will be converted to a `Vertex`, where the first field will be the vertex ID and the second field will be the vertex value:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

DataSet<Tuple2<Long, String>> topVertexTuples = env.readCsvFile("path/to/top/vertex/input").types(Long.class, String.class);
DataSet<Tuple2<Long, String>> bottomVertexTuples = env.readCsvFile("path/to/bottom/vertex/input").types(Long.class, String.class);

DataSet<Tuple3<Long, Long, Double>> edgeTuples = env.readCsvFile("path/to/edge/input").types(String.class, String.class, Double.class);

Graph<Long, Long, String, String, Double> graph = BipartiteGraph.fromTupleDataSet(topVertexTuples, bottomVertexTuples, edgeTuples, env);
{% endhighlight %}

* from a CSV file of Edge data and an optional CSV file of Vertex data. In this case, Gelly will convert each row from the Edge CSV file to a `BipartiteEdge`, where the first field will be the top vertex ID, the second field will be the bottom vertex ID and the third field (if present) will be the edge value. Equivalently, each row from the optional Vertex CSV files will be converted to a `Vertex`, where the first field will be the vertex ID and the second field (if present) will be the vertex value. In order to get a `BipartieGraph` from a `BipartiteGraphCsvReader` one has to specify the types, using one of the following methods:

- `types(Class<KT> topVertexKey, Class<KB> bottomVertexKey, Class<VVT> topVertexValue, Class<VVB> bottomVertexValue, Class<EV> edgeValue)`: both vertex and edge values are present.
- `edgeTypes(Class<KT> topVertexKey, Class<KB> bottomVertexKey, Class<EV> edgeValue)`: the Graph has edge values, but no vertex values.
- `vertexTypes(Class<KT> topVertexKey, Class<KB> bottomVertexKey, Class<VVT> topVertexValue, Class<VVB> bottomVertexValue)`: the Graph has vertex values, but no edge values.
- `keyType(Class<KT> topVertexKey, Class<KB> bottomVertexKey)`: the Graph has no vertex values and no edge values.

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// create a BipartiteGraph with Long Vertex IDs, String Vertex values and Double Edge values
BipartiteGraph<String, String, Long, Long, Double> graph = BipartiteGraph.fromCsvReader("path/to/top/vertex/input", "path/to/bottom/vertex/input", "path/to/edge/input", env)
					.types(Long.class, Long.class, String.class, String.class Double.class);


// create a BipartiteGraph with neither Vertex nor Edge values
BipartiteGraph<Long, String.class, NullValue, NullValue, NullValue> simpleGraph
		= BipartiteGraph.fromCsvReader("path/to/edge/input", env).keyType(Long.class, String.class);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// TODO: Add Scala examples
{% endhighlight %}
</div>
</div>

* from a CSV file of Edge data and an optional CSV file of Vertex data.
In this case, Gelly will convert each row from the Edge CSV file to an `BipartiteEdge`.
The first field of the each row will be the top vertex ID, the second field will be the bottom vertex ID and the third field (if present) will be the edge value.
If the edges have no associated value, set the edge value type parameter (3rd type argument) to `NullValue`.
You can also specify that the vertices are initialized with a vertex value.
If you provide a path to a CSV file via `pathVertices`, each row of this file will be converted to a `Vertex`.
The first field of each row will be the vertex ID and the second field will be the vertex value.
If you provide a vertex value initializer two `MapFunction`s via the `topVertexValueInitializer` and `bottomVertexValueInitializer` parameters, then these function are used to generate the vertex values.
The set of vertices will be created automatically from the edges input.
If the vertices have no associated value, set the vertex value type parameter (2nd type argument) to `NullValue`.
The vertices will then be automatically created from the edges input with vertex value of type `NullValue`.

{% highlight scala %}
// Scala API is not yet supported
{% endhighlight %}
</div>
</div>


* from a `Collection` of edges and an optional `Collection`s of top and bottom vertices:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

List<Vertex<Long, String>> topVertexList = new ArrayList...
List<Vertex<Long, String>> bottomVertexList = new ArrayList...

List<BipartiteEdge<Long, Long, String>> edgeList = new ArrayList...

Graph<Long, Long, String> graph = BipartiteGraph.fromCollection(topVertexList, bottomVertexList, edgeList, env);
{% endhighlight %}

If no vertex input is provided during BipartiteGraph creation, Gelly will automatically produce the `Vertex` `DataSet` from the edge input. In this case, the created vertices will have no values. Alternatively, you can provide two `MapFunction`s as arguments to the creation method, in order to initialize the top and bottom `Vertex` values:

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// initialize the vertex value to be equal to the vertex ID
Graph<Long, Long, Long, Long, String> graph = BipartiteGraph.fromCollection(edgeList,
				new MapFunction<Long, Long>() {
					public Long map(Long value) {
						return value;
					}
				},
				new MapFunction<Long, Long>() {
					public Long map(Long value) {
						return value;
					}
				},
				env);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// TODO: Add Scala support
{% endhighlight %}

If no vertex input is provided during Graph creation, Gelly will automatically produce the `Vertex` `DataSet` from the edge input. In this case, the created vertices will have no values. Alternatively, you can provide a `MapFunction` as an argument to the creation method, in order to initialize the `Vertex` values:

{% highlight java %}
// TODO: Add Scala support
{% endhighlight %}
</div>
</div>

{% top %}

Graph Transformations
---------------------


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
DataSet<BipartiteEdge<Long, Long, String>> edges = ...

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
