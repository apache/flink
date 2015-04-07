---
title: "Gelly: Flink Graph API"
is_beta: true
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

<a href="#top"></a>

Gelly is a Java Graph API for Flink. It contains a set of methods and utilities which aim to simplify the development of graph analysis applications in Flink. In Gelly, graphs can be transformed and modified using high-level functions similar to the ones provided by the batch processing API. Gelly provides methods to create, transform and modify graphs, as well as a library of graph algorithms.

* This will be replaced by the TOC
{:toc}

Using Gelly
-----------

Gelly is currently part of the *staging* Maven project. All relevant classes are located in the *org.apache.flink.graph* package.

Add the following dependency to your `pom.xml` to use Gelly.

~~~xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-gelly</artifactId>
    <version>{{site.version}}</version>
</dependency>
~~~

The remaining sections provide a description of available methods and present several examples of how to use Gelly and how to mix it with the Flink Java API. After reading this guide, you might also want to check the {% gh_link /flink-staging/flink-gelly/src/main/java/org/apache/flink/graph/example/ "Gelly examples" %}.

Graph Representation
-----------

In Gelly, a `Graph` is represented by a `DataSet` of vertices and a `DataSet` of edges.

The `Graph` nodes are represented by the `Vertex` type. A `Vertex` is defined by a unique ID and a value. `Vertex` IDs should implement the `Comparable` interface. Vertices without value can be represented by setting the value type to `NullValue`.

{% highlight java %}
// create a new vertex with a Long ID and a String value
Vertex<Long, String> v = new Vertex<Long, String>(1L, "foo");

// create a new vertex with a Long ID and no value
Vertex<Long, NullValue> v = new Vertex<Long, NullValue>(1L, NullValue.getInstance());
{% endhighlight %}

The graph edges are represented by the `Edge` type. An `Edge` is defined by a source ID (the ID of the source `Vertex`), a target ID (the ID of the target `Vertex`) and an optional value. The source and target IDs should be of the same type as the `Vertex` IDs. Edges with no value have a `NullValue` value type.

{% highlight java %}
Edge<Long, Double> e = new Edge<Long, Double>(1L, 2L, 0.5);

// reverse the source and target of this edge
Edge<Long, Double> reversed = e.reverse();

Double weight = e.getValue(); // weight = 0.5
{% endhighlight %}

[Back to top](#top)

Graph Creation
-----------

You can create a `Graph` in the following ways:

* from a `DataSet` of edges and an optional `DataSet` of vertices:

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

DataSet<Vertex<String, Long>> vertices = ...

DataSet<Edge<String, Double>> edges = ...

Graph<String, Long, Double> graph = Graph.fromDataSet(vertices, edges, env);
{% endhighlight %}

* from a `DataSet` of `Tuple3` and an optional `DataSet` of `Tuple2`. In this case, Gelly will convert each `Tuple3` to an `Edge`, where the first field will be the source ID, the second field will be the target ID and the third field will be the edge value. Equivalently, each `Tuple2` will be converted to a `Vertex`, where the first field will be the vertex ID and the second field will be the vertex value:

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

DataSet<Tuple2<String, Long>> vertexTuples = env.readCsvFile("path/to/vertex/input");

DataSet<Tuple3<String, String, Double>> edgeTuples = env.readCsvFile("path/to/edge/input");

Graph<String, Long, Double> graph = Graph.fromTupleDataSet(vertexTuples, edgeTuples, env);
{% endhighlight %}

* from a `Collection` of edges and an optional `Collection` of vertices:

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

List<Vertex<Long, Long>> vertexList = new ArrayList...

List<Edge<Long, String>> edgeList = new ArrayList...

Graph<Long, Long, String> graph = Graph.fromCollection(vertexList, edgeList, env);
{% endhighlight %}

If no vertex input is provided during Graph creation, Gelly will automatically produce the `Vertex` `DataSet` from the edge input. In this case, the created vertices will have no values. Alternatively, you can provide a `MapFunction` as an argument to the creation method, in order to initialize the `Vertex` values:

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// initialize the vertex value to be equal to the vertex ID
Graph<Long, Long, String> graph = Graph.fromCollection(edges, 
				new MapFunction<Long, Long>() {
					public Long map(Long value) { 
						return value; 
					} 
				}, env);
{% endhighlight %}

[Back to top](#top)

Graph Properties
------------

Gelly includes the following methods for retrieving various Graph properties and metrics:

{% highlight java %}
// get the Vertex DataSet
DataSet<Vertex<K, VV>> getVertices()

// get the Edge DataSet
DataSet<Edge<K, EV>> getEdges()

// get the IDs of the vertices as a DataSet
DataSet<K> getVertexIds()

// get the source-target pairs of the edge IDs as a DataSet
DataSet<Tuple2<K, K>> getEdgeIds() 

// get a DataSet of <vertex ID, in-degree> pairs for all vertices
DataSet<Tuple2<K, Long>> inDegrees() 

// get a DataSet of <vertex ID, out-degree> pairs for all vertices
DataSet<Tuple2<K, Long>> outDegrees()

// get a DataSet of <vertex ID, degree> pairs for all vertices, where degree is the sum of in- and out- degrees
DataSet<Tuple2<K, Long>> getDegrees()

// get the number of vertices
long numberOfVertices()

// get the number of edges
long numberOfEdges()

// get a DataSet of Triplets<srcVertex, trgVertex, edge>
DataSet<Triplet<K, VV, EV>> getTriplets()

{% endhighlight %}

[Back to top](#top)

Graph Transformations
-----------------

* <strong>Map</strong>: Gelly provides specialized methods for applying a map transformation on the vertex values or edge values. `mapVertices` and `mapEdges` return a new `Graph`, where the IDs of the vertices (or edges) remain unchanged, while the values are transformed according to the provided user-defined map function. The map functions also allow changing the type of the vertex or edge values.

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
Graph<Long, Long, Long> graph = Graph.fromDataSet(vertices, edges, env);

// increment each vertex value by one
Graph<Long, Long, Long> updatedGraph = graph.mapVertices(
				new MapFunction<Vertex<Long, Long>, Long>() {
					public Long map(Vertex<Long, Long> value) {
						return value.getValue() + 1;
					}
				});
{% endhighlight %}

* <strong>Filter</strong>: A filter transformation applies a user-defined filter function on the vertices or edges of the `Graph`. `filterOnEdges` will create a sub-graph of the original graph, keeping only the edges that satisfy the provided predicate. Note that the vertex dataset will not be modified. Respectively, `filterOnVertices` applies a filter on the vertices of the graph. Edges whose source and/or target do not satisfy the vertex predicate are removed from the resulting edge dataset. The `subgraph` method can be used to apply a filter function to the vertices and the edges at the same time.

{% highlight java %}
Graph<Long, Long, Long> graph = ...

graph.subgraph(
		new FilterFunction<Vertex<Long, Long>>() {
			   	public boolean filter(Vertex<Long, Long> vertex) {
					// keep only vertices with positive values
					return (vertex.getValue() > 0);
			   }
		   },
		new FilterFunction<Edge<Long, Long>>() {
				public boolean filter(Edge<Long, Long> edge) {
					// keep only edges with negative values
					return (edge.getValue() < 0);
				}
		})
{% endhighlight %}

<p class="text-center">
    <img alt="Filter Transformations" width="80%" src="fig/gelly-filter.png"/>
</p>

* <strong>Join</strong>: Gelly provides specialized methods for joining the vertex and edge datasets with other input datasets. `joinWithVertices` joins the vertices with a `Tuple2` input data set. The join is performed using the vertex ID and the first field of the `Tuple2` input as the join keys. The method returns a new `Graph` where the vertex values have been updated according to a provided user-defined map function.
Similarly, an input dataset can be joined with the edges, using one of three methods. `joinWithEdges` expects an input `DataSet` of `Tuple3` and joins on the composite key of both source and target vertex IDs. `joinWithEdgesOnSource` expects a `DataSet` of `Tuple2` and joins on the source key of the edges and the first attribute of the input dataset and `joinWithEdgesOnTarget` expects a `DataSet` of `Tuple2` and joins on the target key of the edges and the first attribute of the input dataset. All three methods apply a map function on the edge and the input data set values.
Note that if the input dataset contains a key multiple times, all Gelly join methods will only consider the first value encountered.

{% highlight java %}
Graph<Long, Double, Double> network = ...

DataSet<Tuple2<Long, Long>> vertexOutDegrees = network.outDegrees();

// assign the transition probabilities as the edge weights
Graph<Long, Double, Double> networkWithWeights = network.joinWithEdgesOnSource(vertexOutDegrees,
				new MapFunction<Tuple2<Double, Long>, Double>() {
					public Double map(Tuple2<Double, Long> value) {
						return value.f0 / value.f1;
					}
				});
{% endhighlight %}

* <strong>Reverse</strong>: the `reverse()` method returns a new `Graph` where the direction of all edges has been reversed.

* <strong>Undirected</strong>: In Gelly, a `Graph` is always directed. Undirected graphs can be represented by adding all opposite-direction edges to a graph. For this purpose, Gelly provides the `getUndirected()` method.

* <strong>Union</strong>: Gelly's `union()` method performs a union on the vertex and edges sets of the input graphs. Duplicate vertices are removed from the resulting `Graph`, while if duplicate edges exists, these will be maintained.

<p class="text-center">
    <img alt="Union Transformation" width="50%" src="fig/gelly-union.png"/>
</p>

[Back to top](#top)

Graph Mutations
-----------

Gelly includes the following methods for adding and removing vertices and edges from an input `Graph`:

{% highlight java %}
// adds a Vertex and the given edges to the Graph. If the Vertex already exists, it will not be added again, but the given edges will.
Graph<K, VV, EV> addVertex(final Vertex<K, VV> vertex, List<Edge<K, EV>> edges)

// adds an Edge to the Graph. If the source and target vertices do not exist in the graph, they will also be added.
Graph<K, VV, EV> addEdge(Vertex<K, VV> source, Vertex<K, VV> target, EV edgeValue)

// removes the given Vertex and its edges from the Graph.
Graph<K, VV, EV> removeVertex(Vertex<K, VV> vertex)

// removes *all* edges that match the given Edge from the Graph.
Graph<K, VV, EV> removeEdge(Edge<K, EV> edge)
{% endhighlight %}

Neighborhood Methods
-----------

Neighborhood methods allow vertices to perform an aggregation on their first-hop neighborhood.

`groupReduceOnEdges()` can be used to compute an aggregation on the neighboring edges of a vertex, while `groupReduceOnNeighbors()` has access on both the neighboring edges and vertices. The neighborhood scope is defined by the `EdgeDirection` parameter, which takes the values `IN`, `OUT` or `ALL`. `IN` will gather all in-coming edges (neighbors) of a vertex, `OUT` will gather all out-going edges (neighbors), while `ALL` will gather all edges (neighbors).

The `groupReduceOnEdges()` and `groupReduceOnNeighbors()` methods return zero, one or more values per vertex.
When returning a single value per vertex, `reduceOnEdges()` or `reduceOnNeighbors()` should be called as they are more efficient.

For example, assume that you want to select the minimum weight of all out-edges for each vertex in the following graph:

<p class="text-center">
    <img alt="reduceOnEdges Example" width="50%" src="fig/gelly-example-graph.png"/>
</p>

The following code will collect the out-edges for each vertex and apply the `SelectMinWeight()` user-defined function on each of the resulting neighborhoods:

{% highlight java %}
Graph<Long, Long, Double> graph = ...

DataSet<Tuple2<Long, Double>> minWeights = graph.groupReduceOnEdges(
				new SelectMinWeight(), EdgeDirection.OUT);

// user-defined function to select the minimum weight
static final class SelectMinWeightNeighbor implements EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges, Collector<Tuple2<Long, Long>> out) throws Exception {

			long weight = Long.MAX_VALUE;
			long minNeighborId = 0;

			for (Edge<Long, Long> edge: edges) {
				if (edge.getValue() < weight) {
					weight = edge.getValue();
					minNeighborId = edge.getTarget();
				}
			}
			out.collect(new Tuple2<Long, Long>(v.getId(), minNeighborId));
		}
	}
{% endhighlight %}

<p class="text-center">
    <img alt="reduceOnEdges Example" width="50%" src="fig/gelly-reduceOnEdges.png"/>
</p>

Similarly, assume that you would like to compute the sum of the values of all in-coming neighbors, for every vertex. The following code will collect the in-coming neighbors for each vertex and apply the `SumValues()` user-defined function on each neighborhood:

{% highlight java %}
Graph<Long, Long, Double> graph = ...

DataSet<Tuple2<Long, Long>> verticesWithSum = graph.reduceOnNeighbors(
				new SumValues(), EdgeDirection.IN);

// user-defined function to sum the neighbor values
static final class SumValues implements ReduceNeighborsFunction<Long, Long, Double> {

    public Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>> reduceNeighbors(Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>> firstNeighbor,
    																		Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>> secondNeighbor) {

    	long sum = firstNeighbor.f2.getValue() + secondNeighbor.f2.getValue();
    	return new Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>>(firstNeighbor.f0, firstNeighbor.f1,
    			new Vertex<Long, Long>(firstNeighbor.f0, sum));
    }
}
{% endhighlight %}

<p class="text-center">
    <img alt="reduceOnNeighbors Example" width="70%" src="img/gelly-reduceOnNeighbors.png"/>
</p>

When the aggregation computation does not require access to the vertex value (for which the aggregation is performed), it is advised to use the more efficient `EdgesFunction` and `NeighborsFunction` for the user-defined functions. When access to the vertex value is required, one should use `EdgesFunctionWithVertexValue` and `NeighborsFunctionWithVertexValue` instead.

[Back to top](#top)

Vertex-centric Iterations
-----------

Gelly wraps Flink's [Spargel API](spargel_guide.html) to provide methods for vertex-centric iterations.
Like in Spargel, the user only needs to implement two functions: a `VertexUpdateFunction`, which defines how a vertex will update its value
based on the received messages and a `MessagingFunction`, which allows a vertex to send out messages for the next superstep.
These functions and the maximum number of iterations to run are given as parameters to Gelly's `runVertexCentricIteration`.
This method will execute the vertex-centric iteration on the input Graph and return a new Graph, with updated vertex values:

{% highlight java %}
Graph<Long, Double, Double> graph = ...

// run Single-Source-Shortest-Paths vertex-centric iteration
Graph<Long, Double, Double> result = 
			graph.runVertexCentricIteration(
			new VertexDistanceUpdater(), new MinDistanceMessenger(), maxIterations);

// user-defined functions
public static final class VertexDistanceUpdater {...}
public static final class MinDistanceMessenger {...}

{% endhighlight %}

### Configuring a Vertex-Centric Iteration
A vertex-centric iteration can be configured using an `IterationConfiguration` object.
Currently, the following parameters can be specified:

* <strong>Name</strong>: The name for the vertex-centric iteration. The name is displayed in logs and messages 
and can be specified using the `setName()` method.

* <strong>Parallelism</strong>: The parallelism for the iteration. It can be set using the `setParallelism()` method.	

* <strong>Solution set in unmanaged memory</strong>: Defines whether the solution set is kept in managed memory (Flink's internal way of keeping objects in serialized form) or as a simple object map. By default, the solution set runs in managed memory. This property can be set using the `setSolutionSetUnmanagedMemory()` method.

* <strong>Aggregators</strong>: Iteration aggregators can be registered using the `registerAggregator()` method. An iteration aggregator combines
all aggregates globally once per superstep and makes them available in the next superstep. Registered aggregators can be accessed inside the user-defined `VertexUpdateFunction` and `MessagingFunction`.

* <strong>Broadcast Variables</strong>: DataSets can be added as [Broadcast Variables](programming_guide.html#broadcast-variables) to the `VertexUpdateFunction` and `MessagingFunction`, using the `addBroadcastSetForUpdateFunction()` and `addBroadcastSetForMessagingFunction()` methods, respectively.

{% highlight java %}

Graph<Long, Double, Double> graph = ...

// configure the iteration
IterationConfiguration parameters = new IterationConfiguration();

// set the iteration name
parameters.setName("Gelly Iteration");

// set the parallelism
parameters.setParallelism(16);

// register an aggregator
parameters.registerAggregator("sumAggregator", new LongSumAggregator());

// run the vertex-centric iteration, also passing the configuration parameters
Graph<Long, Double, Double> result = 
			graph.runVertexCentricIteration(
			new VertexUpdater(), new Messenger(), maxIterations, parameters);

// user-defined functions
public static final class VertexUpdater extends VertexUpdateFunction {

	LongSumAggregator aggregator = new LongSumAggregator();

	public void preSuperstep() {
	
		// retrieve the Aggregator
		aggregator = getIterationAggregator("sumAggregator");
	}


	public void updateVertex(Long vertexKey, Long vertexValue, MessageIterator inMessages) {
		
		//do some computation
		Long partialValue = ...

		// aggregate the partial value
		aggregator.aggregate(partialValue);

		// update the vertex value
		setNewVertexValue(...);
	}
}

public static final class Messenger extends MessagingFunction {...}

{% endhighlight %}

[Back to top](#top)

Graph Validation
-----------

Gelly provides a simple utility for performing validation checks on input graphs. Depending on the application context, a graph may or may not be valid according to certain criteria. For example, a user might need to validate whether their graph contains duplicate edges or whether its structure is bipartite. In order to validate a graph, one can define a custom `GraphValidator` and implement its `validate()` method. `InvalidVertexIdsValidator` is Gelly's pre-defined validator. It checks that the edge set contains valid vertex IDs, i.e. that all edge IDs
also exist in the vertex IDs set.

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// create a list of vertices with IDs = {1, 2, 3, 4, 5}
List<Vertex<Long, Long>> vertices = ...

// create a list of edges with IDs = {(1, 2) (1, 3), (2, 4), (5, 6)}
List<Edge<Long, Long>> edges = ...

Graph<Long, Long, Long> graph = Graph.fromCollection(vertices, edges, env);

// will return false: 6 is an invalid ID
graph.validate(new InvalidVertexIdsValidator<Long, Long, Long>()); 

{% endhighlight %}

[Back to top](#top)

Library Methods
-----------
Gelly has a growing collection of graph algorithms for easily analyzing large-scale Graphs. So far, the following library methods are implemented:

* PageRank
* Single-Source Shortest Paths
* Label Propagation
* Simple Community Detection

Gelly's library methods can be used by simply calling the `run()` method on the input graph:

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

Graph<Long, Long, NullValue> graph = ...

// run Label Propagation for 30 iterations to detect communities on the input graph
DataSet<Vertex<Long, Long>> verticesWithCommunity = graph.run(
				new LabelPropagation<Long>(30)).getVertices();

// print the result
verticesWithCommunity.print();

env.execute();
{% endhighlight %}

[Back to top](#top)


