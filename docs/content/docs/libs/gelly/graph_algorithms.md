---
title: Graph Algorithms
weight: 5
type: docs
aliases:
  - /dev/libs/gelly/graph_algorithms.html
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

# Graph Algorithms

The logic blocks with which the `Graph` API and top-level algorithms are assembled are accessible in Gelly as graph
algorithms in the `org.apache.flink.graph.asm` package. These algorithms provide optimization and tuning through
configuration parameters and may provide implicit runtime reuse when processing the same input with a similar
configuration.

## VertexInDegree

Annoate vertices of a directed graph with the in-degree.

```java
DataSet<Vertex<K, LongValue>> inDegree = graph
  .run(new VertexInDegree().setIncludeZeroDegreeVertices(true));
```

Optional Configuration:

* **setIncludeZeroDegreeVertices**: by default only the edge set is processed for the computation of degree; when this flag is set an additional join is performed against the vertex set in order to output vertices with an in-degree of zero.

* **setParallelism**: override the operator parallelism

## VertexOutDegree

Annotate vertices of a directed graph with the out-degree.

```java
DataSet<Vertex<K, LongValue>> outDegree = graph
  .run(new VertexOutDegree().setIncludeZeroDegreeVertices(true));
```

Optional Configuration:

* **setIncludeZeroDegreeVertices**: by default only the edge set is processed for the computation of degree; when this flag is set an additional join is performed against the vertex set in order to output vertices with an out-degree of zero.

* **setParallelism**: override the operator parallelism

## VertexDegrees

Annotate vertices of a directed graph with the degree, out-degree, and in-degree.

```java
DataSet<Vertex<K, Tuple2<LongValue, LongValue>>> degrees = graph
  .run(new VertexDegrees().setIncludeZeroDegreeVertices(true));
```

Optional configuration:

* **setIncludeZeroDegreeVertices**: by default only the edge set is processed for the computation of degree; when this flag is set an additional join is performed against the vertex set in order to output vertices with out- and in-degree of zero.

* **setParallelism**: override the operator parallelism.

## EdgeSourceDegrees

Annotate edges of a directed graph with the degree, out-degree, and in-degree of the source ID.

```java
DataSet<Edge<K, Tuple2<EV, Degrees>>> sourceDegrees = graph
  .run(new EdgeSourceDegrees());
```

Optional configuration:

* **setParallelism**: override the operator parallelism.

## EdgeTargetDegrees

Annotate edges of a directed graph with the degree, out-degree, and in-degree of the target ID.

```java
DataSet<Edge<K, Tuple2<EV, Degrees>>> targetDegrees = graph
  .run(new EdgeTargetDegrees());
```

Optional configuration:

* **setParallelism**: override the operator parallelism.

## EdgeDegreesPair

Annotate edges of a directed graph with the degree, out-degree, and in-degree of both the source and target vertices.

```java
DataSet<Vertex<K, LongValue>> degree = graph
  .run(new VertexDegree()
    .setIncludeZeroDegreeVertices(true)
    .setReduceOnTargetId(true));
```

Optional Configuration:

* **setIncludeZeroDegreeVertices**: by default only the edge set is processed for the computation of degree; when this flag is set an additional join is performed against the vertex set in order to output vertices with a degree of zero

* **setParallelism**: override the operator parallelism

* **setReduceOnTargetId**: the degree can be counted from either the edge source or target IDs. By default the source IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is sorted by target ID.

## EdgeSourceDegree

Annotate edges of an undirected graph with degree of the source ID.

```java
DataSet<Edge<K, Tuple2<EV, LongValue>>> sourceDegree = graph
  .run(new EdgeSourceDegree()
    .setReduceOnTargetId(true));
```

Optional Configuration:

* **setParallelism**: override the operator parallelism

* **setReduceOnTargetId**: the degree can be counted from either the edge source or target IDs. By default the source IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is sorted by target ID.

## EdgeTargetDegree

Annotate edges of an undirected graph with degree of the target ID.

```java
DataSet<Edge<K, Tuple2<EV, LongValue>>> targetDegree = graph
  .run(new EdgeTargetDegree()
    .setReduceOnSourceId(true));
```

Optional configuration:

* **setParallelism**: override the operator parallelism

* **setReduceOnSourceId**: the degree can be counted from either the edge source or target IDs. By default the target IDs are counted. Reducing on source IDs may optimize the algorithm if the input edge list is sorted by source ID.

## EdgeDegreePair

Annotate edges of an undirected graph with the degree of both the source and target vertices.

```java
DataSet<Edge<K, Tuple3<EV, LongValue, LongValue>>> pairDegree = graph
  .run(new EdgeDegreePair().setReduceOnTargetId(true));
```

Optional configuration:

* **setParallelism**: override the operator parallelism

* **setReduceOnTargetId**: the degree can be counted from either the edge source or target IDs. By default the source IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is sorted by target ID.

## MaximumDegree

Filter an undirected graph by maximum degree.

```java
Graph<K, VV, EV> filteredGraph = graph
  .run(new MaximumDegree(5000)
    .setBroadcastHighDegreeVertices(true)
    .setReduceOnTargetId(true));
```

Optional configuration:

* **setBroadcastHighDegreeVertices**: join high-degree vertices using a broadcast-hash to reduce data shuffling when removing a relatively small number of high-degree vertices.

* **setParallelism**: override the operator parallelism

* **setReduceOnTargetId**: the degree can be counted from either the edge source or target IDs. By default the source IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is sorted by target ID.

## Simplify

Remove self-loops and duplicate edges from a directed graph.

```java
graph.run(new Simplify());
```

## TranslateGraphIds

Translate vertex and edge IDs using the given `TranslateFunction`.

```java
graph.run(new TranslateGraphIds(new LongValueToStringValue()));
```

Required configuration:

* **translator**: implements type or value conversion

Optional configuration:

* **setParallelism**: override the operator parallelism

## TranslateVertexValues

Translate vertex values using the given `TranslateFunction`.

```java
graph.run(new TranslateVertexValues(new LongValueAddOffset(vertexCount)));
```

Required configuration:

* **translator**: implements type or value conversion

Optional configuration:

* **setParallelism**: override the operator parallelism

## TranslateEdgeValues

Translate edge values using the given `TranslateFunction`.

```java
graph.run(new TranslateEdgeValues(new Nullify()));
```

Required configuration:

* **translator**: implements type or value conversion

Optional configuration:

* **setParallelism**: override the operator parallelism

{{< top >}}