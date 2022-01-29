---
title: Graph Generators
weight: 6
type: docs
aliases:
  - /dev/libs/gelly/graph_generators.html
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

# Graph Generators

Gelly provides a collection of scalable graph generators. Each generator is

* parallelizable, in order to create large datasets
* scale-free, generating the same graph regardless of parallelism
* thrifty, using as few operators as possible

Graph generators are configured using the builder pattern. The parallelism of generator
operators can be set explicitly by calling `setParallelism(parallelism)`. Lowering the
parallelism will reduce the allocation of memory and network buffers.

Graph-specific configuration must be called first, then configuration common to all
generators, and lastly the call to `generate()`. The following example configures a
grid graph with two dimensions, configures the parallelism, and generates the graph.

{{< tabs "16d10fd5-546e-4b2b-8fd0-d2c1a07a5970" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

boolean wrapEndpoints = false;

int parallelism = 4;

Graph<LongValue, NullValue, NullValue> graph = new GridGraph(env)
    .addDimension(2, wrapEndpoints)
    .addDimension(4, wrapEndpoints)
    .setParallelism(parallelism)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.GridGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

wrapEndpoints = false

val parallelism = 4

val graph = new GridGraph(env.getJavaEnv).addDimension(2, wrapEndpoints).addDimension(4, wrapEndpoints).setParallelism(parallelism).generate()
```
{{< /tab >}}
{{< /tabs >}}

## Circulant Graph

A [circulant graph](http://mathworld.wolfram.com/CirculantGraph.html) is an
[oriented graph](http://mathworld.wolfram.com/OrientedGraph.html) configured
with one or more contiguous ranges of offsets. Edges connect integer vertex IDs
whose difference equals a configured offset. The circulant graph with no offsets
is the [empty graph](#empty-graph) and the graph with the maximum range is the
[complete graph](#complete-graph).

{{< tabs "9b217b3a-3d63-4fcb-9d6a-2dcceef66497" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5;

Graph<LongValue, NullValue, NullValue> graph = new CirculantGraph(env, vertexCount)
    .addRange(1, 2)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.CirculantGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5

val graph = new CirculantGraph(env.getJavaEnv, vertexCount).addRange(1, 2).generate()
```
{{< /tab >}}
{{< /tabs >}}

## Complete Graph

An undirected graph connecting every distinct pair of vertices.

{{< tabs "44312557-0140-48d3-b166-c2f778a56d26" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5;

Graph<LongValue, NullValue, NullValue> graph = new CompleteGraph(env, vertexCount)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.CompleteGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5

val graph = new CompleteGraph(env.getJavaEnv, vertexCount).generate()
```
{{< /tab >}}
{{< /tabs >}}

## Cycle Graph

An undirected graph where the set of edges form a single cycle by connecting
each vertex to two adjacent vertices in a chained loop.

{{< tabs "83639826-1d41-4bc7-8b36-e97a89bbde3d" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5;

Graph<LongValue, NullValue, NullValue> graph = new CycleGraph(env, vertexCount)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.CycleGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5

val graph = new CycleGraph(env.getJavaEnv, vertexCount).generate()
```
{{< /tab >}}
{{< /tabs >}}

## Echo Graph

An echo graph is a
[circulant graph](#circulant-graph) with `n` vertices defined by the width of a
single range of offsets centered at `n/2`. A vertex is connected to 'far'
vertices, which connect to 'near' vertices, which connect to 'far' vertices, ....

{{< tabs "80d36990-8864-4045-8020-131244f85a8c" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5;
long vertexDegree = 2;

Graph<LongValue, NullValue, NullValue> graph = new EchoGraph(env, vertexCount, vertexDegree)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.EchoGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5
val vertexDegree = 2

val graph = new EchoGraph(env.getJavaEnv, vertexCount, vertexDegree).generate()
```
{{< /tab >}}
{{< /tabs >}}

## Empty Graph

A graph containing no edges.

{{< tabs "5bdbd0e5-78f2-477e-9178-f082cb903ac4" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5;

Graph<LongValue, NullValue, NullValue> graph = new EmptyGraph(env, vertexCount)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.EmptyGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5

val graph = new EmptyGraph(env.getJavaEnv, vertexCount).generate()
```
{{< /tab >}}
{{< /tabs >}}

## Grid Graph

An undirected graph connecting vertices in a regular tiling in one or more dimensions.
Each dimension is configured separately. When the dimension size is at least three the
endpoints are optionally connected by setting `wrapEndpoints`. Changing the following
example to `addDimension(4, true)` would connect `0` to `3` and `4` to `7`.

{{< tabs "b0e6a768-24c2-4b6e-9b82-ac2e6a6c3c28" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

boolean wrapEndpoints = false;

Graph<LongValue, NullValue, NullValue> graph = new GridGraph(env)
    .addDimension(2, wrapEndpoints)
    .addDimension(4, wrapEndpoints)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.GridGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val wrapEndpoints = false

val graph = new GridGraph(env.getJavaEnv).addDimension(2, wrapEndpoints).addDimension(4, wrapEndpoints).generate()
```
{{< /tab >}}
{{< /tabs >}}

## Hypercube Graph

An undirected graph where edges form an `n`-dimensional hypercube. Each vertex
in a hypercube connects to one other vertex in each dimension.

{{< tabs "a8665a56-d94f-4f6a-956e-87affa74e4d7" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long dimensions = 3;

Graph<LongValue, NullValue, NullValue> graph = new HypercubeGraph(env, dimensions)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.HypercubeGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val dimensions = 3

val graph = new HypercubeGraph(env.getJavaEnv, dimensions).generate()
```
{{< /tab >}}
{{< /tabs >}}

## Path Graph

An undirected graph where the set of edges form a single path by connecting
two `endpoint` vertices with degree `1` and all midpoint vertices with degree
`2`. A path graph can be formed by removing a single edge from a cycle graph.

{{< tabs "a4da2589-9aba-477f-a0cf-e5fae58607da" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5

Graph<LongValue, NullValue, NullValue> graph = new PathGraph(env, vertexCount)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.PathGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5

val graph = new PathGraph(env.getJavaEnv, vertexCount).generate()
```
{{< /tab >}}
{{< /tabs >}}

## RMat Graph

A directed power-law multigraph generated using the
[Recursive Matrix (R-Mat)](http://www.cs.cmu.edu/~christos/PUBLICATIONS/siam04.pdf) model.

RMat is a stochastic generator configured with a source of randomness implementing the
`RandomGenerableFactory` interface. Provided implementations are `JDKRandomGeneratorFactory`
and `MersenneTwisterFactory`. These generate an initial sequence of random values which are
then used as seeds for generating the edges.

{{< tabs "f48cd996-9b34-4460-905d-21a9537c88fc" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

int vertexCount = 1 << scale;
int edgeCount = edgeFactor * vertexCount;

Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.RMatGraph

val env = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 1 << scale
val edgeCount = edgeFactor * vertexCount

val graph = new RMatGraph(env.getJavaEnv, rnd, vertexCount, edgeCount).generate()
```
{{< /tab >}}
{{< /tabs >}}

The default RMat constants can be overridden as shown in the following example.
The constants define the interdependence of bits from each generated edge's source
and target labels. The RMat noise can be enabled and progressively perturbs the
constants while generating each edge.

The RMat generator can be configured to produce a simple graph by removing self-loops
and duplicate edges. Symmetrization is performed either by a "clip-and-flip" throwing away
the half matrix above the diagonal or a full "flip" preserving and mirroring all edges.

{{< tabs "c513c2df-1d6c-4fdd-8de3-7222c6f3e5ae" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

int vertexCount = 1 << scale;
int edgeCount = edgeFactor * vertexCount;

boolean clipAndFlip = false;

Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
    .setConstants(0.57f, 0.19f, 0.19f)
    .setNoise(true, 0.10f)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.RMatGraph

val env = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 1 << scale
val edgeCount = edgeFactor * vertexCount

clipAndFlip = false

val graph = new RMatGraph(env.getJavaEnv, rnd, vertexCount, edgeCount).setConstants(0.57f, 0.19f, 0.19f).setNoise(true, 0.10f).generate()
```
{{< /tab >}}
{{< /tabs >}}

## Singleton Edge Graph

An undirected graph containing isolated two-paths where every vertex has degree
`1`.

{{< tabs "0c7a337f-6b43-41ac-86ea-6960907c7193" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexPairCount = 4

// note: configured with the number of vertex pairs
Graph<LongValue, NullValue, NullValue> graph = new SingletonEdgeGraph(env, vertexPairCount)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.SingletonEdgeGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexPairCount = 4

// note: configured with the number of vertex pairs
val graph = new SingletonEdgeGraph(env.getJavaEnv, vertexPairCount).generate()
```
{{< /tab >}}
{{< /tabs >}}

## Star Graph

An undirected graph containing a single central vertex connected to all other leaf vertices.

{{< tabs "f3f77c46-b110-489d-8d36-351e58ce0a4c" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 6;

Graph<LongValue, NullValue, NullValue> graph = new StarGraph(env, vertexCount)
    .generate();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.StarGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 6

val graph = new StarGraph(env.getJavaEnv, vertexCount).generate()
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
