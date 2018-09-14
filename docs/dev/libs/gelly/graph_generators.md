---
title: Graph Generators
nav-parent_id: graphs
nav-pos: 5
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

* This will be replaced by the TOC
{:toc}

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

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

boolean wrapEndpoints = false;

int parallelism = 4;

Graph<LongValue, NullValue, NullValue> graph = new GridGraph(env)
    .addDimension(2, wrapEndpoints)
    .addDimension(4, wrapEndpoints)
    .setParallelism(parallelism)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.GridGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

wrapEndpoints = false

val parallelism = 4

val graph = new GridGraph(env.getJavaEnv).addDimension(2, wrapEndpoints).addDimension(4, wrapEndpoints).setParallelism(parallelism).generate()
{% endhighlight %}
</div>
</div>

## Circulant Graph

A [circulant graph](http://mathworld.wolfram.com/CirculantGraph.html) is an
[oriented graph](http://mathworld.wolfram.com/OrientedGraph.html) configured
with one or more contiguous ranges of offsets. Edges connect integer vertex IDs
whose difference equals a configured offset. The circulant graph with no offsets
is the [empty graph](#empty-graph) and the graph with the maximum range is the
[complete graph](#complete-graph).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5;

Graph<LongValue, NullValue, NullValue> graph = new CirculantGraph(env, vertexCount)
    .addRange(1, 2)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.CirculantGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5

val graph = new CirculantGraph(env.getJavaEnv, vertexCount).addRange(1, 2).generate()
{% endhighlight %}
</div>
</div>

## Complete Graph

An undirected graph connecting every distinct pair of vertices.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5;

Graph<LongValue, NullValue, NullValue> graph = new CompleteGraph(env, vertexCount)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.CompleteGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5

val graph = new CompleteGraph(env.getJavaEnv, vertexCount).generate()
{% endhighlight %}
</div>
</div>

<svg class="graph" width="540" height="540"
    xmlns="http://www.w3.org/2000/svg"
    xmlns:xlink="http://www.w3.org/1999/xlink">

    <line x1="270" y1="40" x2="489" y2="199" />
    <line x1="270" y1="40" x2="405" y2="456" />
    <line x1="270" y1="40" x2="135" y2="456" />
    <line x1="270" y1="40" x2="51" y2="199" />

    <line x1="489" y1="199" x2="405" y2="456" />
    <line x1="489" y1="199" x2="135" y2="456" />
    <line x1="489" y1="199" x2="51" y2="199" />

    <line x1="405" y1="456" x2="135" y2="456" />
    <line x1="405" y1="456" x2="51" y2="199" />

    <line x1="135" y1="456" x2="51" y2="199" />

    <circle cx="270" cy="40" r="20" />
    <text x="270" y="40">0</text>

    <circle cx="489" cy="199" r="20" />
    <text x="489" y="199">1</text>

    <circle cx="405" cy="456" r="20" />
    <text x="405" y="456">2</text>

    <circle cx="135" cy="456" r="20" />
    <text x="135" y="456">3</text>

    <circle cx="51" cy="199" r="20" />
    <text x="51" y="199">4</text>
</svg>

## Cycle Graph

An undirected graph where the set of edges form a single cycle by connecting
each vertex to two adjacent vertices in a chained loop.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5;

Graph<LongValue, NullValue, NullValue> graph = new CycleGraph(env, vertexCount)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.CycleGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5

val graph = new CycleGraph(env.getJavaEnv, vertexCount).generate()
{% endhighlight %}
</div>
</div>

<svg class="graph" width="540" height="540"
    xmlns="http://www.w3.org/2000/svg"
    xmlns:xlink="http://www.w3.org/1999/xlink">

    <line x1="270" y1="40" x2="489" y2="199" />
    <line x1="489" y1="199" x2="405" y2="456" />
    <line x1="405" y1="456" x2="135" y2="456" />
    <line x1="135" y1="456" x2="51" y2="199" />
    <line x1="51" y1="199" x2="270" y2="40" />

    <circle cx="270" cy="40" r="20" />
    <text x="270" y="40">0</text>

    <circle cx="489" cy="199" r="20" />
    <text x="489" y="199">1</text>

    <circle cx="405" cy="456" r="20" />
    <text x="405" y="456">2</text>

    <circle cx="135" cy="456" r="20" />
    <text x="135" y="456">3</text>

    <circle cx="51" cy="199" r="20" />
    <text x="51" y="199">4</text>
</svg>

## Echo Graph

An [echo graph](http://mathworld.wolfram.com/EchoGraph.html) is a
[circulant graph](#circulant-graph) with `n` vertices defined by the width of a
single range of offsets centered at `n/2`. A vertex is connected to 'far'
vertices, which connect to 'near' vertices, which connect to 'far' vertices, ....

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5;
long vertexDegree = 2;

Graph<LongValue, NullValue, NullValue> graph = new EchoGraph(env, vertexCount, vertexDegree)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.EchoGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5
val vertexDegree = 2

val graph = new EchoGraph(env.getJavaEnv, vertexCount, vertexDegree).generate()
{% endhighlight %}
</div>
</div>

## Empty Graph

A graph containing no edges.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5;

Graph<LongValue, NullValue, NullValue> graph = new EmptyGraph(env, vertexCount)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.EmptyGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5

val graph = new EmptyGraph(env.getJavaEnv, vertexCount).generate()
{% endhighlight %}
</div>
</div>

<svg class="graph" width="540" height="80"
    xmlns="http://www.w3.org/2000/svg"
    xmlns:xlink="http://www.w3.org/1999/xlink">

    <circle cx="30" cy="40" r="20" />
    <text x="30" y="40">0</text>

    <circle cx="150" cy="40" r="20" />
    <text x="150" y="40">1</text>

    <circle cx="270" cy="40" r="20" />
    <text x="270" y="40">2</text>

    <circle cx="390" cy="40" r="20" />
    <text x="390" y="40">3</text>

    <circle cx="510" cy="40" r="20" />
    <text x="510" y="40">4</text>
</svg>

## Grid Graph

An undirected graph connecting vertices in a regular tiling in one or more dimensions.
Each dimension is configured separately. When the dimension size is at least three the
endpoints are optionally connected by setting `wrapEndpoints`. Changing the following
example to `addDimension(4, true)` would connect `0` to `3` and `4` to `7`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

boolean wrapEndpoints = false;

Graph<LongValue, NullValue, NullValue> graph = new GridGraph(env)
    .addDimension(2, wrapEndpoints)
    .addDimension(4, wrapEndpoints)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.GridGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val wrapEndpoints = false

val graph = new GridGraph(env.getJavaEnv).addDimension(2, wrapEndpoints).addDimension(4, wrapEndpoints).generate()
{% endhighlight %}
</div>
</div>

<svg class="graph" width="540" height="200"
    xmlns="http://www.w3.org/2000/svg"
    xmlns:xlink="http://www.w3.org/1999/xlink">

    <line x1="30" y1="40" x2="510" y2="40" />
    <line x1="30" y1="160" x2="510" y2="160" />

    <line x1="30" y1="40" x2="30" y2="160" />
    <line x1="190" y1="40" x2="190" y2="160" />
    <line x1="350" y1="40" x2="350" y2="160" />
    <line x1="510" y1="40" x2="510" y2="160" />

    <circle cx="30" cy="40" r="20" />
    <text x="30" y="40">0</text>

    <circle cx="190" cy="40" r="20" />
    <text x="190" y="40">1</text>

    <circle cx="350" cy="40" r="20" />
    <text x="350" y="40">2</text>

    <circle cx="510" cy="40" r="20" />
    <text x="510" y="40">3</text>

    <circle cx="30" cy="160" r="20" />
    <text x="30" y="160">4</text>

    <circle cx="190" cy="160" r="20" />
    <text x="190" y="160">5</text>

    <circle cx="350" cy="160" r="20" />
    <text x="350" y="160">6</text>

    <circle cx="510" cy="160" r="20" />
    <text x="510" y="160">7</text>
</svg>

## Hypercube Graph

An undirected graph where edges form an `n`-dimensional hypercube. Each vertex
in a hypercube connects to one other vertex in each dimension.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long dimensions = 3;

Graph<LongValue, NullValue, NullValue> graph = new HypercubeGraph(env, dimensions)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.HypercubeGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val dimensions = 3

val graph = new HypercubeGraph(env.getJavaEnv, dimensions).generate()
{% endhighlight %}
</div>
</div>

<svg class="graph" width="540" height="320"
    xmlns="http://www.w3.org/2000/svg"
    xmlns:xlink="http://www.w3.org/1999/xlink">

    <line x1="190" y1="120" x2="350" y2="120" />
    <line x1="190" y1="200" x2="350" y2="200" />
    <line x1="190" y1="120" x2="190" y2="200" />
    <line x1="350" y1="120" x2="350" y2="200" />

    <line x1="30" y1="40" x2="510" y2="40" />
    <line x1="30" y1="280" x2="510" y2="280" />
    <line x1="30" y1="40" x2="30" y2="280" />
    <line x1="510" y1="40" x2="510" y2="280" />

    <line x1="190" y1="120" x2="30" y2="40" />
    <line x1="350" y1="120" x2="510" y2="40" />
    <line x1="190" y1="200" x2="30" y2="280" />
    <line x1="350" y1="200" x2="510" y2="280" />

    <circle cx="190" cy="120" r="20" />
    <text x="190" y="120">0</text>

    <circle cx="350" cy="120" r="20" />
    <text x="350" y="120">1</text>

    <circle cx="190" cy="200" r="20" />
    <text x="190" y="200">2</text>

    <circle cx="350" cy="200" r="20" />
    <text x="350" y="200">3</text>

    <circle cx="30" cy="40" r="20" />
    <text x="30" y="40">4</text>

    <circle cx="510" cy="40" r="20" />
    <text x="510" y="40">5</text>

    <circle cx="30" cy="280" r="20" />
    <text x="30" y="280">6</text>

    <circle cx="510" cy="280" r="20" />
    <text x="510" y="280">7</text>
</svg>

## Path Graph

An undirected graph where the set of edges form a single path by connecting
two `endpoint` vertices with degree `1` and all midpoint vertices with degree
`2`. A path graph can be formed by removing a single edge from a cycle graph.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 5

Graph<LongValue, NullValue, NullValue> graph = new PathGraph(env, vertexCount)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.PathGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 5

val graph = new PathGraph(env.getJavaEnv, vertexCount).generate()
{% endhighlight %}
</div>
</div>

<svg class="graph" width="540" height="80"
    xmlns="http://www.w3.org/2000/svg"
    xmlns:xlink="http://www.w3.org/1999/xlink">

    <line x1="30" y1="40" x2="510" y2="40" />

    <circle cx="30" cy="40" r="20" />
    <text x="30" y="40">0</text>

    <circle cx="150" cy="40" r="20" />
    <text x="150" y="40">1</text>

    <circle cx="270" cy="40" r="20" />
    <text x="270" y="40">2</text>

    <circle cx="390" cy="40" r="20" />
    <text x="390" y="40">3</text>

    <circle cx="510" cy="40" r="20" />
    <text x="510" y="40">4</text>
</svg>

## RMat Graph

A directed power-law multigraph generated using the
[Recursive Matrix (R-Mat)](http://www.cs.cmu.edu/~christos/PUBLICATIONS/siam04.pdf) model.

RMat is a stochastic generator configured with a source of randomness implementing the
`RandomGenerableFactory` interface. Provided implementations are `JDKRandomGeneratorFactory`
and `MersenneTwisterFactory`. These generate an initial sequence of random values which are
then used as seeds for generating the edges.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

int vertexCount = 1 << scale;
int edgeCount = edgeFactor * vertexCount;

Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.RMatGraph

val env = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 1 << scale
val edgeCount = edgeFactor * vertexCount

val graph = new RMatGraph(env.getJavaEnv, rnd, vertexCount, edgeCount).generate()
{% endhighlight %}
</div>
</div>

The default RMat constants can be overridden as shown in the following example.
The constants define the interdependence of bits from each generated edge's source
and target labels. The RMat noise can be enabled and progressively perturbs the
constants while generating each edge.

The RMat generator can be configured to produce a simple graph by removing self-loops
and duplicate edges. Symmetrization is performed either by a "clip-and-flip" throwing away
the half matrix above the diagonal or a full "flip" preserving and mirroring all edges.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

int vertexCount = 1 << scale;
int edgeCount = edgeFactor * vertexCount;

boolean clipAndFlip = false;

Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
    .setConstants(0.57f, 0.19f, 0.19f)
    .setNoise(true, 0.10f)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.RMatGraph

val env = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 1 << scale
val edgeCount = edgeFactor * vertexCount

clipAndFlip = false

val graph = new RMatGraph(env.getJavaEnv, rnd, vertexCount, edgeCount).setConstants(0.57f, 0.19f, 0.19f).setNoise(true, 0.10f).generate()
{% endhighlight %}
</div>
</div>

## Singleton Edge Graph

An undirected graph containing isolated two-paths where every vertex has degree
`1`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexPairCount = 4

// note: configured with the number of vertex pairs
Graph<LongValue, NullValue, NullValue> graph = new SingletonEdgeGraph(env, vertexPairCount)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.SingletonEdgeGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexPairCount = 4

// note: configured with the number of vertex pairs
val graph = new SingletonEdgeGraph(env.getJavaEnv, vertexPairCount).generate()
{% endhighlight %}
</div>
</div>

<svg class="graph" width="540" height="200"
    xmlns="http://www.w3.org/2000/svg"
    xmlns:xlink="http://www.w3.org/1999/xlink">

    <line x1="30" y1="40" x2="190" y2="40" />
    <line x1="350" y1="40" x2="510" y2="40" />
    <line x1="30" y1="160" x2="190" y2="160" />
    <line x1="350" y1="160" x2="510" y2="160" />

    <circle cx="30" cy="40" r="20" />
    <text x="30" y="40">0</text>

    <circle cx="190" cy="40" r="20" />
    <text x="190" y="40">1</text>

    <circle cx="350" cy="40" r="20" />
    <text x="350" y="40">2</text>

    <circle cx="510" cy="40" r="20" />
    <text x="510" y="40">3</text>

    <circle cx="30" cy="160" r="20" />
    <text x="30" y="160">4</text>

    <circle cx="190" cy="160" r="20" />
    <text x="190" y="160">5</text>

    <circle cx="350" cy="160" r="20" />
    <text x="350" y="160">6</text>

    <circle cx="510" cy="160" r="20" />
    <text x="510" y="160">7</text>
</svg>

## Star Graph

An undirected graph containing a single central vertex connected to all other leaf vertices.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

long vertexCount = 6;

Graph<LongValue, NullValue, NullValue> graph = new StarGraph(env, vertexCount)
    .generate();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.graph.generator.StarGraph

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

val vertexCount = 6

val graph = new StarGraph(env.getJavaEnv, vertexCount).generate()
{% endhighlight %}
</div>
</div>

<svg class="graph" width="540" height="540"
    xmlns="http://www.w3.org/2000/svg"
    xmlns:xlink="http://www.w3.org/1999/xlink">

    <line x1="270" y1="270" x2="270" y2="40" />
    <line x1="270" y1="270" x2="489" y2="199" />
    <line x1="270" y1="270" x2="405" y2="456" />
    <line x1="270" y1="270" x2="135" y2="456" />
    <line x1="270" y1="270" x2="51" y2="199" />

    <circle cx="270" cy="270" r="20" />
    <text x="270" y="270">0</text>

    <circle cx="270" cy="40" r="20" />
    <text x="270" y="40">1</text>

    <circle cx="489" cy="199" r="20" />
    <text x="489" y="199">2</text>

    <circle cx="405" cy="456" r="20" />
    <text x="405" y="456">3</text>

    <circle cx="135" cy="456" r="20" />
    <text x="135" y="456">4</text>

    <circle cx="51" cy="199" r="20" />
    <text x="51" y="199">5</text>
</svg>

{% top %}
