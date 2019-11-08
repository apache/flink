---
title: Graph Algorithms
nav-parent_id: graphs
nav-pos: 4
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

The logic blocks with which the `Graph` API and top-level algorithms are assembled are accessible in Gelly as graph
algorithms in the `org.apache.flink.graph.asm` package. These algorithms provide optimization and tuning through
configuration parameters and may provide implicit runtime reuse when processing the same input with a similar
configuration.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Algorithm</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>degree.annotate.directed.<br/><strong>VertexInDegree</strong></td>
      <td>
        <p>Annotate vertices of a <a href="#graph-representation">directed graph</a> with the in-degree.</p>
{% highlight java %}
DataSet<Vertex<K, LongValue>> inDegree = graph
  .run(new VertexInDegree()
    .setIncludeZeroDegreeVertices(true));
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setIncludeZeroDegreeVertices</strong>: by default only the edge set is processed for the computation of degree; when this flag is set an additional join is performed against the vertex set in order to output vertices with an in-degree of zero</p></li>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>degree.annotate.directed.<br/><strong>VertexOutDegree</strong></td>
      <td>
        <p>Annotate vertices of a <a href="#graph-representation">directed graph</a> with the out-degree.</p>
{% highlight java %}
DataSet<Vertex<K, LongValue>> outDegree = graph
  .run(new VertexOutDegree()
    .setIncludeZeroDegreeVertices(true));
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setIncludeZeroDegreeVertices</strong>: by default only the edge set is processed for the computation of degree; when this flag is set an additional join is performed against the vertex set in order to output vertices with an out-degree of zero</p></li>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>degree.annotate.directed.<br/><strong>VertexDegrees</strong></td>
      <td>
        <p>Annotate vertices of a <a href="#graph-representation">directed graph</a> with the degree, out-degree, and in-degree.</p>
{% highlight java %}
DataSet<Vertex<K, Tuple2<LongValue, LongValue>>> degrees = graph
  .run(new VertexDegrees()
    .setIncludeZeroDegreeVertices(true));
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setIncludeZeroDegreeVertices</strong>: by default only the edge set is processed for the computation of degree; when this flag is set an additional join is performed against the vertex set in order to output vertices with out- and in-degree of zero</p></li>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>degree.annotate.directed.<br/><strong>EdgeSourceDegrees</strong></td>
      <td>
        <p>Annotate edges of a <a href="#graph-representation">directed graph</a> with the degree, out-degree, and in-degree of the source ID.</p>
{% highlight java %}
DataSet<Edge<K, Tuple2<EV, Degrees>>> sourceDegrees = graph
  .run(new EdgeSourceDegrees());
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>degree.annotate.directed.<br/><strong>EdgeTargetDegrees</strong></td>
      <td>
        <p>Annotate edges of a <a href="#graph-representation">directed graph</a> with the degree, out-degree, and in-degree of the target ID.</p>
{% highlight java %}
DataSet<Edge<K, Tuple2<EV, Degrees>>> targetDegrees = graph
  .run(new EdgeTargetDegrees();
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>degree.annotate.directed.<br/><strong>EdgeDegreesPair</strong></td>
      <td>
        <p>Annotate edges of a <a href="#graph-representation">directed graph</a> with the degree, out-degree, and in-degree of both the source and target vertices.</p>
{% highlight java %}
DataSet<Edge<K, Tuple2<EV, Degrees>>> degrees = graph
  .run(new EdgeDegreesPair());
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>degree.annotate.undirected.<br/><strong>VertexDegree</strong></td>
      <td>
        <p>Annotate vertices of an <a href="#graph-representation">undirected graph</a> with the degree.</p>
{% highlight java %}
DataSet<Vertex<K, LongValue>> degree = graph
  .run(new VertexDegree()
    .setIncludeZeroDegreeVertices(true)
    .setReduceOnTargetId(true));
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setIncludeZeroDegreeVertices</strong>: by default only the edge set is processed for the computation of degree; when this flag is set an additional join is performed against the vertex set in order to output vertices with a degree of zero</p></li>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
          <li><p><strong>setReduceOnTargetId</strong>: the degree can be counted from either the edge source or target IDs. By default the source IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is sorted by target ID.</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>degree.annotate.undirected.<br/><strong>EdgeSourceDegree</strong></td>
      <td>
        <p>Annotate edges of an <a href="#graph-representation">undirected graph</a> with degree of the source ID.</p>
{% highlight java %}
DataSet<Edge<K, Tuple2<EV, LongValue>>> sourceDegree = graph
  .run(new EdgeSourceDegree()
    .setReduceOnTargetId(true));
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
          <li><p><strong>setReduceOnTargetId</strong>: the degree can be counted from either the edge source or target IDs. By default the source IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is sorted by target ID.</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>degree.annotate.undirected.<br/><strong>EdgeTargetDegree</strong></td>
      <td>
        <p>Annotate edges of an <a href="#graph-representation">undirected graph</a> with degree of the target ID.</p>
{% highlight java %}
DataSet<Edge<K, Tuple2<EV, LongValue>>> targetDegree = graph
  .run(new EdgeTargetDegree()
    .setReduceOnSourceId(true));
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
          <li><p><strong>setReduceOnSourceId</strong>: the degree can be counted from either the edge source or target IDs. By default the target IDs are counted. Reducing on source IDs may optimize the algorithm if the input edge list is sorted by source ID.</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>degree.annotate.undirected.<br/><strong>EdgeDegreePair</strong></td>
      <td>
        <p>Annotate edges of an <a href="#graph-representation">undirected graph</a> with the degree of both the source and target vertices.</p>
{% highlight java %}
DataSet<Edge<K, Tuple3<EV, LongValue, LongValue>>> pairDegree = graph
  .run(new EdgeDegreePair()
    .setReduceOnTargetId(true));
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
          <li><p><strong>setReduceOnTargetId</strong>: the degree can be counted from either the edge source or target IDs. By default the source IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is sorted by target ID.</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>degree.filter.undirected.<br/><strong>MaximumDegree</strong></td>
      <td>
        <p>Filter an <a href="#graph-representation">undirected graph</a> by maximum degree.</p>
{% highlight java %}
Graph<K, VV, EV> filteredGraph = graph
  .run(new MaximumDegree(5000)
    .setBroadcastHighDegreeVertices(true)
    .setReduceOnTargetId(true));
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setBroadcastHighDegreeVertices</strong>: join high-degree vertices using a broadcast-hash to reduce data shuffling when removing a relatively small number of high-degree vertices.</p></li>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
          <li><p><strong>setReduceOnTargetId</strong>: the degree can be counted from either the edge source or target IDs. By default the source IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is sorted by target ID.</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>simple.directed.<br/><strong>Simplify</strong></td>
      <td>
        <p>Remove self-loops and duplicate edges from a <a href="#graph-representation">directed graph</a>.</p>
{% highlight java %}
graph.run(new Simplify());
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>simple.undirected.<br/><strong>Simplify</strong></td>
      <td>
        <p>Add symmetric edges and remove self-loops and duplicate edges from an <a href="#graph-representation">undirected graph</a>.</p>
{% highlight java %}
graph.run(new Simplify());
{% endhighlight %}
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>translate.<br/><strong>TranslateGraphIds</strong></td>
      <td>
        <p>Translate vertex and edge IDs using the given <code>TranslateFunction</code>.</p>
{% highlight java %}
graph.run(new TranslateGraphIds(new LongValueToStringValue()));
{% endhighlight %}
        <p>Required configuration:</p>
        <ul>
          <li><p><strong>translator</strong>: implements type or value conversion</p></li>
        </ul>
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>translate.<br/><strong>TranslateVertexValues</strong></td>
      <td>
        <p>Translate vertex values using the given <code>TranslateFunction</code>.</p>
{% highlight java %}
graph.run(new TranslateVertexValues(new LongValueAddOffset(vertexCount)));
{% endhighlight %}
        <p>Required configuration:</p>
        <ul>
          <li><p><strong>translator</strong>: implements type or value conversion</p></li>
        </ul>
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
        </ul>
      </td>
    </tr>

    <tr>
      <td>translate.<br/><strong>TranslateEdgeValues</strong></td>
      <td>
        <p>Translate edge values using the given <code>TranslateFunction</code>.</p>
{% highlight java %}
graph.run(new TranslateEdgeValues(new Nullify()));
{% endhighlight %}
        <p>Required configuration:</p>
        <ul>
          <li><p><strong>translator</strong>: implements type or value conversion</p></li>
        </ul>
        <p>Optional configuration:</p>
        <ul>
          <li><p><strong>setParallelism</strong>: override the operator parallelism</p></li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>

{% top %}
