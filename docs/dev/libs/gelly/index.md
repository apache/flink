---
title: "Gelly: Flink Graph API"
nav-id: graphs
nav-show_overview: true
nav-title: "Graphs: Gelly"
nav-parent_id: libs
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

Gelly is a Graph API for Flink. It contains a set of methods and utilities which aim to simplify the development of graph analysis applications in Flink. In Gelly, graphs can be transformed and modified using high-level functions similar to the ones provided by the batch processing API. Gelly provides methods to create, transform and modify graphs, as well as a library of graph algorithms.

{:#markdown-toc}
* [Graph API](graph_api.html)
* [Iterative Graph Processing](iterative_graph_processing.html)
* [Library Methods](library_methods.html)
* [Graph Algorithms](graph_algorithms.html)
* [Graph Generators](graph_generators.html)
* [Bipartite Graphs](bipartite_graph.html)

Using Gelly
-----------

Gelly is currently part of the *libraries* Maven project. All relevant classes are located in the *org.apache.flink.graph* package.

Add the following dependency to your `pom.xml` to use Gelly.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-gelly{{ site.scala_version_suffix }}</artifactId>
    <version>{{site.version}}</version>
</dependency>
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-gelly-scala{{ site.scala_version_suffix }}</artifactId>
    <version>{{site.version}}</version>
</dependency>
{% endhighlight %}
</div>
</div>

Note that Gelly is not part of the binary distribution. See [linking]({% link dev/project-configuration.md %}) for
instructions on packaging Gelly libraries into Flink user programs.

The remaining sections provide a description of available methods and present several examples of how to use Gelly and how to mix it with the Flink DataSet API.

Running Gelly Examples
----------------------

The Gelly library jars are provided in the [Flink distribution](https://flink.apache.org/downloads.html "Apache Flink: Downloads")
in the **opt** directory (for versions older than Flink 1.2 these can be manually downloaded from
[Maven Central](http://search.maven.org/#search|ga|1|flink%20gelly)). To run the Gelly examples the **flink-gelly** (for
Java) or **flink-gelly-scala** (for Scala) jar must be copied to Flink's **lib** directory.

{% highlight bash %}
cp opt/flink-gelly_*.jar lib/
cp opt/flink-gelly-scala_*.jar lib/
{% endhighlight %}

Gelly's examples jar includes drivers for each of the library methods and is provided in the **examples** directory.
After configuring and starting the cluster, list the available algorithm classes:

{% highlight bash %}
./bin/start-cluster.sh
./bin/flink run examples/gelly/flink-gelly-examples_*.jar
{% endhighlight %}

The Gelly drivers can generate graph data or read the edge list from a CSV file (each node in a cluster must have access
to the input file). The algorithm description, available inputs and outputs, and configuration are displayed when an
algorithm is selected. Print usage for [JaccardIndex](./library_methods.html#jaccard-index):

{% highlight bash %}
./bin/flink run examples/gelly/flink-gelly-examples_*.jar --algorithm JaccardIndex
{% endhighlight %}

Display [graph metrics](./library_methods.html#metric) for a million vertex graph:

{% highlight bash %}
./bin/flink run examples/gelly/flink-gelly-examples_*.jar \
    --algorithm GraphMetrics --order directed \
    --input RMatGraph --type integer --scale 20 --simplify directed \
    --output print
{% endhighlight %}

The size of the graph is adjusted by the *\-\-scale* and *\-\-edge_factor* parameters. The
[library generator](./graph_generators.html#rmat-graph) provides access to additional configuration to adjust the
power-law skew and random noise.

Sample social network data is provided by the [Stanford Network Analysis Project](http://snap.stanford.edu/data/index.html).
The [com-lj](http://snap.stanford.edu/data/bigdata/communities/com-lj.ungraph.txt.gz) data set is a good starter size.
Run a few algorithms and monitor the job progress in Flink's Web UI:

{% highlight bash %}
wget -O - http://snap.stanford.edu/data/bigdata/communities/com-lj.ungraph.txt.gz | gunzip -c > com-lj.ungraph.txt

./bin/flink run -q examples/gelly/flink-gelly-examples_*.jar \
    --algorithm GraphMetrics --order undirected \
    --input CSV --type integer --simplify undirected --input_filename com-lj.ungraph.txt --input_field_delimiter $'\t' \
    --output print

./bin/flink run -q examples/gelly/flink-gelly-examples_*.jar \
    --algorithm ClusteringCoefficient --order undirected \
    --input CSV --type integer --simplify undirected --input_filename com-lj.ungraph.txt --input_field_delimiter $'\t' \
    --output hash

./bin/flink run -q examples/gelly/flink-gelly-examples_*.jar \
    --algorithm JaccardIndex \
    --input CSV --type integer --simplify undirected --input_filename com-lj.ungraph.txt --input_field_delimiter $'\t' \
    --output hash
{% endhighlight %}

Please submit feature requests and report issues on the user [mailing list](https://flink.apache.org/community.html#mailing-lists)
or [Flink Jira](https://issues.apache.org/jira/browse/FLINK). We welcome suggestions for new algorithms and features as
well as [code contributions](https://flink.apache.org/contributing/contribute-code.html).

{% top %}
