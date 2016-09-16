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

Note that Gelly is currently not part of the binary distribution. See linking with it for cluster execution [here]({{ site.baseurl }}/dev/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

The remaining sections provide a description of available methods and present several examples of how to use Gelly and how to mix it with the Flink DataSet API. After reading this guide, you might also want to check the {% gh_link /flink-libraries/flink-gelly-examples/ "Gelly examples" %}.

{% top %}
