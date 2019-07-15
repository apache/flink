---
title: Examples
nav-id: examples
nav-title: '<i class="fa fa-file-code-o title appetizer" aria-hidden="true"></i> Examples'
nav-parent_id: getting-started
nav-pos: 3
nav-show_overview: true
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


## Bundled Examples

The Flink sources include many examples for Flink's different APIs:

* DataStream applications ({% gh_link flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples "Java" %} / {% gh_link flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples "Scala" %}) 
* DataSet applications ({% gh_link flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java "Java" %} / {% gh_link flink-examples/flink-examples-batch/src/main/scala/org/apache/flink/examples/scala "Scala" %})
* Table API / SQL queries ({% gh_link flink-examples/flink-examples-table/src/main/java/org/apache/flink/table/examples/java "Java" %} / {% gh_link flink-examples/flink-examples-table/src/main/scala/org/apache/flink/table/examples/scala "Scala" %})

These [instructions]({{ site.baseurl }}/dev/batch/examples.html#running-an-example) explain how to run the examples.

## Examples on the Web

There are also a few blog posts published online that discuss example applications:

* [How to build stateful streaming applications with Apache Flink
](https://www.infoworld.com/article/3293426/big-data/how-to-build-stateful-streaming-applications-with-apache-flink.html) presents an event-driven application implemented with the DataStream API and two SQL queries for streaming analytics.

* [Building real-time dashboard applications with Apache Flink, Elasticsearch, and Kibana](https://www.elastic.co/blog/building-real-time-dashboard-applications-with-apache-flink-elasticsearch-and-kibana) is a blog post at elastic.co showing how to build a real-time dashboard solution for streaming data analytics using Apache Flink, Elasticsearch, and Kibana.

* The [Flink training website](https://training.ververica.com/) from Ververica has a number of examples. Check out the hands-on sections and the exercises.

{% top %}
