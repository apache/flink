---
title: "Apache Flink Documentation"
nav-pos: 0
nav-title: '<i class="fa fa-home title" aria-hidden="true"></i> Home'
nav-parent_id: root
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

This documentation is for Apache Flink version {{ site.version }}.

Apache Flink is an open source platform for distributed stream and batch data processing. Flink’s core is a streaming dataflow engine that provides data distribution, communication, and fault tolerance for distributed computations over data streams. Flink also builds batch processing on top of the streaming engine, overlaying native iteration support, managed memory, and program optimization.

## First Steps

- **Concepts**: Start with the basic concepts of Flink's [Dataflow Programming Model]({{ site.baseurl }}/concepts/programming-model.html) and [Distributed Runtime Environment]({{ site.baseurl }}/concepts/runtime.html). This will help you to fully understand the other parts of the documentation, including the setup and programming guides. It is highly recommended to read these sections first.

- **Quickstarts**: [Run an example program](quickstart/setup_quickstart.html) on your local machine or [write a simple program](quickstart/run_example_quickstart.html) working on live Wikipedia edits.

- **Setup:** The [local]({{ site.baseurl }}/setup/local_setup.html), [cluster](setup/cluster_setup.html), and [cloud](setup/gce_setup.html) setup guides show you how to deploy Flink.

- **Programming Guides**: You can check out our guides about [basic concepts](dev/api_concepts.html) and the [DataStream API](dev/datastream_api.html) or [DataSet API](dev/batch/index.html) to learn how to write your first Flink programs.

{: style="color: red"}
## TODO

* improve this page
* give the quickstart more love
* find a way to raise the visibility of rich functions
* fix bugs
  * find and fix broken links
  * [fault tolerance](dev/batch/fault_tolerance) mixes batch and streaming in a confusing way
  * [rescaling figure is confusing](fig/rescale.svg)
  * the [info about mongodb](dev/batch/connectors) seems to be stale. There's an indirect pointer to https://flink.incubator.apache.org/news/2014/01/28/querying_mongodb.html which doesn't exist
* break up the streaming page somewhat to better group material that goes with content on other pages, and to raise the discoverability of certain topics (since the sidebar navigation can only link to whole pages)
* also break up and reorganize [Basic API Concepts](dev/api_concepts) somewhat
* [batch#dataset-transformations](http://localhost:4000/dev/batch/#dataset-transformations) has strong overlap with [batch/dataset_transformations](http://localhost:4000/dev/batch/dataset_transformations.html). Not sure what to do about it.
* gather together (some of) the material on debugging
* the explanation of windowAll needs love
* move the material on [custom serializers](http://localhost:4000/monitoring/best_practices.html#register-a-custom-serializer-for-your-flink-program) into the [section on serialization](dev/types_serialization)
* [local execution](dev/local_execution) and [cluster execution](dev/cluster_execution) have already been moved under batch, because their current content is batch specific. It's not clear these pages should still exist. Some of their content has already been generalized elsewhere (eg [linking with flink](dev/api_concepts.html#linking-with-flink)).
* [data streaming fault tolerance](internals/stream_checkpointing) is very nice. Maybe it should be promoted into the application development section (and out of internals).
* add more examples, eg
  * connected streams (from training slides)
  * point to the [blog post on kafka/elasticsearch/kibana](https://www.elastic.co/blog/building-real-time-dashboard-applications-with-apache-flink-elasticsearch-and-kibana)
  * point to the training site

{: style="color: red"}
## GOALS

* make a good first impression on first-time visitors
* reduce duplication: ideally every piece of information would have one natural place to be
* improve navigation and discoverability: important topics shouldn't be hard to find
* update/remove out-of-date material
* arrange all of the content in a natural, linear ordering for those who want to read (or skim through) everything
* improve important sections that are difficult to understand
