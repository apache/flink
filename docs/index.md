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



This documentation is for Apache Flink version {{ site.version_title }}. These pages were built at: {% build_time %}.

Apache Flink is an open source platform for distributed stream and batch data processing. Flink’s core is a streaming dataflow engine that provides data distribution, communication, and fault tolerance for distributed computations over data streams. Flink builds batch processing on top of the streaming engine, overlaying native iteration support, managed memory, and program optimization.

## First Steps

- **Concepts**: Start with the basic concepts of Flink's [Dataflow Programming Model](concepts/programming-model.html) and [Distributed Runtime Environment](concepts/runtime.html). This will help you understand other parts of the documentation, including the setup and programming guides. We recommend you read these sections first.

- **Tutorials**: 
  * [Implement and run a DataStream application](./getting-started/tutorials/datastream_api.html)
  * [Setup a local Flink cluster](./getting-started/tutorials/local_setup.html)

- **Programming Guides**: You can read our guides about [basic API concepts](dev/api_concepts.html) and the [DataStream API](dev/datastream_api.html) or the [DataSet API](dev/batch/index.html) to learn how to write your first Flink programs.

## Deployment

Before putting your Flink job into production, read the [Production Readiness Checklist](ops/production_ready.html).

## Release Notes

Release notes cover important changes between Flink versions. Please carefully read these notes if you plan to upgrade your Flink setup to a later version. 

* [Release notes for Flink 1.9](release-notes/flink-1.9.html).
* [Release notes for Flink 1.8](release-notes/flink-1.8.html).
* [Release notes for Flink 1.7](release-notes/flink-1.7.html).
* [Release notes for Flink 1.6](release-notes/flink-1.6.html).
* [Release notes for Flink 1.5](release-notes/flink-1.5.html).

## External Resources

- **Flink Forward**: Talks from past conferences are available at the [Flink Forward](http://flink-forward.org/) website and on [YouTube](https://www.youtube.com/channel/UCY8_lgiZLZErZPF47a2hXMA). [Robust Stream Processing with Apache Flink](http://2016.flink-forward.org/kb_sessions/robust-stream-processing-with-apache-flink/) is a good place to start.

- **Training**: The [training materials](https://training.ververica.com/) from Ververica include slides, exercises, and sample solutions.

- **Blogs**: The [Apache Flink](https://flink.apache.org/blog/) and [Ververica](https://www.ververica.com/blog) blogs publish frequent, in-depth technical articles about Flink.
