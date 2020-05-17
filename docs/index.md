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

* **Code Walkthroughs**: Follow step-by-step guides and implement a simple application or query in one of Flink's APIs. 
  * [Implement a DataStream application](./getting-started/walkthroughs/datastream_api.html)
  * [Write a Table API query](./getting-started/walkthroughs/table_api.html)

* **Docker Playgrounds**: Set up a sandboxed Flink environment in just a few minutes to explore and play with Flink.
  * [Run and manage Flink streaming applications](./getting-started/docker-playgrounds/flink-operations-playground.html)

* **Concepts**: Learn about Flink's concepts to better understand the documentation.
  * [Stateful Stream Processing](concepts/stateful-stream-processing.html)
  * [Timely Stream Processing](concepts/timely-stream-processing.html)
  * [Flink Architecture](concepts/flink-architecture.html)
  * [Glossary](concepts/glossary.html)

## API References

The API references list and explain all features of Flink's APIs.

* [DataStream API](dev/datastream_api.html)
* [DataSet API](dev/batch/index.html)
* [Table API &amp; SQL](dev/table/index.html)

## Deployment

Before putting your Flink job into production, read the [Production Readiness Checklist](ops/production_ready.html).

## Release Notes

Release notes cover important changes between Flink versions. Please carefully read these notes if you plan to upgrade your Flink setup to a later version. 

* [Release notes for Flink 1.10](release-notes/flink-1.10.html).
* [Release notes for Flink 1.9](release-notes/flink-1.9.html).
* [Release notes for Flink 1.8](release-notes/flink-1.8.html).
* [Release notes for Flink 1.7](release-notes/flink-1.7.html).
* [Release notes for Flink 1.6](release-notes/flink-1.6.html).
* [Release notes for Flink 1.5](release-notes/flink-1.5.html).

## External Resources

- **Flink Forward**: Talks from past conferences are available at the [Flink Forward](http://flink-forward.org/) website and on [YouTube](https://www.youtube.com/channel/UCY8_lgiZLZErZPF47a2hXMA). [Robust Stream Processing with Apache Flink](http://2016.flink-forward.org/kb_sessions/robust-stream-processing-with-apache-flink/) is a good place to start.

- **Training**: The [training materials](https://training.ververica.com/) from Ververica include slides, exercises, and sample solutions.

- **Blogs**: The [Apache Flink](https://flink.apache.org/blog/) and [Ververica](https://www.ververica.com/blog) blogs publish frequent, in-depth technical articles about Flink.
