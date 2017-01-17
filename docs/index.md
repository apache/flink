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

- **Concepts**: Start with the basic concepts of Flink's [Dataflow Programming Model](concepts/programming-model.html) and [Distributed Runtime Environment](concepts/runtime.html). This will help you to fully understand the other parts of the documentation, including the setup and programming guides. It is highly recommended to read these sections first.

- **Quickstarts**: [Run an example program](quickstart/setup_quickstart.html) on your local machine or [write a simple program](quickstart/run_example_quickstart.html) working on live Wikipedia edits.

- **Programming Guides**: You can check out our guides about [basic concepts](dev/api_concepts.html) and the [DataStream API](dev/datastream_api.html) or [DataSet API](dev/batch/index.html) to learn how to write your first Flink programs.

## Migration Guide

For users that have used prior versions of Apache Flink we recommend checking out the [API migration guide](dev/migration.html).
While all parts of the API that were marked as public and stable are still supported (backwards compatible), we suggest to migrate applications to the
newer interfaces where applicable.

For users that look to upgrade an operation Flink system to the latest version, we recommend to check out the guide on [upgrading Apache Flink](ops/upgrading.html)

