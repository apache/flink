---
title:  "Hadoop"
weight: 4
type: docs
aliases:
  - /dev/batch/connectors/hadoop.html
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

# Hadoop formats

Apache Flink allows users to access many different systems as data sources or sinks.
The system is designed for very easy extensibility. Similar to Apache Hadoop, Flink has the concept
of so called `InputFormat`s and `OutputFormat`s.

One implementation of these `InputFormat`s is the `HadoopInputFormat`. This is a wrapper that allows
users to use all existing Hadoop input formats with Flink.

This section shows some examples for connecting Flink to other systems.
[Read more about Hadoop compatibility in Flink]({{< ref "docs/dev/dataset/hadoop_compatibility" >}}).

{{< top >}}
