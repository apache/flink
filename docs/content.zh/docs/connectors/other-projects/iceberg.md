---
title: Apache Iceberg
weight: 10
type: docs
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

# Apache Iceberg

{{< hint info >}}
The Apache Iceberg community maintains this integration, not the Apache Flink community. It may not
support this Flink version yet, so check its supported Flink versions below.
{{< /hint >}}

[Apache Iceberg](https://iceberg.apache.org/) is an open table format for large analytic tables.
Its Flink integration provides a catalog and reads and writes Iceberg tables in streaming and batch
mode from SQL, the Table API and the DataStream API.

Iceberg tables are often managed by a catalog service that implements the Iceberg REST catalog API,
such as [Apache Polaris](https://polaris.apache.org/guides/flink/). Flink connects to such a service
through Iceberg's REST catalog.

- [Flink integration](https://iceberg.apache.org/docs/latest/flink/)
- [Supported Flink versions](https://iceberg.apache.org/multi-engine-support/#apache-flink)
- [REST catalog](https://iceberg.apache.org/docs/latest/flink/#rest-catalog)

{{< top >}}
