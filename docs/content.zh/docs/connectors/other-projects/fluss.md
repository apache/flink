---
title: Apache Fluss
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

# Apache Fluss

{{< hint info >}}
The Apache Fluss community maintains this integration, not the Apache Flink community. It may not
support this Flink version yet, so check its supported Flink versions below.
{{< /hint >}}

[Apache Fluss](https://fluss.apache.org/) is streaming storage for real-time analytics. Its Flink
integration provides a catalog, reads and writes Fluss tables in streaming and batch mode, and
supports lookup joins. Fluss tables can be used as sources for
[delta joins]({{< ref "docs/dev/table/tuning" >}}#delta-joins).

- [Flink integration](https://fluss.apache.org/docs/engine-flink/getting-started/)
- [Supported Flink versions](https://fluss.apache.org/docs/engine-flink/getting-started/#supported-flink-versions)

{{< top >}}
