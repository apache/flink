---
title: Overview
weight: 1
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

# Integrations Maintained by Other Projects

The projects listed here maintain their own Flink integration. Each integration is developed,
released and supported by that project's community, not by the Apache Flink community. That
project's documentation covers which artifacts to use and how to configure it. Report issues and ask
questions in that project's channels.

A project is listed here when it is an Apache Software Foundation project and has released a Flink
integration for Flink 2.x.

{{< hint info >}}
These integrations follow their own release schedules and may not support this Flink version yet.
Check each project's supported Flink versions before you choose one.
{{< /hint >}}

| Project | Flink integration |
|---------|-------------------|
| [Apache Doris]({{< ref "docs/connectors/other-projects/doris" >}}) | Source, sink, lookup joins and database synchronization through Flink CDC |
| [Apache Fluss]({{< ref "docs/connectors/other-projects/fluss" >}}) | Catalog, source, sink, lookup joins and delta joins |
| [Apache Hudi]({{< ref "docs/connectors/other-projects/hudi" >}}) | Catalog, source and sink |
| [Apache Iceberg]({{< ref "docs/connectors/other-projects/iceberg" >}}) | Catalog, source and sink; REST catalogs such as Apache Polaris |
| [Apache Paimon]({{< ref "docs/connectors/other-projects/paimon" >}}) | Catalog, source, sink and lookup joins |

{{< top >}}
