---
title: Data Lineage
weight: 12
type: docs
aliases:
  - /internals/data_lineage.html
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

# Native Lineage Support
As organisations look to govern their data ecosystems; understanding data lineage, where data is coming from and going to, becomes critical. As Apache Flink is widely used for data ingestion and ETL in Streaming Data Lakes, we need 
an end to end lineage solution for scenarios including but not limited to:
  - `Data Quality Assurance`: Identifying and rectifying data inconsistencies by tracing data errors back to their origin within the data pipeline.
  - `Data Governance`： Establishing clear data ownership and accountability by documenting data origins and transformations.
  - `Regulatory Compliance`: Ensuring adherence to data privacy and compliance regulations by tracking data flow and transformations throughout its lifecycle.
  - `Data Optimization`: Identifying redundant data processing steps and optimizing data flows to improve efficiency.

Apache Flink provides a native lineage support by providing an internal lineage data model and [Job Status Listener]({{< ref "docs/deployment/advanced/job_status_listener" >}}) for
developer to integrate lineage metadata into external lineage system, for example [OpenLineage](https://openlineage.io). When a job is created in Flink runtime, the JobCreatedEvent 
contains the Lineage Graph metadata that will be sent to Job Status Listeners.

# Lineage Data Model
Flink native lineage interfaces are defined in two layers. The first layer is the generic interface for all Flink jobs and connector, and the second layer defines
the extended interfaces for Table and DataStream independently. The interface and class relationships are defined in the diagram below.

{{< img src="/fig/lineage_interfaces.png" alt="Lineage Data Model" width="80%">}}

By default, Table related lineage interfaces or classes are used in Flink Table environment, thus Flink users doesn't need to touch these interfaces. The Flink community will gradually support all
of the common connectors, such as Kafka, JDBC, Cassandra, Hive. If you have a customized connector defined, you need to have customized source/sink implementations of the LineageVertexProvider interface.
Within a LineageVertex, a list of Lineage Datasets are defined as metadata for Flink source/sink. 


```java
@PublicEvolving
public interface LineageVertexProvider {
  LineageVertex getLineageVertex();
}
```

For the interface details, please refer to [FLIP-314](https://cwiki.apache.org/confluence/display/FLINK/FLIP-314%3A+Support+Customized+Job+Lineage+Listener).

{{< top >}}
