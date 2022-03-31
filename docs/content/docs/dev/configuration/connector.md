---
title: "Connectors and Formats"
weight: 5
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

# Connectors and Formats

Flink applications can read from and write to various external systems via connectors.
It supports multiple formats in order to encode and decode data to match Flink's data structures.

An overview of available connectors and formats is available for both
[DataStream]({{< ref "docs/connectors/datastream/overview.md" >}}) and
[Table API/SQL]({{< ref "docs/connectors/table/overview.md" >}}).

## Available artifacts

In order to use connectors and formats, you need to make sure Flink has access to the artifacts implementing them. 
For each connector supported by the Flink community, we publish two artifacts on [Maven Central](https://search.maven.org):

* `flink-connector-<NAME>` which is a thin JAR including only the connector code, but excluding eventual third-party dependencies
* `flink-sql-connector-<NAME>` which is an uber JAR ready to use with all the connector third-party dependencies

The same applies for formats as well. Note that some connectors may not have a corresponding 
`flink-sql-connector-<NAME>` artifact because they do not require third-party dependencies.

{{< hint info >}}
The uber/fat JARs are supported mostly for being used in conjunction with the [SQL client]({{< ref "docs/dev/table/sqlClient" >}}),
but you can also use them in any DataStream/Table application.
{{< /hint >}}

## Using artifacts

In order to use a connector/format module, you can either:

* Shade the thin JAR and its transitive dependencies in your job JAR
* Shade the uber JAR in your job JAR
* Copy the uber JAR directly in the `/lib` folder of the Flink distribution

For shading dependencies, check out the specific [Maven]({{< ref "docs/dev/configuration/maven" >}}) 
and [Gradle]({{< ref "docs/dev/configuration/gradle" >}}) guides. 
For a reference about the Flink distribution, check [Anatomy of the Flink distribution]({{< ref "docs/dev/configuration/overview" >}}#anatomy-of-the-flink-distribution).

{{< hint info >}}
Deciding whether to shade the uber JAR, the thin JAR or just include the dependency in the distribution is up to you and your use case.
If you shade a dependency, you will have more control over the dependency version in the job JAR.
In case of shading the thin JAR, you will have even more control over the transitive dependencies,
since you can change the versions without changing the connector version (binary compatibility permitting).
In case of embedding the connector uber JAR directly in the Flink distribution `/lib` folder,
you will be able to control in one place connector versions for all jobs.
{{< /hint >}}
