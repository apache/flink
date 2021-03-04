---
title: NiFi
weight: 8
type: docs
aliases:
  - /dev/connectors/nifi.html
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

# Apache NiFi Connector

This connector provides a Source and Sink that can read from and write to
[Apache NiFi](https://nifi.apache.org/). To use this connector, add the
following dependency to your project:

{{< artifact flink-connector-nifi withScalaVersion >}}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here]({{< ref "docs/dev/datastream/project-configuration" >}})
for information about how to package the program with the libraries for
cluster execution.

#### Installing Apache NiFi

Instructions for setting up a Apache NiFi cluster can be found
[here](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#how-to-install-and-start-nifi).

#### Apache NiFi Source

The connector provides a Source for reading data from Apache NiFi to Apache Flink.

The class `NiFiSource(…)` provides 2 constructors for reading data from NiFi.

- `NiFiSource(SiteToSiteConfig config)` - Constructs a `NiFiSource(…)` given the client's SiteToSiteConfig and a
     default wait time of 1000 ms.

- `NiFiSource(SiteToSiteConfig config, long waitTimeMs)` - Constructs a `NiFiSource(…)` given the client's
     SiteToSiteConfig and the specified wait time (in milliseconds).

Example:

{{< tabs "14bce3dd-fcc4-4c98-bdd8-ed7819b7f0c4" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment();

SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
        .url("http://localhost:8080/nifi")
        .portName("Data for Flink")
        .requestBatchCount(5)
        .buildConfig();

SourceFunction<NiFiDataPacket> nifiSource = new NiFiSource(clientConfig);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment()

val clientConfig: SiteToSiteClientConfig = new SiteToSiteClient.Builder()
       .url("http://localhost:8080/nifi")
       .portName("Data for Flink")
       .requestBatchCount(5)
       .buildConfig()

val nifiSource = new NiFiSource(clientConfig)       
```       
{{< /tab >}}
{{< /tabs >}}

Here data is read from the Apache NiFi Output Port called "Data for Flink" which is part of Apache NiFi
Site-to-site protocol configuration.

#### Apache NiFi Sink

The connector provides a Sink for writing data from Apache Flink to Apache NiFi.

The class `NiFiSink(…)` provides a constructor for instantiating a `NiFiSink`.

- `NiFiSink(SiteToSiteClientConfig, NiFiDataPacketBuilder<T>)` constructs a `NiFiSink(…)` given the client's `SiteToSiteConfig` and a `NiFiDataPacketBuilder` that converts data from Flink to `NiFiDataPacket` to be ingested by NiFi.

Example:

{{< tabs "bcf46513-edfb-4b41-b588-51009eb9f59a" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment();

SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
        .url("http://localhost:8080/nifi")
        .portName("Data from Flink")
        .requestBatchCount(5)
        .buildConfig();

SinkFunction<NiFiDataPacket> nifiSink = new NiFiSink<>(clientConfig, new NiFiDataPacketBuilder<T>() {...});

streamExecEnv.addSink(nifiSink);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment()

val clientConfig: SiteToSiteClientConfig = new SiteToSiteClient.Builder()
       .url("http://localhost:8080/nifi")
       .portName("Data from Flink")
       .requestBatchCount(5)
       .buildConfig()

val nifiSink: NiFiSink[NiFiDataPacket] = new NiFiSink[NiFiDataPacket](clientConfig, new NiFiDataPacketBuilder<T>() {...})

streamExecEnv.addSink(nifiSink)
```       
{{< /tab >}}
{{< /tabs >}}      

More information about [Apache NiFi](https://nifi.apache.org) Site-to-Site Protocol can be found [here](https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#site-to-site)

{{< top >}}
