---
title: NiFi
weight: 8
type: docs
aliases:
  - /zh/dev/connectors/nifi.html
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

# Apache NiFi 连接器

[Apache NiFi](https://nifi.apache.org/) 连接器提供了可以读取和写入的 Source 和 Sink。
使用这个连接器，需要在工程中添加下面的依赖:

{{< artifact flink-connector-nifi withScalaVersion >}}

注意这些连接器目前还没有包含在二进制发行版中。添加依赖、打包配置以及集群运行的相关信息请参考 [这里]({{< ref "docs/dev/datastream/project-configuration" >}})。

#### 安装 Apache NiFi

安装 Apache NiFi 集群请参考 [这里](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#how-to-install-and-start-nifi)。

#### Apache NiFi Source

该连接器提供了一个 Source 可以用来从 Apache NiFi 读取数据到 Apache Flink。

`NiFiSource(…)` 类有两个构造方法。

- `NiFiSource(SiteToSiteConfig config)` - 构造一个 `NiFiSource(…)` ，需要指定参数 SiteToSiteConfig ，采用默认的等待时间 1000 ms。

- `NiFiSource(SiteToSiteConfig config, long waitTimeMs)` - 构造一个 `NiFiSource(…)`，需要指定参数 SiteToSiteConfig 和等待时间（单位为毫秒）。

示例:

{{< tabs "44ccc35b-83c3-464f-9464-995d4981f4d9" >}}
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

数据从 Apache NiFi Output Port 读取，Apache NiFi Output Port 也被称为 "Data for Flink"，是 Apache NiFi Site-to-site 协议配置的一部分。

#### Apache NiFi Sink

该连接器提供了一个 Sink 可以用来把 Apache Flink 的数据写入到 Apache NiFi。

`NiFiSink(…)` 类只有一个构造方法。

- `NiFiSink(SiteToSiteClientConfig, NiFiDataPacketBuilder<T>)` 构造一个 `NiFiSink(…)`，需要指定 `SiteToSiteConfig` 和  `NiFiDataPacketBuilder` 参数 ，`NiFiDataPacketBuilder` 可以将Flink数据转化成可以被NiFi识别的 `NiFiDataPacket`.

示例:

{{< tabs "599dbd31-e2a4-4203-a428-0a4c95c8fd07" >}}
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

更多关于 [Apache NiFi](https://nifi.apache.org) Site-to-Site Protocol 的信息请参考 [这里](https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#site-to-site)。

{{< top >}}
