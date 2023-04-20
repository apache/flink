---
title:  "Text files"
weight: 4
type: docs
aliases:
- /zh//connectors/formats/text_files.html
- /zh/apis/streaming/connectors/formats/text_files.html
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


# Text files format

Flink 支持使用 `TextLineInputFormat` 从文件中读取文本行。此 format 使用 Java 的内置 InputStreamReader 以支持的字符集编码来解码字节流。
要使用该 format，你需要将 Flink Connector Files 依赖项添加到项目中：

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-files</artifactId>
	<version>{{< version >}}</version>
</dependency>
```

PyFlink 用户可直接使用相关接口，无需添加依赖。

此 format 与新 Source 兼容，可以在批处理和流模式下使用。
因此，你可以通过两种方式使用此 format：
- 批处理模式的有界读取
- 流模式的连续读取：监视目录中出现的新文件

**有界读取示例**:

在此示例中，我们创建了一个 DataStream，其中包含作为字符串的文本文件的行。
此处不需要水印策略，因为记录不包含事件时间戳。

{{< tabs "bounded" >}}
{{< tab "Java" >}}
```java
final FileSource<String> source =
  FileSource.forRecordStreamFormat(new TextLineInputFormat(), /* Flink Path */)
  .build();
final DataStream<String> stream =
  env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
source = FileSource.for_record_stream_format(StreamFormat.text_line_format(), *path).build()
stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "file-source")
```
{{< /tab >}}
{{< /tabs >}}

**连续读取示例**:
在此示例中，我们创建了一个 DataStream，随着新文件被添加到目录中，其中包含的文本文件行的字符串流将无限增长。我们每秒会进行新文件监控。
此处不需要水印策略，因为记录不包含事件时间戳。

{{< tabs "continous" >}}
{{< tab "Java" >}}
```java
final FileSource<String> source =
    FileSource.forRecordStreamFormat(new TextLineInputFormat(), /* Flink Path */)
  .monitorContinuously(Duration.ofSeconds(1L))
  .build();
final DataStream<String> stream =
  env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
source = FileSource \
    .for_record_stream_format(StreamFormat.text_line_format(), *path) \
    .monitor_continously(Duration.of_seconds(1)) \
    .build()
stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "file-source")
```
{{< /tab >}}
{{< /tabs >}}
