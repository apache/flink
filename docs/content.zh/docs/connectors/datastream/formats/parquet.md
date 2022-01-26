---
title:  "Parquet"
weight: 4
type: docs
aliases:
- /dev/connectors/formats/parquet.html
- /apis/streaming/connectors/formats/parquet.html
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


# Parquet format

Flink 支持读取 [Parquet](https://parquet.apache.org/) 文件并生成 [Flink rows](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/types/Row.html)。
要使用 Parquet format，你需要将 Flink Parquet 依赖项添加到项目中：

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-parquet</artifactId>
	<version>{{< version >}}</version>
</dependency>
```
 
此 format 与新 Source 兼容，可以在批处理和流模式下使用。
因此，你可以通过两种方式使用此 format：
- 批处理模式的有界读取
- 流模式的连续读取：监视目录中出现的新文件

**有界读取示例**:
在这个例子中，我们创建了一个包含 Parquet 记录作为 Flink Rows 的 DataStream。我们将模式投影为仅读的取某些字段（“f7”、“f4” 和 “f99”）。
我们分批读取 500 条记录。第一个布尔参数指定是否需要将时间戳列处理为 UTC。
第二个布尔值指定应用程序是否要以区分大小写的方式处理投影的 Parquet 字段名称。
不需要水印策略，因为记录不包含事件时间戳。

```java
final LogicalType[] fieldTypes =
  new LogicalType[] {
  new DoubleType(), new IntType(), new VarCharType()
  };

final ParquetColumnarRowInputFormat<FileSourceSplit> format =
  new ParquetColumnarRowInputFormat<>(
  new Configuration(),
  RowType.of(fieldTypes, new String[] {"f7", "f4", "f99"}),
  500,
  false,
  true);
final FileSource<RowData> source =
  FileSource.forBulkFileFormat(format,  /* Flink Path */)
  .build();
final DataStream<RowData> stream =
  env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```

**连续读取示例**:

在这个例子中，我们创建了一个包含 Parquet 记录的 DataStream，作为 Flink Rows，随着新文件添加到目录中，它将无限增长。每秒进行新文件监控。
我们将模式投影为仅读取某些字段（“f7”、“f4” 和 “f99”）。
分批读取 500 条记录。第一个布尔参数指定是否需要将时间戳列处理为 UTC。
第二个布尔值指示应用程序是否要以区分大小写的方式处理投影的 Parquet 字段名称。
不需要水印策略，因为记录不包含事件时间戳。

```java
final LogicalType[] fieldTypes =
  new LogicalType[] {
  new DoubleType(), new IntType(), new VarCharType()
  };

final ParquetColumnarRowInputFormat<FileSourceSplit> format =
  new ParquetColumnarRowInputFormat<>(
  new Configuration(),
  RowType.of(fieldTypes, new String[] {"f7", "f4", "f99"}),
  500,
  false,
  true);
final FileSource<RowData> source =
  FileSource.forBulkFileFormat(format,  /* Flink Path */)
  .monitorContinuously(Duration.ofSeconds(1L))
  .build();
final DataStream<RowData> stream =
  env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```
