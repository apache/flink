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

Flink supports reading [Parquet](https://parquet.apache.org/) files and producing [Flink rows](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/types/Row.html).
To use the format you need to add the Flink Parquet dependency to your project:

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-parquet</artifactId>
	<version>{{< version >}}</version>
</dependency>
```
 
This format is compatible with the new Source that can be used in both batch and streaming modes.
Thus, you can use this format in two ways:
- Bounded read for batch mode
- Continuous read for streaming mode: monitors a directory for new files that appear 

**Bounded read example**:

In this example we create a DataStream containing Parquet records as Flink Rows. We project the schema to read only certain fields ("f7", "f4" and "f99").  
We read records in batches of 500 records. The first boolean parameter specifies if timestamp columns need to be interpreted as UTC. 
The second boolean instructs the application if the projected Parquet fields names are to be interpreted in a case sensitive way.
There is no need for a watermark strategy as records do not contain event timestamps.

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

**Continuous read example**:

In this example we create a DataStream containing Parquet records as Flink Rows that will 
infinitely grow as new files are added to the directory. We monitor for new files each second.
We project the schema to read only certain fields ("f7", "f4" and "f99").  
We read records in batches of 500 records. The first boolean parameter specifies if timestamp columns need to be interpreted as UTC.
The second boolean instructs the application if the projected Parquet fields names are to be interpreted in a case sensitive way.
There is no need for a watermark strategy as records do not contain event timestamps.

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
