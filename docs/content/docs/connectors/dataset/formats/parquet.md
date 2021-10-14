---
title:  "Parquet"
weight: 4
type: docs
aliases:
  - /dev/batch/connectors/formats/parquet.html
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

Flink has extensive built-in support for [Apache Parquet](http://parquet.apache.org/). This allows to easily read from Parquet files with Flink. 
Be sure to include the Flink Parquet dependency to the pom.xml of your project.

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-parquet_{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
</dependency>
```

In order to read data from a Parquet file, you have to specify one of the implementation of `ParquetInputFormat`. There are several depending on your needs:
- `ParquetPojoInputFormat<E>` to read POJOs from parquet files
- `ParquetRowInputFormat` to read Flink `Rows` (column oriented records) from parquet files
- `ParquetMapInputFormat` to read Map records (Map of nested Flink type objects) from parquet files
- `ParquetAvroInputFormat` to read Avro Generic Records from parquet files


**Example for ParquetRowInputFormat**:

```java
MessageType parquetSchema = // use parquet libs to provide the parquet schema file and parse it or extract it from the parquet files
ParquetRowInputFormat parquetInputFormat = new ParquetRowInputFormat(new Path(filePath),  parquetSchema);
// project only needed fields if suited to reduce the amount of data. Use: parquetSchema#selectFields(projectedFieldNames);
DataSet<Row> input = env.createInput(parquetInputFormat);
```

**Example for ParquetAvroInputFormat**:

```java
MessageType parquetSchema = // use parquet libs to provide the parquet schema file and parse it or extract it from the parquet files
ParquetAvroInputFormat parquetInputFormat = new ParquetAvroInputFormat(new Path(filePath),  parquetSchema);
// project only needed fields if suited to reduce the amount of data. Use: parquetSchema#selectFields(projectedFieldNames);
DataSet<GenericRecord> input = env.createInput(parquetInputFormat);
```


