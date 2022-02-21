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

Flink supports reading from text lines from a file using `TextLineInputFormat`. This format uses Java's built-in InputStreamReader to decode the byte stream using various supported charset encodings.
To use the format you need to add the Flink Connector Files dependency to your project:

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-files</artifactId>
	<version>{{< version >}}</version>
</dependency>
```

This format is compatible with the new Source that can be used in both batch and streaming modes.
Thus, you can use this format in two ways:
- Bounded read for batch mode
- Continuous read for streaming mode: monitors a directory for new files that appear

**Bounded read example**:

In this example we create a DataStream containing the lines of a text file as Strings. 
There is no need for a watermark strategy as records do not contain event timestamps.

```java
final FileSource<String> source =
  FileSource.forRecordStreamFormat(new TextLineInputFormat(), /* Flink Path */)
  .build();
final DataStream<String> stream =
  env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```

**Continuous read example**:
In this example, we create a DataStream containing the lines of text files as Strings that will infinitely grow 
as new files are added to the directory. We monitor for new files each second.
There is no need for a watermark strategy as records do not contain event timestamps.

```java
final FileSource<String> source =
    FileSource.forRecordStreamFormat(new TextLineInputFormat(), /* Flink Path */)
  .monitorContinuously(Duration.ofSeconds(1L))
  .build();
final DataStream<String> stream =
  env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```
