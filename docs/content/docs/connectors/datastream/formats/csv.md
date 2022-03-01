---
title:  "CSV"
weight: 4
type: docs
aliases:
- /dev/connectors/formats/csv.html
- /apis/streaming/connectors/formats/csv.html
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


# CSV format

To use the CSV format you need to add the Flink CSV dependency to your project:

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-csv</artifactId>
	<version>{{< version >}}</version>
</dependency>
```

Flink supports reading CSV files using `CsvReaderFormat`. The reader utilizes Jackson library and allows passing the corresponding configuration for the CSV schema and parsing options.

`CsvReaderFormat` can be initialized and used like this:
```java
CsvReaderFormat<SomePojo> csvFormat = CsvReaderFormat.forPojo(SomePojo.class);
FileSource<SomePojo> source = 
        FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(...)).build();
```

The schema for CSV parsing, in this case, is automatically derived based on the fields of the `SomePojo` class using the `Jackson` library. (Note: you might need to add `@JsonPropertyOrder({field1, field2, ...})` annotation to your class definition with the fields order exactly matching those of the CSV file columns).

If you need more fine-grained control over the CSV schema or the parsing options, use the more low-level `forSchema` static factory method of `CsvReaderFormat`:

```java
CsvReaderFormat<T> forSchema(CsvMapper mapper, 
                             CsvSchema schema, 
                             TypeInformation<T> typeInformation) 
```

Similarly to the `TextLineInputFormat`, `CsvReaderFormat` can be used in both continues and batch modes (see [TextLineInputFormat]({{< ref "docs/connectors/datastream/formats/text_files" >}})  for examples).
