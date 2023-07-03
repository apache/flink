---
title:  "Hadoop"
weight: 4
type: docs
aliases:
  - /dev/connectors/formats/hadoop.html
  - /apis/streaming/connectors/formats/hadoop.html

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

# Hadoop formats

## Project Configuration

Support for Hadoop is contained in the `flink-hadoop-compatibility`
Maven module.

Add the following dependency to your `pom.xml` to use hadoop

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-hadoop-compatibility{{< scala_version >}}</artifactId>
	<version>{{< version >}}</version>
</dependency>
```

If you want to run your Flink application locally (e.g. from your IDE), you also need to add
a `hadoop-client` dependency such as:

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.10.2</version>
    <scope>provided</scope>
</dependency>
```

## Using Hadoop InputFormats

To use Hadoop `InputFormats` with Flink the format must first be wrapped
using either `readHadoopFile` or `createHadoopInput` of the
`HadoopInputs` utility class.
The former is used for input formats derived
from `FileInputFormat` while the latter has to be used for general purpose
input formats.
The resulting `InputFormat` can be used to create a data source by using
`ExecutionEnvironment#createInput`.

The resulting `DataStream` contains 2-tuples where the first field
is the key and the second field is the value retrieved from the Hadoop
InputFormat.

The following example shows how to use Hadoop's `TextInputFormat`.

{{< tabs "baa59ec9-046e-4fe3-a2db-db5ee09d0635" >}}
{{< tab "Java" >}}

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
KeyValueTextInputFormat textInputFormat = new KeyValueTextInputFormat();

DataStream<Tuple2<Text, Text>> input = env.createInput(HadoopInputs.readHadoopFile(
  textInputFormat, Text.class, Text.class, textPath));

// Do something with the data.
[...]
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
val textInputFormat = new KeyValueTextInputFormat
val input: DataStream[(Text, Text)] =
  env.createInput(HadoopInputs.readHadoopFile(
    textInputFormat, classOf[Text], classOf[Text], textPath))

// Do something with the data.
[...]
```

{{< /tab >}}
{{< /tabs >}}

{{< top >}}
