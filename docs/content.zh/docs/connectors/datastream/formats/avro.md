---
title:  "Avro"
weight: 4
type: docs
aliases:
- /dev/connectors/formats/avro.html
- /apis/streaming/connectors/formats/avro.html
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


# Avro 模式

Flink 已经支持 [Apache Avro](http://avro.apache.org/) 模式。在 Flink 中将更容易地基于 Avro 模式进行数据的读写。
Flink 的序列化框架能够处理从 Avro 模式生成的类。为了使用 Avro 模式需要在自动构建工具（例如 Maven 或 SBT ）中添加如下依赖到工程中。

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-avro</artifactId>
  <version>{{< version >}}</version>
</dependency>
```

为了从 Avro 文件中读取数据，你必须指定 `AvroInputFormat` 。

**示例**:

```java
AvroInputFormat<User> users = new AvroInputFormat<User>(in, User.class);
DataStream<User> usersDS = env.createInput(users);
```

注意，`User` 是一个由 Avro 生成的 POJO 类。 Flink 还允许对这些 POJO 执行基于字符串的键选择。例如:

```java
usersDS.keyBy("name")
```


注意， 在 Flink 中可以使用 `GenericData.Record` 数据类型, 但是不推荐使用。因为记录包含完整模式，所以它的数据非常密集，使用起来可能很慢。

Flink 的 POJO 字段选择也适用于 Avro 生成的POJO。因此，只有在字段类型正确写入生成的类中时，才可以使用。如果一个字段是 `Object` 类型，你不可以使用该字段进行 join 或者 group by 操作。
在 Avro 中例如 `{"name": "type_double_test", "type": "double"},` 这种格式的数据指定字段是可以的，但是像 (`{"name": "type_double_test", "type": ["double"]},`) 这种格式数据去指定字段，字段就会生成为 `Object` 类型。注意，像 (`{"name": "type_double_test", "type": ["null", "double"]},`) 这种带有 NULL 类型格式的数据也是可能的!
