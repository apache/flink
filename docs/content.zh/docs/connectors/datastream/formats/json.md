---
title:  "JSON"
weight: 4
type: docs
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

# Json format

To use the JSON format you need to add the Flink JSON dependency to your project:

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-json</artifactId>
	<version>{{< version >}}</version>
	<scope>provided</scope>
</dependency>
```

Flink supports reading/writing JSON records via the `JsonSerializationSchema/JsonDeserializationSchema`.
These utilize the [Jackson](https://github.com/FasterXML/jackson) library, and support any type that is supported by Jackson, including, but not limited to, `POJO`s and `ObjectNode`.

The `JsonDeserializationSchema` can be used with any connector that supports the `DeserializationSchema`.

For example, this is how you use it with a `KafkaSource` to deserialize a `POJO`:

```java
JsonDeserializationSchema<SomePojo> jsonFormat = new JsonDeserializationSchema<>(SomePojo.class);
KafkaSource<SomePojo> source =
    KafkaSource.<SomePojo>builder()
        .setValueOnlyDeserializer(jsonFormat)
        ...
```

The `JsonSerializationSchema` can be used with any connector that supports the `SerializationSchema`.

For example, this is how you use it with a `KafkaSink` to serialize a `POJO`:

```java
JsonSerializationSchema<SomePojo> jsonFormat = new JsonSerializationSchema<>();
KafkaSink<SomePojo> source =
    KafkaSink.<SomePojo>builder()
        .setRecordSerializer(
            new KafkaRecordSerializationSchemaBuilder<>()
                .setValueSerializationSchema(jsonFormat)
                ...
```

## Custom Mapper

Both schemas have constructors that accept a `SerializableSupplier<ObjectMapper>`, acting a factory for object mappers.
With this factory you gain full control over the created mapper, and can enable/disable various Jackson features or register modules to extend the set of supported types or add additional functionality.

```java
JsonSerializationSchema<SomeClass> jsonFormat = new JsonSerializationSchema<>(
    () -> new ObjectMapper()
        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS))
        .registerModule(new ParameterNamesModule());
```