---
title:  "Parquet"
weight: 4
type: docs
aliases:
- /zh/dev/connectors/formats/parquet.html
- /zh/apis/streaming/connectors/formats/parquet.html
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

Flink supports reading [Parquet](https://parquet.apache.org/) files,
producing {{< javadoc file="org/apache/flink/table/data/RowData.html" name="Flink RowData">}} and producing [Avro](https://avro.apache.org/) records.
To use the format you need to add the Flink Parquet dependency to your project:

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-parquet</artifactId>
	<version>{{< version >}}</version>
</dependency>
```

This format is compatible with the new Source that can be used in both batch and streaming modes.
Thus, you can use this format for two kinds of data:
- Bounded data
- Unbounded data: monitors a directory for new files that appear

## Flink RowData

#### Bounded data example

In this example, you will create a DataStream containing Parquet records as Flink RowDatas. The schema is projected to read only the specified fields ("f7", "f4" and "f99").  
Flink will read records in batches of 500 records. The first boolean parameter specifies that timestamp columns will be interpreted as UTC.
The second boolean instructs the application that the projected Parquet fields names are case-sensitive.
There is no watermark strategy defined as records do not contain event timestamps.

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

#### Unbounded data example

In this example, you will create a DataStream containing Parquet records as Flink RowDatas that will
infinitely grow as new files are added to the directory. It will monitor for new files each second.
The schema is projected to read only the specified fields ("f7", "f4" and "f99").  
Flink will read records in batches of 500 records. The first boolean parameter specifies that timestamp columns will be interpreted as UTC.
The second boolean instructs the application that the projected Parquet fields names are case-sensitive.
There is no watermark strategy defined as records do not contain event timestamps.

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

## Avro Records

Flink supports producing three types of Avro records by reading Parquet files:

- [Generic record](https://avro.apache.org/docs/1.10.0/api/java/index.html)
- [Specific record](https://avro.apache.org/docs/1.10.0/api/java/index.html)
- [Reflect record](https://avro.apache.org/docs/1.10.0/api/java/org/apache/avro/reflect/package-summary.html)

### Generic record

Avro schemas are defined using JSON. You can get more information about Avro schemas and types from the [Avro specification](https://avro.apache.org/docs/1.10.0/spec.html).
This example uses an Avro schema example similar to the one described in the [official Avro tutorial](https://avro.apache.org/docs/1.10.0/gettingstartedjava.html):

```json lines
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favoriteNumber",  "type": ["int", "null"]},
     {"name": "favoriteColor", "type": ["string", "null"]}
 ]
}
```

This schema defines a record representing a user with three fields: name, favoriteNumber, and favoriteColor. You can find more details at [record specification](https://avro.apache.org/docs/1.10.0/spec.html#schema_record) for how to define an Avro schema.

#### Bounded data example

In this example, you will create a DataStream containing Parquet records as Avro Generic records.
It will parse the Avro schema based on the JSON string. There are many other ways to parse a schema, e.g. from java.io.File or java.io.InputStream. Please refer to [Avro Schema](https://avro.apache.org/docs/1.10.0/api/java/org/apache/avro/Schema.html) for details.
After that, you will create an `AvroParquetRecordFormat` via `AvroParquetReaders` for Avro Generic records.

```java
// parsing avro schema
final Schema schema =
        new Schema.Parser()
            .parse(
                    "{\"type\": \"record\", "
                        + "\"name\": \"User\", "
                        + "\"fields\": [\n"
                        + "        {\"name\": \"name\", \"type\": \"string\" },\n"
                        + "        {\"name\": \"favoriteNumber\",  \"type\": [\"int\", \"null\"] },\n"
                        + "        {\"name\": \"favoriteColor\", \"type\": [\"string\", \"null\"] }\n"
                        + "    ]\n"
                        + "    }");

final FileSource<GenericRecord> source =
        FileSource.forRecordStreamFormat(
                AvroParquetReaders.forGenericRecord(schema), /* Flink Path */)
        .build();

final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10L);
        
final DataStream<GenericRecord> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```

#### Unbounded data example

This example is similar to the bounded batch example. The application monitors for new files every second
and reads Avro Generic records from Parquet files infinitely as new files are added to the directory.
```java
// parsing avro schema
final Schema schema =
        new Schema.Parser()
            .parse(
                    "{\"type\": \"record\", "
                        + "\"name\": \"User\", "
                        + "\"fields\": [\n"
                        + "        {\"name\": \"name\", \"type\": \"string\" },\n"
                        + "        {\"name\": \"favoriteNumber\",  \"type\": [\"int\", \"null\"] },\n"
                        + "        {\"name\": \"favoriteColor\", \"type\": [\"string\", \"null\"] }\n"
                        + "    ]\n"
                        + "    }");

final FileSource<GenericRecord> source =
        FileSource.forRecordStreamFormat(
                AvroParquetReaders.forGenericRecord(schema), /* Flink Path */)
        .monitorContinuously(Duration.ofSeconds(1L))
        .build();

final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10L);
        
final DataStream<GenericRecord> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```

### Specific record

Based on the previously defined schema, you can generate classes by leveraging Avro code generation.
Once the classes have been generated, there is no need to use the schema directly in your programs.
You can either use `avro-tools.jar` to generate code manually or you could use the Avro Maven plugin to perform
code generation on any .avsc files present in the configured source directory. Please refer to
[Avro Getting Started](https://avro.apache.org/docs/1.10.0/gettingstartedjava.html) for more information.

#### Bounded data example

This example uses the example schema [testdata.avsc](https://github.com/apache/flink/blob/master/flink-formats/flink-parquet/src/test/resources/avro/testdata.avsc):

```json lines
[
  {"namespace": "org.apache.flink.formats.parquet.generated",
    "type": "record",
    "name": "Address",
    "fields": [
      {"name": "num", "type": "int"},
      {"name": "street", "type": "string"},
      {"name": "city", "type": "string"},
      {"name": "state", "type": "string"},
      {"name": "zip", "type": "string"}
    ]
  }
]
```

You will use the Avro Maven plugin to generate the `Address` Java class:

```java
@org.apache.avro.specific.AvroGenerated
public class Address extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    // generated code...
}
```

You will create an `AvroParquetRecordFormat` via `AvroParquetReaders` for Avro Specific record
and then create a DataStream containing Parquet records as Avro Specific records.

```java
final FileSource<GenericRecord> source =
        FileSource.forRecordStreamFormat(
                AvroParquetReaders.forSpecificRecord(Address.class), /* Flink Path */)
        .build();

final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10L);
        
final DataStream<GenericRecord> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```

#### Unbounded data example

This example, similar to the bounded batch example, uses the same generated Address Java class
and monitors for the new files every second to read Avro Specific records from Parquet files
infinitely as new files are added to the directory.

```java
final FileSource<GenericRecord> source =
        FileSource.forRecordStreamFormat(
                AvroParquetReaders.forSpecificRecord(Address.class), /* Flink Path */)
        .monitorContinuously(Duration.ofSeconds(1L))
        .build();

final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10L);
        
final DataStream<GenericRecord> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```

### Reflect record

Beyond Avro Generic and Specific record that requires a predefined Avro schema,
Flink also supports creating a DataStream from Parquet files based on existing Java POJO classes.
In this case, Avro will use Java reflection to generate schemas and protocols for these POJO classes.
Java types are mapped to Avro schemas, please refer to the [Avro reflect](https://avro.apache.org/docs/1.10.0/api/java/index.html) documentation for more details.

#### Bounded data example

This example uses a simple Java POJO class [Datum](https://github.com/apache/flink/blob/master/flink-formats/flink-parquet/src/test/java/org/apache/flink/formats/parquet/avro/Datum.java):

```java
public class Datum implements Serializable {

    public String a;
    public int b;

    public Datum() {}

    public Datum(String a, int b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Datum datum = (Datum) o;
        return b == datum.b && (a != null ? a.equals(datum.a) : datum.a == null);
    }

    @Override
    public int hashCode() {
        int result = a != null ? a.hashCode() : 0;
        result = 31 * result + b;
        return result;
    }
}
```

You will create an `AvroParquetRecordFormat` via `AvroParquetReaders` for Avro Reflect record
and then create a DataStream containing Parquet records as Avro Reflect records.

```java
final FileSource<GenericRecord> source =
        FileSource.forRecordStreamFormat(
                AvroParquetReaders.forReflectRecord(Datum.class), /* Flink Path */)
        .build();

final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10L);
        
final DataStream<GenericRecord> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```

#### Unbounded data example

This example, similar to the bounded batch example, uses the same POJO Java class `Datum`
and monitors for the new files every second to read Avro Reflect records from Parquet files
infinitely as new files are added to the directory.

```java
final FileSource<GenericRecord> source =
        FileSource.forRecordStreamFormat(
                AvroParquetReaders.forReflectRecord(Datum.class), /* Flink Path */)
        .monitorContinuously(Duration.ofSeconds(1L))
        .build();

final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10L);
        
final DataStream<GenericRecord> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```
