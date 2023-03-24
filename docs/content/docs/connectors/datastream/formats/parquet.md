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

Flink supports reading [Parquet](https://parquet.apache.org/) files, 
producing {{< javadoc file="org/apache/flink/table/data/RowData.html" name="Flink RowData">}} and producing [Avro](https://avro.apache.org/) records.
To use the format you need to add the `flink-parquet` dependency to your project:

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-parquet</artifactId>
	<version>{{< version >}}</version>
</dependency>
```

To read Avro records, you will need to add the `parquet-avro` dependency:

```xml
<dependency>
    <groupId>org.apache.parquet</groupId>
    <artifactId>parquet-avro</artifactId>
    <version>1.12.2</version>
    <optional>true</optional>
    <exclusions>
        <exclusion>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
        </exclusion>
        <exclusion>
            <groupId>it.unimi.dsi</groupId>
            <artifactId>fastutil</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

{{< py_download_link "parquet" >}}

This format is compatible with the new Source that can be used in both batch and streaming execution modes.
Thus, you can use this format for two kinds of data:

- Bounded data: lists all files and reads them all.
- Unbounded data: monitors a directory for new files that appear.

{{< hint info >}}
When you start a File Source it is configured for bounded data by default.
To configure the File Source for unbounded data, you must additionally call
`AbstractFileSource.AbstractFileSourceBuilder.monitorContinuously(Duration)`.
{{< /hint >}}

**Vectorized reader**

{{< tabs "0b1b298a-b92f-4f95-8d06-49544b48ab75" >}}
{{< tab "Java" >}}
```java

// Parquet rows are decoded in batches
FileSource.forBulkFileFormat(BulkFormat,Path...)

// Monitor the Paths to read data as unbounded data
FileSource.forBulkFileFormat(BulkFormat,Path...)
        .monitorContinuously(Duration.ofMillis(5L))
        .build();

```
{{< /tab >}}
{{< tab "Python" >}}
```python

# Parquet rows are decoded in batches
FileSource.for_bulk_file_format(BulkFormat, Path...)

# Monitor the Paths to read data as unbounded data
FileSource.for_bulk_file_format(BulkFormat, Path...) \
          .monitor_continuously(Duration.of_millis(5)) \
          .build()

```
{{< /tab >}}
{{< /tabs >}}

**Avro Parquet reader**

{{< tabs "0b1b298a-b92f-4f95-8d06-49544b12ab75" >}}
{{< tab "Java" >}}
```java

// Parquet rows are decoded in batches
FileSource.forRecordStreamFormat(StreamFormat,Path...)

// Monitor the Paths to read data as unbounded data
FileSource.forRecordStreamFormat(StreamFormat,Path...)
        .monitorContinuously(Duration.ofMillis(5L))
        .build();

```
{{< /tab >}}
{{< tab "Python" >}}
```python

# Parquet rows are decoded in batches
FileSource.for_record_stream_format(StreamFormat, Path...)

# Monitor the Paths to read data as unbounded data
FileSource.for_record_stream_format(StreamFormat, Path...) \
          .monitor_continuously(Duration.of_millis(5)) \
          .build()

```
{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
Following examples are all configured for bounded data.
To configure the File Source for unbounded data, you must additionally call
`AbstractFileSource.AbstractFileSourceBuilder.monitorContinuously(Duration)`.
{{< /hint >}}

## Flink RowData

In this example, you will create a DataStream containing Parquet records as Flink RowDatas. The schema is projected to read only the specified fields ("f7", "f4" and "f99").  
Flink will read records in batches of 500 records. The first boolean parameter specifies that timestamp columns will be interpreted as UTC.
The second boolean instructs the application that the projected Parquet fields names are case-sensitive.
There is no watermark strategy defined as records do not contain event timestamps.

{{< tabs "RowData" >}}
{{< tab "Java" >}}
```java
final LogicalType[] fieldTypes =
        new LogicalType[] {
                new DoubleType(), new IntType(), new VarCharType()};
final RowType rowType = RowType.of(fieldTypes, new String[] {"f7", "f4", "f99"});

final ParquetColumnarRowInputFormat<FileSourceSplit> format =
        new ParquetColumnarRowInputFormat<>(
                new Configuration(),
                rowType,
                InternalTypeInfo.of(rowType),
                500,
                false,
                true);
final FileSource<RowData> source =
        FileSource.forBulkFileFormat(format,  /* Flink Path */)
                .build();
final DataStream<RowData> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
row_type = DataTypes.ROW([
    DataTypes.FIELD('f7', DataTypes.DOUBLE()),
    DataTypes.FIELD('f4', DataTypes.INT()),
    DataTypes.FIELD('f99', DataTypes.VARCHAR()),
])
source = FileSource.for_bulk_file_format(ParquetColumnarRowInputFormat(
    row_type=row_type,
    hadoop_config=Configuration(),
    batch_size=500,
    is_utc_timestamp=False,
    is_case_sensitive=True,
), PARQUET_FILE_PATH).build()
ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "file-source")
```
{{< /tab >}}
{{< /tabs >}}

## Avro Records

Flink supports producing three types of Avro records by reading Parquet files (Only Generic record is supported in PyFlink):

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

In the following example, you will create a DataStream containing Parquet records as Avro Generic records. 
It will parse the Avro schema based on the JSON string. There are many other ways to parse a schema, e.g. from java.io.File or java.io.InputStream. Please refer to [Avro Schema](https://avro.apache.org/docs/1.10.0/api/java/org/apache/avro/Schema.html) for details.
After that, you will create an `AvroParquetRecordFormat` via `AvroParquetReaders` for Avro Generic records.

{{< tabs "GenericRecord" >}}
{{< tab "Java" >}}
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
{{< /tab >}}
{{< tab "Python" >}}
```python
# parsing avro schema
schema = AvroSchema.parse_string("""
{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favoriteNumber",  "type": ["int", "null"]},
        {"name": "favoriteColor", "type": ["string", "null"]}
    ]
}
""")

source = FileSource.for_record_stream_format(
    AvroParquetReaders.for_generic_record(schema), # file paths
).build()

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10)

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "file-source")
```
{{< /tab >}}
{{< /tabs >}}

### Specific record

Based on the previously defined schema, you can generate classes by leveraging Avro code generation. 
Once the classes have been generated, there is no need to use the schema directly in your programs. 
You can either use `avro-tools.jar` to generate code manually or you could use the Avro Maven plugin to perform 
code generation on any .avsc files present in the configured source directory. Please refer to 
[Avro Getting Started](https://avro.apache.org/docs/1.10.0/gettingstartedjava.html) for more information.

The following example uses the example schema {{< gh_link file="flink-formats/flink-parquet/src/test/resources/avro/testdata.avsc" name="testdata.avsc" >}}:

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

### Reflect record

Beyond Avro Generic and Specific record that requires a predefined Avro schema, 
Flink also supports creating a DataStream from Parquet files based on existing Java POJO classes.
In this case, Avro will use Java reflection to generate schemas and protocols for these POJO classes.
Java types are mapped to Avro schemas, please refer to the [Avro reflect](https://avro.apache.org/docs/1.10.0/api/java/index.html) documentation for more details.

This example uses a simple Java POJO class {{< gh_link file="flink-formats/flink-parquet/src/test/java/org/apache/flink/formats/parquet/avro/Datum.java" name="Datum" >}}:

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

#### Prerequisite for Parquet files

In order to support reading Avro reflect records, the Parquet file must contain specific meta information.
The Avro schema used for creating the Parquet data must contain a `namespace`, 
which will be used by the program to identify the concrete Java class for the reflection process.

The following example shows the `User` schema used previously. But this time it contains a namespace 
pointing to the location(in this case the package), where the `User` class for the reflection could be found.

```java
// avro schema with namespace
final String schema = 
                    "{\"type\": \"record\", "
                        + "\"name\": \"User\", "
                        + "\"namespace\": \"org.apache.flink.formats.parquet.avro\", "
                        + "\"fields\": [\n"
                        + "        {\"name\": \"name\", \"type\": \"string\" },\n"
                        + "        {\"name\": \"favoriteNumber\",  \"type\": [\"int\", \"null\"] },\n"
                        + "        {\"name\": \"favoriteColor\", \"type\": [\"string\", \"null\"] }\n"
                        + "    ]\n"
                        + "    }";

```

Parquet files created with this schema will contain meta information like:

```text
creator:        parquet-mr version 1.12.2 (build 77e30c8093386ec52c3cfa6c34b7ef3321322c94)
extra:          parquet.avro.schema =
{"type":"record","name":"User","namespace":"org.apache.flink.formats.parquet.avro","fields":[{"name":"name","type":"string"},{"name":"favoriteNumber","type":["int","null"]},{"name":"favoriteColor","type":["string","null"]}]}
extra:          writer.model.name = avro

file schema:    org.apache.flink.formats.parquet.avro.User
--------------------------------------------------------------------------------
name:           REQUIRED BINARY L:STRING R:0 D:0
favoriteNumber: OPTIONAL INT32 R:0 D:1
favoriteColor:  OPTIONAL BINARY L:STRING R:0 D:1

row group 1:    RC:3 TS:143 OFFSET:4
--------------------------------------------------------------------------------
name:            BINARY UNCOMPRESSED DO:0 FPO:4 SZ:47/47/1.00 VC:3 ENC:PLAIN,BIT_PACKED ST:[min: Jack, max: Tom, num_nulls: 0]
favoriteNumber:  INT32 UNCOMPRESSED DO:0 FPO:51 SZ:41/41/1.00 VC:3 ENC:RLE,PLAIN,BIT_PACKED ST:[min: 1, max: 3, num_nulls: 0]
favoriteColor:   BINARY UNCOMPRESSED DO:0 FPO:92 SZ:55/55/1.00 VC:3 ENC:RLE,PLAIN,BIT_PACKED ST:[min: green, max: yellow, num_nulls: 0]

```

With the `User` class defined in the package org.apache.flink.formats.parquet.avro:

```java
public class User {
        private String name;
        private Integer favoriteNumber;
        private String favoriteColor;

        public User() {}

        public User(String name, Integer favoriteNumber, String favoriteColor) {
            this.name = name;
            this.favoriteNumber = favoriteNumber;
            this.favoriteColor = favoriteColor;
        }

        public String getName() {
            return name;
        }

        public Integer getFavoriteNumber() {
            return favoriteNumber;
        }

        public String getFavoriteColor() {
            return favoriteColor;
        }
    }

```

you can write the following program to read Avro Reflect records of User type from parquet files:

```java
final FileSource<GenericRecord> source =
        FileSource.forRecordStreamFormat(
        AvroParquetReaders.forReflectRecord(User.class), /* Flink Path */)
        .build();

final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10L);

final DataStream<GenericRecord> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
```
