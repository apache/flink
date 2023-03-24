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


<a name="parquet-format"></a>

# Parquet format

Flink 支持读取 [Parquet](https://parquet.apache.org/) 文件并生成 {{< javadoc file="org/apache/flink/table/data/RowData.html" name="Flink RowData">}} 和 [Avro](https://avro.apache.org/) 记录。
要使用 Parquet format，你需要将 flink-parquet 依赖添加到项目中：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-parquet</artifactId>
    <version>{{< version >}}</version>
</dependency>
```

要使用 Avro 格式，你需要将 parquet-avro 依赖添加到项目中：

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

此格式与新的 Source 兼容，可以同时在批和流模式下使用。
因此，你可使用此格式处理以下两类数据：

- 有界数据: 列出所有文件并全部读取。
- 无界数据：监控目录中出现的新文件

{{< hint info >}}
当你开启一个 File Source，会被默认为有界读取。
如果你想在连续读取模式下使用 File Source，你必须额外调用
`AbstractFileSource.AbstractFileSourceBuilder.monitorContinuously(Duration)`。
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
下面的案例都是基于有界数据的。
如果你想在连续读取模式下使用 File Source，你必须额外调用
`AbstractFileSource.AbstractFileSourceBuilder.monitorContinuously(Duration)`。
{{< /hint >}}

<a name="flink-rowdata"></a>

## Flink RowData

在此示例中，你将创建由 Parquet 格式的记录构成的 Flink RowDatas DataStream。我们把 schema 信息映射为只读字段（"f7"、"f4" 和 "f99"）。
每个批次读取 500 条记录。其中，第一个布尔类型的参数用来指定是否需要将时间戳列处理为 UTC。
第二个布尔类型参数用来指定在进行 Parquet 字段映射时，是否要区分大小写。
这里不需要水印策略，因为记录中不包含事件时间戳。

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

Flink 支持三种方式来读取 Parquet 文件并创建 Avro records （PyFlink 只支持 Generic record）：

- [Generic record](https://avro.apache.org/docs/1.10.0/api/java/index.html)
- [Specific record](https://avro.apache.org/docs/1.10.0/api/java/index.html)
- [Reflect record](https://avro.apache.org/docs/1.10.0/api/java/org/apache/avro/reflect/package-summary.html)

### Generic record

使用 JSON 定义 Avro schemas。你可以从 [Avro specification](https://avro.apache.org/docs/1.10.0/spec.html) 获取更多关于 Avro schemas 和类型的信息。
此示例使用了一个在 [official Avro tutorial](https://avro.apache.org/docs/1.10.0/gettingstartedjava.html) 中描述的示例相似的 Avro schema：

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
这个 schema 定义了一个具有三个属性的的 user 记录：name，favoriteNumber 和 favoriteColor。你可以
在 [record specification](https://avro.apache.org/docs/1.10.0/spec.html#schema_record) 找到更多关于如何定义 Avro schema 的详细信息。

在此示例中，你将创建包含由 Avro Generic records 格式构成的 Parquet records 的 DataStream。
Flink 会基于 JSON 字符串解析 Avro schema。也有很多其他的方式解析 schema，例如基于 java.io.File 或 java.io.InputStream。
请参考 [Avro Schema](https://avro.apache.org/docs/1.10.0/api/java/org/apache/avro/Schema.html) 以获取更多详细信息。
然后，你可以通过 `AvroParquetReaders` 为 Avro Generic 记录创建 `AvroParquetRecordFormat`。

{{< tabs "GenericRecord" >}}
{{< tab "Java" >}}
```java
// 解析 avro schema
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
# 解析 avro schema
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

基于之前定义的 schema，你可以通过利用 Avro 代码生成来生成类。
一旦生成了类，就不需要在程序中直接使用 schema。
你可以使用 `avro-tools.jar` 手动生成代码，也可以直接使用 Avro Maven 插件对配置的源目录中的任何 .avsc 文件执行代码生成。
请参考 [Avro Getting Started](https://avro.apache.org/docs/1.10.0/gettingstartedjava.html) 获取更多信息。

此示例使用了样例 schema {{< gh_link file="flink-formats/flink-parquet/src/test/resources/avro/testdata.avsc" name="testdata.avsc" >}}：

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

你可以使用 Avro Maven plugin 生成 `Address` Java 类。

```java
@org.apache.avro.specific.AvroGenerated
public class Address extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    // 生成的代码...
}
```

你可以通过 `AvroParquetReaders` 为 Avro Specific 记录创建 `AvroParquetRecordFormat`，
然后创建一个包含由 Avro Specific records 格式构成的 Parquet records 的 DateStream。

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

除了需要预定义 Avro Generic 和 Specific 记录， Flink 还支持基于现有 Java POJO 类从 Parquet 文件创建 DateStream。
在这种场景中，Avro 会使用 Java 反射为这些 POJO 类生成 schema 和协议。
请参考 [Avro reflect](https://avro.apache.org/docs/1.10.0/api/java/index.html) 文档获取更多关于 Java 类型到 Avro schemas 映射的详细信息。

本例使用了一个简单的 Java POJO 类 {{< gh_link file="flink-formats/flink-parquet/src/test/java/org/apache/flink/formats/parquet/avro/Datum.java" name="Datum" >}}：

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

你可以通过 `AvroParquetReaders` 为 Avro Reflect 记录创建一个 `AvroParquetRecordFormat`，
然后创建一个包含由 Avro Reflect records 格式构成的 Parquet records 的 DateStream。

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

### 使用 Parquet files 必备条件

为了支持读取 Avro Reflect 数据，Parquet 文件必须包含特定的 meta 信息。为了生成 Parquet 数据，Avro schema 信息中必须包含 namespace，
以便让程序在反射执行过程中能确定唯一的 Java Class 对象。

下面的案例展示了上文中的 User 对象的 schema 信息。但是当前案例包含了一个指定文件目录的 namespace（当前案例下的包路径），反射过程中可以找到对应的 User 类。

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

由上述 scheme 信息创建的 Parquet 文件包含以下 meta 信息：

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

使用包 `org.apache.flink.formats.parquet.avro` 路径下已定义的 User 类：

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

你可以通过下面的程序读取类型为 User 的 Avro Reflect records：

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
