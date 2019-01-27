---
title: "SQL Sources & Sinks"
nav-parent_id: tableapi
nav-pos: 35
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

Flink SQL provides access to data which is stored in external systems (database, key-value store, message queue) or files.

By CREATE TABLE statement, data could be accessed as a SQL table in the following DML statements and translated to `TableSource` or `TableSink` automatically.

We use WITH clauses to describe the information necessary to access a external system.

* This will be replaced by the TOC
{:toc}

Provided Connectors
-------------------

### CSV connector
#### Support Matrix
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Stream Mode</th>
      <th class="text-left">Source</th>
      <th class="text-left">Sink</th>
      <th class="text-left">Temporal Join</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th class="text-left"><strong>Batch</strong></th>
      <th class="text-left">Y</th>
      <th class="text-left">Y</th>
      <th class="text-left">Y</th>
    </tr>
    <tr>
      <th class="text-left"><strong>Streaming</strong></th>
      <th class="text-left">Y</th>
      <th class="text-left">Y</th>
      <th class="text-left">Y</th>
    </tr>      
  </tbody>
</table>

{% highlight sql %}

-- Create a table named `Orders` which includes a primary key, and is stored as a CSV file
CREATE TABLE Orders (
    orderId BIGINT NOT NULL,
    customId VARCHAR NOT NULL,
    itemId BIGINT NOT NULL,
    totalPrice BIGINT NOT NULL,
    orderTime TIMESTAMP NOT NULL,
    description VARCHAR,
    PRIMARY KEY(orderId)
) WITH (
    type='csv',
    path='file:///abc/csv_file1'
)

{% endhighlight %}

#### Required configuration
* **type** : use `CSV` to create a Csv Table to read CSV files or to write into CSV files.
* **path** : locations of the CSV files.  Accepts standard Hadoop globbing expressions. To read a directory of CSV files, specify a directory.

#### Optional Configuration
* **enumerateNestedFiles** : when set to `true`, reader descends the directory for csv files. By default `true`.
* **fieldDelim** : the field delimiter. By default `,`, but can be set to any character.
* **lineDelim** : the line delimiter. By default `\n`, but can be set to any character.
* **charset** : defaults to `UTF-8`, but can be set to other valid charset names.
* **override** : when set to `true` the existing files are overwritten. By default `false`.
* **emptyColumnAsNull** : when set to `true`, any empty column will be set as null. By default `false`.
* **quoteCharacter** : by default no quote character, but can be set to any character.
* **firstLineAsHeader** : when set to `true`, the first line of files are used to name columns and are not included in data. All types are assumed to be string. By default `false`.
* **parallelism** : the number of files to write to.
* **timeZone** : timeZone to parse DateTime columns. Defaults to `UTC`, but can be set to other valid time zones.
* **commentsPrefix** : skip lines beginning with this character. By default no commentsPrefix, but can be set to any string.
* **updateMode** : the ways to encode a changes of a dynamic table. By default `append`. [See Table to Stream Conversion]({{ site.baseurl }}/dev/table/streaming/dynamic_tables.html#table-to-stream-conversion)
   * **append** : encoding INSERT changes.
   * **upsert** : encoding INSERT and UPDATE changes as upsert message and DELETE changes as delete message.
   * **retract** : encoding INSERT as add message and DELETE changes as retract message,  and an UPDATE change as a retract message for the updated (previous) row and an add message for the updating (new) row.

### HBase Connector
#### Support Matrix

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Stream Mode</th>
      <th class="text-left">Source</th>
      <th class="text-left">Sink</th>
      <th class="text-left">Temporal Join</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th class="text-left"><strong>Batch</strong></th>
      <th class="text-left">I</th>
      <th class="text-left">Y</th>
      <th class="text-left">Y</th>
    </tr>
    <tr>
      <th class="text-left"><strong>Streaming</strong></th>
      <th class="text-left">N</th>
      <th class="text-left">Y</th>
      <th class="text-left">Y</th>
    </tr>      
  </tbody>
</table>

**Legend**:
- Y: support
- N: not support
- I: incoming soon

{% highlight sql %}
CREATE TABLE testSinkTable (
     ROWKEY BIGINT,
     `family1.col1` VARCHAR,
     `family2.col1` INTEGER,
     `family2.col2` VARCHAR,
     `family3.col1` DOUBLE,
     `family3.col2` DATE,
     `family3.col3` BIGINT,
     PRIMARY KEY(ROWKEY)
) WITH (
    type='HBASE',
    connector.property-version='1.4.3',
    hbase.zookeeper.quorum='test_hostname:2181'
)

{% endhighlight %}

**Note** : the HBase table schema (that used for writing or temporal joining) must have a single column primary key which named `ROWKEY` and the column name format should be `columnFamily.qualifier`.

#### Required Configuration
* **type** : use `HBASE` to create an HBase table to read/write data.
* **connector.property-version** : specify the HBase client version, currently only '1.4.3' is available. More version(s) will'be supported later.
* **tableName** : specify the name of the table in HBase.
* **hbase.zookeeper.quorum** : specify the ZooKeeper quorum configuration for accessing the HBase cluster. **Note** : please specify this parameter or ensure a default `hbase-site.xml` is valid in the current classpath.

#### Optional Configuration
* **`hbase.*`** : support all the parameters that have the 'hbase.' prefix, e.g., 'hbase.client.operation.timeout'.

### Kafka Connector
#### Support Matrix

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Stream Mode</th>
      <th class="text-left">Source</th>
      <th class="text-left">Sink</th>
      <th class="text-left">Temporal Join</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th class="text-left"><strong>Batch</strong></th>
      <th class="text-left">I</th>
      <th class="text-left">Y</th>
      <th class="text-left">N</th>
    </tr>
    <tr>
      <th class="text-left"><strong>Streaming</strong></th>
      <th class="text-left">Y</th>
      <th class="text-left">Y</th>
      <th class="text-left">N</th>
    </tr>      
  </tbody>
</table>

**Legend**:
- Y: support
- N: not support
- I: incoming soon

#### Create Source Tables

{% highlight sql %}
CREATE TABLE kafka_source (
     key VARBINARY, 
     msg VARBINARY, 
     `topic` VARCHAR, 
     `partition` INT, 
     `offset` BIGINT
) WITH (
    type = 'KAFKA010',
    `bootstrap.servers` = 'test_hostname:9092',
    `group.id` = 'test-group-id',
    `topic` = 'source-topic',
    startupMode = 'EARLIEST'
)
{% endhighlight %}

**Note**: At this point, the Kafka source table must be created with the above five columns.

##### Kafka Source Table Configurations in WITH block
<table class="table table-borderd"> 
    <thead> 
        <tr> 
            <th>Configuration</th>
            <th>Applicable Connector Version</th>
            <th>Required</th>
            <th>Description</th>
            <th>Note</th> 
        </tr> 
    </thead> 
    <tbody>
        <tr>
            <td>type</td>
            <td>All</td>
            <td>Y</td>
            <td>The connector type, including Kafka version.</td> 
            <td>the valid values are <tt>KAFKA08</tt>, <tt>KAFKA09</tt>, <tt>KAFKA010</tt> or 
            <tt>KAFKA011</tt></td> 
        </tr>
        <tr>
            <td>topic</td>
            <td>All</td>
            <td>(Y)</td>
            <td>The single topic to read from Kafka</td>
            <td></td> 
        </tr> 
        <tr> 
            <td>topicPattern</td>
            <td>All</td>
            <td>(Y)</td>
            <td>The regular expression for topic names to read from Kafka. 
            </td> 
            <td>Should and should only be set if `topic` is not configured.</td>
        </tr>
        <tr>
            <td>zookeeper.connect</td>
            <td><tt>KAFKA08</tt></td>
            <td>Y</td>
            <td>The Zookeeper connect address.</td> 
            <td>The Zookeeper connect address. Only used by Kafka 0.8</td> 
        </tr>
        <tr>
            <td>bootstrap.servers</td>
            <td>All</td>
            <td>Y</td>
            <td>The Kafka cluster address.</td> 
            <td>The Kafka cluster address.</td>
        </tr>
        <tr> 
            <td>group.id</td>
            <td>All</td>
            <td>Y</td>
            <td>The consumer group id.</td> 
            <td>The consumer group id will be used to commit offsets back to Kafka for 
            reporting purpose.</td> 
        </tr>
        <tr>
            <td>startupMode</td>
            <td>All</td> 
            <td>N</td>
            <td>Specify the position to start reading from the Kafka topic</td> 
            <td> 
                <ul> 
                    <li><b>EARLIEST</b>: start reading from the first available message.</li> 
                    <li><b>Group_OFFSETS</b>: start reading from the last committed offset.</li> 
                    <li><b>LATEST(default)</b>: start reading from the latest offset</li> 
                    <li><b>TIMESTAMP</b>: start reading from the given timestamp (only supported 
                    in <tt>KAFKA010</tt> and <tt>KAFKA011</tt>)</li>
                </ul>
            </td> 
        </tr> 
        <tr> 
            <td>partitionDiscoveryIntervalMS</td>
            <td>All</td>
            <td>N</td>
            <td>Peoriodically check if there is new partititions added to the topic</td> 
            <td>The default value is 60 secons.</td> 
        </tr>
        <tr> 
            <td>extraConfig</td>
            <td>All</td>
            <td>N</td>
            <td>Additional configurations to use</td> 
            <td>The syntax is <tt>extraConfig='key1=value1;key2=value2;'</tt></td> 
        </tr>
    </tbody>
</table>

##### Additional Kafka Source Table Configurations
When defining a table from Kafka, in the with block, users can also set the configurations 
supported by Kafka consumer from the corresponding Kafka version. See the following links for 
all the configurations supported by Kafka consumers.

[KAFKA09](https://kafka.apache.org/090/documentation.html?spm=a2c4g.11186623.2.13.63994d25uXMjnS#newconsumerconfigs)

[KAFKA010](https://kafka.apache.org/0102/documentation.html?spm=a2c4g.11186623.2.14.63994d25jsbGRr#newconsumerconfigs)

[KAFKA011](https://kafka.apache.org/0110/documentation.html?spm=a2c4g.11186623.2.12.63994d25uXMjnS#consumerconfigs)

#### Create Sink Tables
{% highlight sql %}
CREATE TABLE kafka_sink (
    messageKey VARBINARY, 
    messageValue VARBINARY,
    PRIMARY KEY (messageKey)) 
with (
    type = 'KAFKA010', 
    topic = 'sink-topic', 
    `bootstrap.servers` = 'test_hostname:9092', 
    retries = '3'
)
{% endhighlight %}

**Note**: The primary key is mandatory for Kafka table as a sink table.

##### Kafka Sink Table Configurations in WITH block
<table class="table table-borderd"> 
    <thead> 
        <tr> 
            <th>Configuration</th>
            <th>Applicable Connector Version</th>
            <th>Required</th>
            <th>Description</th>
            <th>Note</th> 
        </tr> 
    </thead> 
    <tbody>
        <tr>
            <td>type</td>
            <td>All</td>
            <td>Y</td>
            <td>The connector type, including Kafka version.</td> 
            <td>the valid values are <tt>KAFKA08</tt>, <tt>KAFKA09</tt>, <tt>KAFKA010</tt> or 
            <tt>KAFKA011</tt></td> 
        </tr>
        <tr>
            <td>topic</td>
            <td>All</td>
            <td>Y</td>
            <td>The single Kafka topic to producer message</td>
            <td></td> 
        </tr> 
        <tr>
            <td>bootstrap.servers</td>
            <td>All</td>
            <td>Y</td>
            <td>The Kafka cluster address.</td> 
            <td>Used by Kafka 0.9 and above</td>
        </tr>
        <tr> 
            <td>extraConfig</td>
            <td>All</td>
            <td>N</td>
            <td>Additional configurations to use</td> 
            <td>The syntax is <tt>extraConfig='key1=value1;key2=value2;'</tt></td> 
        </tr>        
    </tbody>
</table>

##### Additional Sink Table Configurations
When defining a table from Kafka, in the with block, users can also set the configurations 
supported by Kafka producer from the corresponding Kafka version. See the following links for 
all the configurations supported by Kafka producers.

[KAFKA09](https://kafka.apache.org/090/documentation.html?spm=a2c4g.11186623.2.13.1f02740b30T3oC#producerconfigs)

[KAFKA010](https://kafka.apache.org/0102/documentation.html?spm=a2c4g.11186623.2.13.1f02740b30T3oC#producerconfigs)

[KAFKA011](https://kafka.apache.org/0110/documentation.html?spm=a2c4g.11186623.2.13.1f02740b30T3oC#producerconfigs)

### PARQUET Connector
#### Support Matrix

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Stream Mode</th>
      <th class="text-left">Source</th>
      <th class="text-left">Sink</th>
      <th class="text-left">Temporal Join</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th class="text-left"><strong>Batch</strong></th>
      <th class="text-left">Y</th>
      <th class="text-left">Y</th>
      <th class="text-left">I</th>
    </tr>
    <tr>
      <th class="text-left"><strong>Streaming</strong></th>
      <th class="text-left">N</th>
      <th class="text-left">N</th>
      <th class="text-left">N</th>
    </tr>      
  </tbody>
</table>

**Legend**:
- Y: support
- N: not support
- I: incoming soon

{% highlight sql %}
CREATE TABLE testSinkTable (
     `family1.col1` VARCHAR,
     `family2.col1` INTEGER,
     `family2.col2` VARCHAR
) WITH (
    type='PARQUET',
    filePath='schema://file1/file2.csv'
)

{% endhighlight %}

#### Required Configuration
+ **type** : use `PARQUET` declare this data source is a parquet format.
+ **filePath** : the path to write the data to or consume from.

#### Optional Configuration
+ **enumerateNestedFiles** : If to read all the data files from `filePath` recursively, default to be `true`. This only works for table source.
+ **writeMode** : If to override the file if there is already a file same name to the path to write to. Default to be `no_overwrite`, which means the file would not be overridden, so an error will thrown out if there exists same name files. This only works for table sink.
+ **compressionCodecName**: The compression codec of the parquet format, the options are `uncompressed`/`snappy`/`gzip`/`lzo` and default to be `snappy`. This only works for table sink.

### ORC Connector
#### Support Matrix

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Stream Mode</th>
      <th class="text-left">Source</th>
      <th class="text-left">Sink</th>
      <th class="text-left">Temporal Join</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th class="text-left"><strong>Batch</strong></th>
      <th class="text-left">Y</th>
      <th class="text-left">Y</th>
      <th class="text-left">I</th>
    </tr>
    <tr>
      <th class="text-left"><strong>Streaming</strong></th>
      <th class="text-left">N</th>
      <th class="text-left">N</th>
      <th class="text-left">N</th>
    </tr>      
  </tbody>
</table>

**Legend**:
- Y: support
- N: not support
- I: incoming soon

{% highlight sql %}
CREATE TABLE testSinkTable (
     `family1.col1` VARCHAR,
     `family2.col1` INTEGER,
     `family2.col2` VARCHAR,
     primary key(`family1.col1`)
) WITH (
    type='ORC',
    filePath='schema://file1/file2.csv'
)

{% endhighlight %}

#### Required Configuration
+ **type** : use `ORC` declare this data source is a ORC format.
+ **filePath** : the path to write the data to or consume from.

#### Optional Configuration
+ **enumerateNestedFiles** : If to read all the data files from `filePath` recursively, default to be `true`. This only works for table source.
+ **writeMode** : If to override the file if there is already a file same name to the path to write to. Default to be `no_overwrite`, which means the file would not be overridden, so an error will thrown out if there exists same name files. This only works for table sink.
+ **compressionCodecName**: The compression codec of the orc format, the options are `uncompressed`/`snappy`/`gzip`/`lzo` and default to be `snappy`. This only works for table sink.


