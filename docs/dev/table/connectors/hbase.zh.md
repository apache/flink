---
title: "HBase SQL Connector"
nav-title: HBase
nav-parent_id: sql-connectors
nav-pos: 6
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

<span class="label label-primary">Scan Source: Bounded</span>
<span class="label label-primary">Lookup Source: Sync Mode</span>
<span class="label label-primary">Sink: Batch</span>
<span class="label label-primary">Sink: Streaming Upsert Mode</span>

* This will be replaced by the TOC
{:toc}

The HBase connector allows for reading from and writing to an HBase cluster. This document describes how to setup the HBase Connector to run SQL queries against HBase.

HBase always works in upsert mode for exchange changelog messages with the external system using a primary key defined on the DDL. The primary key must be defined on the HBase rowkey field (rowkey field must be declared). If the PRIMARY KEY clause is not declared, the HBase connector will take rowkey as the primary key by default.

Dependencies
------------

In order to setup the HBase connector, the following table provide dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

| HBase Version       | Maven dependency                                          | SQL Client JAR         |
| :------------------ | :-------------------------------------------------------- | :----------------------|
| 1.4.x               | `flink-connector-hbase{{site.scala_version_suffix}}`      | {% if site.is_stable %} [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-hbase{{site.scala_version_suffix}}/{{site.version}}/flink-connector-hbase{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for [stable releases]({{ site.stable_baseurl }}/zh/dev/table/connectors/hbase.html) {% endif %}|


How to create an HBase table
----------------

All the column families in HBase table must be declared as ROW type, the field name maps to the column family name, and the nested field names map to the column qualifier names. There is no need to declare all the families and qualifiers in the schema, users can declare what’s used in the query. Except the ROW type fields, the single atomic type field (e.g. STRING, BIGINT) will be recognized as HBase rowkey. The rowkey field can be arbitrary name, but should be quoted using backticks if it is a reserved keyword.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
-- register the HBase table 'mytable' in Flink SQL
CREATE TABLE hTable (
 rowkey INT,
 family1 ROW<q1 INT>,
 family2 ROW<q2 STRING, q3 BIGINT>,
 family3 ROW<q4 DOUBLE, q5 BOOLEAN, q6 STRING>,
 PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
 'connector' = 'hbase-1.4',
 'table-name' = 'mytable',
 'zookeeper.quorum' = 'localhost:2181'
);

-- use ROW(...) construction function construct column families and write data into the HBase table.
-- assuming the schema of "T" is [rowkey, f1q1, f2q2, f2q3, f3q4, f3q5, f3q6]
INSERT INTO hTable
SELECT rowkey, ROW(f1q1), ROW(f2q2, f2q3), ROW(f3q4, f3q5, f3q6) FROM T;

-- scan data from the HBase table
SELECT rowkey, family1, family3.q4, family3.q6 FROM hTable;

-- temporal join the HBase table as a dimension table
SELECT * FROM myTopic
LEFT JOIN hTable FOR SYSTEM_TIME AS OF myTopic.proctime
ON myTopic.key = hTable.rowkey;
{% endhighlight %}
</div>
</div>

Connector Options
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, here should be <code>'hbase-1.4'</code>.</td>
    </tr>
    <tr>
      <td><h5>table-name</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of HBase table to connect.</td>
    </tr>
    <tr>
      <td><h5>zookeeper.quorum</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The HBase Zookeeper quorum.</td>
    </tr>
    <tr>
      <td><h5>zookeeper.znode.parent</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">/hbase</td>
      <td>String</td>
      <td>The root dir in Zookeeper for HBase cluster.</td>
    </tr>
    <tr>
      <td><h5>null-string-literal</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">null</td>
      <td>String</td>
      <td>Representation for null values for string fields. HBase source and sink encodes/decodes empty bytes as null values for all types except string type.</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.max-size</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2mb</td>
      <td>MemorySize</td>
      <td>Writing option, maximum size in memory of buffered rows for each writing request.
      This can improve performance for writing data to HBase database, but may increase the latency.
      Can be set to <code>'0'</code> to disable it.
      </td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.max-rows</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Writing option, maximum number of rows to buffer for each writing request.
      This can improve performance for writing data to HBase database, but may increase the latency.
      Can be set to <code>'0'</code> to disable it.
      </td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.interval</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1s</td>
      <td>Duration</td>
      <td>Writing option, the interval to flush any buffered rows.
      This can improve performance for writing data to HBase database, but may increase the latency.
      Can be set to <code>'0'</code> to disable it. Note, both <code>'sink.buffer-flush.max-size'</code> and <code>'sink.buffer-flush.max-rows'</code>
      can be set to <code>'0'</code> with the flush interval set allowing for complete async processing of buffered actions.
      </td>
    </tr>
    </tbody>
</table>



Data Type Mapping
----------------

HBase stores all data as byte arrays. The data needs to be serialized and deserialized during read and write operation

When serializing and de-serializing, Flink HBase connector uses utility class `org.apache.hadoop.hbase.util.Bytes` provided by HBase (Hadoop) to convert Flink Data Types to and from byte arrays.

Flink HBase connector encodes `null` values to empty bytes, and decode empty bytes to `null` values for all data types except string type. For string type, the null literal is determined by `null-string-literal` option.

The data type mappings are as follows:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL type</th>
        <th class="text-left">HBase conversion</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td>
{% highlight java %}
byte[] toBytes(String s)
String toString(byte[] b)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td>
{% highlight java %}
byte[] toBytes(boolean b)
boolean toBoolean(byte[] b)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td>Returns <code>byte[]</code> as is.</td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td>
{% highlight java %}
byte[] toBytes(BigDecimal v)
BigDecimal toBigDecimal(byte[] b)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td>
{% highlight java %}
new byte[] { val }
bytes[0] // returns first and only byte from bytes
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td>
{% highlight java %}
byte[] toBytes(short val)
short toShort(byte[] bytes)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td>
{% highlight java %}
byte[] toBytes(int val)
int toInt(byte[] bytes)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td>
{% highlight java %}
byte[] toBytes(long val)
long toLong(byte[] bytes)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td>
{% highlight java %}
byte[] toBytes(float val)
float toFloat(byte[] bytes)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td>
{% highlight java %}
byte[] toBytes(double val)
double toDouble(byte[] bytes)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td>Stores the number of days since epoch as int value.</td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td>Stores the number of milliseconds of the day as int value.</td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td>Stores the milliseconds since epoch as long value.</td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td>Not supported</td>
    </tr>
    <tr>
      <td><code>MAP / MULTISET</code></td>
      <td>Not supported</td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td>Not supported</td>
    </tr>
    </tbody>
</table>

{% top %}