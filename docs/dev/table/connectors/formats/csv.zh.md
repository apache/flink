---
title: "CSV Format"
nav-title: CSV
nav-parent_id: sql-formats
nav-pos: 1
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

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

* This will be replaced by the TOC
{:toc}

The [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) format allows to read and write CSV data based on an CSV schema. Currently, the CSV schema is derived from table schema.

Dependencies
------------

In order to setup the CSV format, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

| Maven dependency   | SQL Client JAR         |
| :----------------- | :----------------------|
| `flink-csv`        | Built-in               |

How to create a table with CSV format
----------------

Here is an example to create a table using Kafka connector and CSV format.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE user_behavior (
  user_id BIGINT,
  item_id BIGINT,
  category_id BIGINT,
  behavior STRING,
  ts TIMESTAMP(3)
) WITH (
 'connector' = 'kafka',
 'topic' = 'user_behavior',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'csv',
 'csv.ignore-parse-errors' = 'true',
 'csv.allow-comments' = 'true'
)
{% endhighlight %}
</div>
</div>

Format Options
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
      <td><h5>format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what format to use, here should be 'csv'.</td>
    </tr>
    <tr>
      <td><h5>csv.field-delimiter</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>,</code></td>
      <td>String</td>
      <td>Field delimiter character (',' by default).</td>
    </tr>
    <tr>
      <td><h5>csv.line-delimiter</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>\n</code></td>
      <td>String</td>
      <td>Line delimiter ('\n' by default, otherwise
      '\r' or '\r\n' are allowed), unicode is supported if
      the delimiter is an invisible special character,
      e.g. U&'\\000D' is the unicode representation of carriage return '\r'
      e.g. U&'\\000A' is the unicode representation of line feed '\n'.</td>
    </tr>
    <tr>
      <td><h5>csv.disable-quote-character</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Flag to disabled quote character for enclosing field values (false by default)
      if true, quote-character can not be set.</td>
    </tr>
    <tr>
      <td><h5>csv.quote-character</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>"</code></td>
      <td>String</td>
      <td>Quote character for enclosing field values ('"' by default).</td>
    </tr>
    <tr>
      <td><h5>csv.allow-comments</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Flag to ignore comment lines that start with '#'
      (disabled by default);
      if enabled, make sure to also ignore parse errors to allow empty rows.</td>
    </tr>
    <tr>
      <td><h5>csv.ignore-parse-errors</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Flag to skip fields and rows with parse errors instead of failing;
      fields are set to null in case of errors.</td>
    </tr>
    <tr>
      <td><h5>csv.array-element-delimiter</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>;</code></td>
      <td>String</td>
      <td>Array element delimiter string for separating
      array and row element values (';' by default).</td>
    </tr>
    <tr>
      <td><h5>csv.escape-character</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Escape character for escaping values (disabled by default).</td>
    </tr>
    <tr>
      <td><h5>csv.null-literal</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Null literal string that is interpreted as a
      null value (disabled by default).</td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------

Currently, the CSV schema is always derived from table schema. Explicitly defining an CSV schema is not supported yet.

Flink CSV format uses [jackson databind API](https://github.com/FasterXML/jackson-databind) to parse and generate CSV string.

The following table lists the type mapping from Flink type to CSV type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink Data Type</th>
        <th class="text-center">CSV Data Type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR / VARCHAR / STRING</td>
      <td>string</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>boolean</td>
    </tr>
    <tr>
      <td>BINARY / VARBINARY</td>
      <td>string with encoding: base64</td>
    </tr>
    <tr>
      <td>DECIMAL</td>
      <td>number</td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>number</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>number</td>
    </tr>
    <tr>
      <td>INT</td>
      <td>number</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>number</td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>number</td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>number</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>string with format: date</td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>string with format: time</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>string with format: date-time</td>
    </tr>
    <tr>
      <td>INTERVAL</td>
      <td>number</td>
    </tr>
    <tr>
      <td>ARRAY</td>
      <td>array</td>
    </tr>
    <tr>
      <td>ROW</td>
      <td>object</td>
    </tr>
    </tbody>
</table>





