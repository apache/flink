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
      <td>Specify what format to use, here should be <code>'csv'</code>.</td>
    </tr>
    <tr>
      <td><h5>csv.field-delimiter</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>,</code></td>
      <td>String</td>
      <td>Field delimiter character (<code>','</code> by default).</td>
    </tr>
    <tr>
      <td><h5>csv.line-delimiter</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>\n</code></td>
      <td>String</td>
      <td>Line delimiter, <code>\n</code> by default. Note the <code>\n</code> and <code>\r</code> are invisible special characters, you have to use unicode to specify them in plain SQL.
          <ul>
           <li>e.g. <code>'csv.line-delimiter' = U&'\\000D'</code> specifies the to use carriage return <code>\r</code> as line delimiter.</li>
           <li>e.g. <code>'csv.line-delimiter' = U&'\\000A'</code> specifies the to use line feed <code>\n</code> as line delimiter.</li>
          </ul>
      </td>
    </tr>
    <tr>
      <td><h5>csv.disable-quote-character</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Disabled quote character for enclosing field values (false by default).
      If true, option <code>'csv.quote-character'</code> must be set.</td>
    </tr>
    <tr>
      <td><h5>csv.quote-character</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>"</code></td>
      <td>String</td>
      <td>Quote character for enclosing field values (<code>"</code> by default).</td>
    </tr>
    <tr>
      <td><h5>csv.allow-comments</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Ignore comment lines that start with <code>'#'</code> (disabled by default).
      If enabled, make sure to also ignore parse errors to allow empty rows.</td>
    </tr>
    <tr>
      <td><h5>csv.ignore-parse-errors</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Skip fields and rows with parse errors instead of failing.
      Fields are set to null in case of errors.</td>
    </tr>
    <tr>
      <td><h5>csv.array-element-delimiter</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>;</code></td>
      <td>String</td>
      <td>Array element delimiter string for separating
      array and row element values (<code>';'</code> by default).</td>
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
      <td>Null literal string that is interpreted as a null value (disabled by default).</td>
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
        <th class="text-left">Flink SQL type</th>
        <th class="text-left">CSV type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td><code>string</code></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>string with encoding: base64</code></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>string with format: date</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>string with format: time</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>string with format: date-time</code></td>
    </tr>
    <tr>
      <td><code>INTERVAL</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>array</code></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>object</code></td>
    </tr>
    </tbody>
</table>





