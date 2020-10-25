---
title: "Elasticsearch SQL Connector"
nav-title: Elasticsearch
nav-parent_id: sql-connectors
nav-pos: 4
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

<span class="label label-primary">Sink: Batch</span>
<span class="label label-primary">Sink: Streaming Append & Upsert Mode</span>

* This will be replaced by the TOC
{:toc}

The Elasticsearch connector allows for writing into an index of the Elasticsearch engine. This document describes how to setup the Elasticsearch Connector to run SQL queries against Elasticsearch.

The connector can operate in upsert mode for exchanging UPDATE/DELETE messages with the external system using the primary key defined on the DDL.

If no primary key is defined on the DDL, the connector can only operate in append mode for exchanging INSERT only messages with external system.

Dependencies
------------

In order to setup the Elasticsearch connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

| Elasticsearch Version   | Maven dependency                                                   | SQL Client JAR         |
| :---------------------- | :----------------------------------------------------------------- | :----------------------|
| 6.x                     | `flink-connector-elasticsearch6{{site.scala_version_suffix}}`      | {% if site.is_stable %} [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch6{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-elasticsearch6{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for [stable releases]({{ site.stable_baseurl }}/zh/dev/table/connectors/elasticsearch.html) {% endif %}|
| 7.x and later versions  | `flink-connector-elasticsearch7{{site.scala_version_suffix}}`      | {% if site.is_stable %} [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-elasticsearch7{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for [stable releases]({{ site.stable_baseurl }}/zh/dev/table/connectors/elasticsearch.html) {% endif %}|

<br>
<span class="label label-danger">Attention</span> Elasticsearch connector works with JSON format which defines how to encode documents for the external system, therefore, it must be added as a [dependency]({% link dev/table/connectors/formats/index.zh.md %}).

How to create an Elasticsearch table
----------------

The example below shows how to create an Elasticsearch sink table:

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE myUserTable (
  user_id STRING,
  user_name STRING
  uv BIGINT,
  pv BIGINT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://localhost:9200',
  'index' = 'users'
);
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
      <td>Specify what connector to use, valid values are:
      <ul>
      <li><code>elasticsearch-6</code>: connect to Elasticsearch 6.x cluster</li>
      <li><code>elasticsearch-7</code>: connect to Elasticsearch 7.x and later versions cluster</li>
      </ul></td>
    </tr>
    <tr>
      <td><h5>hosts</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>One or more Elasticsearch hosts to connect to, e.g. <code>'http://host_name:9092;http://host_name:9093'</code>.</td>
    </tr>
    <tr>
      <td><h5>index</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Elasticsearch index for every record. Can be a static index (e.g. <code>'myIndex'</code>) or
       a dynamic index (e.g. <code>'index-{log_ts|yyyy-MM-dd}'</code>).
       See the following <a href="#dynamic-index">Dynamic Index</a> section for more details.</td>
    </tr>
    <tr>
      <td><h5>document-type</h5></td>
      <td>required in 6.x</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Elasticsearch document type. Not necessary anymore in <code>elasticsearch-7</code>.</td>
    </tr>
    <tr>
      <td><h5>document-id.key-delimiter</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">_</td>
      <td>String</td>
      <td>Delimiter for composite keys ("_" by default), e.g., "$" would result in IDs "KEY1$KEY2$KEY3"."</td>
    </tr>
    <tr>
      <td><h5>username</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Username used to connect to Elasticsearch instance. Please notice that Elasticsearch doesn't pre-bundled security feature, but you can enable it by following the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/configuring-security.html">guideline</a> to secure an Elasticsearch cluster.</td>
    </tr>
    <tr>
      <td><h5>password</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password used to connect to Elasticsearch instance. If <code>username</code> is configured, this option must be configured with non-empty string as well.</td>
    </tr>
    <tr>
      <td><h5>failure-handler</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">fail</td>
      <td>String</td>
      <td>Failure handling strategy in case a request to Elasticsearch fails. Valid strategies are:
      <ul>
        <li><code>fail</code>: throws an exception if a request fails and thus causes a job failure.</li>
        <li><code>ignore</code>: ignores failures and drops the request.</li>
        <li><code>retry_rejected</code>: re-adds requests that have failed due to queue capacity saturation.</li>
        <li>custom class name: for failure handling with a ActionRequestFailureHandler subclass.</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td><h5>sink.flush-on-checkpoint</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Flush on checkpoint or not. When disabled, a sink will not wait for all pending action requests
       to be acknowledged by Elasticsearch on checkpoints. Thus, a sink does NOT provide any strong
       guarantees for at-least-once delivery of action requests.
      </td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.max-actions</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>Maximum number of buffered actions per bulk request.
      Can be set to <code>'0'</code> to disable it.
      </td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.max-size</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2mb</td>
      <td>MemorySize</td>
      <td>Maximum size in memory of buffered actions per bulk request. Must be in MB granularity.
      Can be set to <code>'0'</code> to disable it.
      </td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.interval</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1s</td>
      <td>Duration</td>
      <td>The interval to flush buffered actions.
        Can be set to <code>'0'</code> to disable it. Note, both <code>'sink.bulk-flush.max-size'</code> and <code>'sink.bulk-flush.max-actions'</code>
        can be set to <code>'0'</code> with the flush interval set allowing for complete async processing of buffered actions.
      </td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.backoff.strategy</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">DISABLED</td>
      <td>String</td>
      <td>Specify how to perform retries if any flush actions failed due to a temporary request error. Valid strategies are:
      <ul>
        <li><code>DISABLED</code>: no retry performed, i.e. fail after the first request error.</li>
        <li><code>CONSTANT</code>: wait for backoff delay between retries.</li>
        <li><code>EXPONENTIAL</code>: initially wait for backoff delay and increase exponentially between retries.</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.backoff.max-retries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">8</td>
      <td>Integer</td>
      <td>Maximum number of backoff retries.</td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.backoff.delay</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50ms</td>
      <td>Duration</td>
      <td>Delay between each backoff attempt. For <code>CONSTANT</code> backoff, this is simply the delay between each retry. For <code>EXPONENTIAL</code> backoff, this is the initial base delay.</td>
    </tr>
    <tr>
      <td><h5>connection.max-retry-timeout</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>Maximum timeout between retries.</td>
    </tr>
    <tr>
      <td><h5>connection.path-prefix</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Prefix string to be added to every REST communication, e.g., <code>'/v1'</code></td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">json</td>
      <td>String</td>
      <td>Elasticsearch connector supports to specify a format. The format must produce a valid json document.
       By default uses built-in <code>'json'</code> format. Please refer to <a href="{% link dev/table/connectors/formats/index.zh.md %}">JSON Format</a> page for more details.
      </td>
    </tr>
    </tbody>
</table>

Features
----------------

### Key Handling

Elasticsearch sink can work in either upsert mode or append mode, it depends on whether primary key is defined.
If primary key is defined, Elasticsearch sink works in upsert mode which can consume queries containing UPDATE/DELETE messages.
If primary key is not defined, Elasticsearch sink works in append mode which can only consume queries containing INSERT only messages.

In Elasticsearch connector, the primary key is used to calculate the Elasticsearch document id, which is a string of up to 512 bytes. It cannot have whitespaces.
The Elasticsearch connector generates a document ID string for every row by concatenating all primary key fields in the order defined in the DDL using a key delimiter specified by `document-id.key-delimiter`.
Certain types are not allowed as primary key field as they do not have a good string representation, e.g. `BYTES`, `ROW`, `ARRAY`, `MAP`, etc.
If no primary key is specified, Elasticsearch will generate a document id automatically.

See [CREATE TABLE DDL]({% link dev/table/sql/create.zh.md %}#create-table) for more details about PRIMARY KEY syntax.

### Dynamic Index

Elasticsearch sink supports both static index and dynamic index.

If you want to have a static index, the `index` option value should be a plain string, e.g. `'myusers'`, all the records will be consistently written into "myusers" index.

If you want to have a dynamic index, you can use `{field_name}` to reference a field value in the record to dynamically generate a target index.
You can also use `'{field_name|date_format_string}'` to convert a field value of `TIMESTAMP/DATE/TIME` type into the format specified by the `date_format_string`.
The `date_format_string` is compatible with Java's [DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/index.html).
For example, if the option value is `'myusers-{log_ts|yyyy-MM-dd}'`, then a record with `log_ts` field value `2020-03-27 12:25:55` will be written into "myusers-2020-03-27" index.


Data Type Mapping
----------------

Elasticsearch stores document in a JSON string. So the data type mapping is between Flink data type and JSON data type.
Flink uses built-in `'json'` format for Elasticsearch connector. Please refer to [JSON Format]({% link dev/table/connectors/formats/json.zh.md %}) page for more type mapping details.

{% top %}
