---
title: "DataGen SQL Connector"
nav-title: DataGen
nav-parent_id: sql-connectors
nav-pos: 10
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
<span class="label label-primary">Scan Source: UnBounded</span>

* This will be replaced by the TOC
{:toc}

The Datagen connector allows for reading by data generation rules.

The Datagen connector can work with [Computed Column syntax]({% link dev/table/sql/create.zh.md %}#create-table).
This allows you to generate records flexibly.

The Datagen connector is built-in.

<span class="label label-danger">Attention</span> Complex types are not supported: Array, Map, Row. Please construct these types by computed column.

How to create a Datagen table
----------------

The boundedness of table: when the generation of field data in the table is completed, the reading
is finished. So the boundedness of the table depends on the boundedness of fields.

For each field, there are two ways to generate data:

- Random generator is the default generator, you can specify random max and min values. For char/varchar/string, the length can be specified. It is a unbounded generator.
- Sequence generator, you can specify sequence start and end values. It is a bounded generator, when the sequence number reaches the end value, the reading ends.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE datagen (
 f_sequence INT,
 f_random INT,
 f_random_str STRING,
 ts AS localtimestamp,
 WATERMARK FOR ts AS ts
) WITH (
 'connector' = 'datagen',

 -- optional options --

 'rows-per-second'='5',

 'fields.f_sequence.kind'='sequence',
 'fields.f_sequence.start'='1',
 'fields.f_sequence.end'='1000',

 'fields.f_random.min'='1',
 'fields.f_random.max'='1000',

 'fields.f_random_str.length'='10'
)
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
      <td>Specify what connector to use, here should be 'datagen'.</td>
    </tr>
    <tr>
      <td><h5>rows-per-second</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Long</td>
      <td>Rows per second to control the emit rate.</td>
    </tr>
    <tr>
      <td><h5>fields.#.kind</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">random</td>
      <td>String</td>
      <td>Generator of this '#' field. Can be 'sequence' or 'random'.</td>
    </tr>
    <tr>
      <td><h5>fields.#.min</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(Minimum value of type)</td>
      <td>(Type of field)</td>
      <td>Minimum value of random generator, work for number types.</td>
    </tr>
    <tr>
      <td><h5>fields.#.max</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(Maximum value of type)</td>
      <td>(Type of field)</td>
      <td>Maximum value of random generator, work for number types.</td>
    </tr>
    <tr>
      <td><h5>fields.#.length</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">100</td>
      <td>Integer</td>
      <td>Length for string generating of random generator, work for char/varchar/string.</td>
    </tr>
    <tr>
      <td><h5>fields.#.start</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>(Type of field)</td>
      <td>Start value of sequence generator.</td>
    </tr>
    <tr>
      <td><h5>fields.#.end</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>(Type of field)</td>
      <td>End value of sequence generator.</td>
    </tr>
    </tbody>
</table>
