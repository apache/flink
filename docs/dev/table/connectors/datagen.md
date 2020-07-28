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

The DataGen connector allows for creating tables based on in-memory data generation.
This is useful when developing queries locally without access to external systems such as Kafka.
Tables can include [Computed Column syntax]({% link dev/table/sql/create.md %}#create-table) which allows for flexible record generation.

The DataGen connector is built-in, no additional dependencies are required.

Usage
-----

By default, a DataGen table will create an unbounded number of rows with a random value for each column.
For variable sized types, char/varchar/string/array/map/multiset, the length can be specified.
Additionally, a total number of rows can be specified, resulting in a bounded table.

There also exists a sequence generator, where users specify a sequence of start and end values.
Complex types cannot be generated as a sequence.
If any column in a table is a sequence type, the table will be bounded and end with the first sequence completes.

Time types are always the local machines current system time.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE datagen (
 f_sequence INT,
 f_random INT,
 f_random_str STRING,
 ts TIMESTAMP(3)
 WATERMARK FOR ts AS ts
) WITH (
 'connector' = 'datagen',

 -- optional options --

 'rows-per-second'='5',
 
 -- make the table bounded
 'number-of-rows'='10'

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
          <td><h5>number-of-rows</h5></td>
          <td>optional</td>
          <td style="word-wrap: break-word;">(none)</td>
          <td>Long</td>
          <td>The total number of rows to emit. By default, the table is unbounded.</td>
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
