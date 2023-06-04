---
title: DataGen
weight: 13
type: docs
aliases:
  - /dev/table/connectors/datagen.html
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

# DataGen SQL Connector

{{< label "Scan Source: Bounded" >}}
{{< label "Scan Source: UnBounded" >}}

The DataGen connector allows for creating tables based on in-memory data generation.
This is useful when developing queries locally without access to external systems such as Kafka.
Tables can include [Computed Column syntax]({{< ref "docs/dev/table/sql/create" >}}#create-table) which allows for flexible record generation.

The DataGen connector is built-in, no additional dependencies are required.

Usage
-----

By default, a DataGen table will create an unbounded number of rows with a random value for each column.
For variable sized types, char/varchar/binary/varbinary/string/array/map/multiset, the length can be specified.
Additionally, a total number of rows can be specified, resulting in a bounded table.

There also exists a sequence generator, where users specify a sequence of start and end values.
If any column in a table is a sequence type, the table will be bounded and end with the first sequence completes.

Time types are always the local machines current system time.

```sql
CREATE TABLE Orders (
    order_number BIGINT,
    price        DECIMAL(32,2),
    buyer        ROW<first_name STRING, last_name STRING>,
    order_time   TIMESTAMP(3)
) WITH (
  'connector' = 'datagen'
)
```

Often, the data generator connector is used in conjunction with the ``LIKE`` clause to mock out physical tables.

```sql
CREATE TABLE Orders (
    order_number BIGINT,
    price        DECIMAL(32,2),
    buyer        ROW<first_name STRING, last_name STRING>,
    order_time   TIMESTAMP(3)
) WITH (...)

-- create a bounded mock table
CREATE TEMPORARY TABLE GenOrders
WITH (
    'connector' = 'datagen',
    'number-of-rows' = '10'
)
LIKE Orders (EXCLUDING ALL)
```

Types
-----

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">Type</th>
            <th class="text-center" style="width: 25%">Supported Generators</th>
            <th class="text-center" style="width: 50%">Notes</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>BOOLEAN</td>
            <td>random</td>
            <td></td>
        </tr>
        <tr>
            <td>CHAR</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>VARCHAR</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>BINARY</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>VARBINARY</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>STRING</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>DECIMAL</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>TINYINT</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>SMALLINT</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>INT</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>BIGINT</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>FLOAT</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>DOUBLE</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>DATE</td>
            <td>random</td>
            <td>Always resolves to the current date of the local machine.</td>
        </tr>
        <tr>
            <td>TIME</td>
            <td>random</td>
            <td>Always resolves to the current time of the local machine.</td>
        </tr>
        <tr>
            <td>TIMESTAMP</td>
            <td>random</td>
            <td>
                Resolves a past timestamp relative to the current timestamp of the local machine.
                The max past can be specified by the 'max-past' option.
            </td>
        </tr>
        <tr>
            <td>TIMESTAMP_LTZ</td>
            <td>random</td>
            <td>
                Resolves a past timestamp relative to the current timestamp of the local machine.
                The max past can be specified by the 'max-past' option.
            </td>
        </tr>
        <tr>
            <td>INTERVAL YEAR TO MONTH</td>
            <td>random</td>
            <td></td>
        </tr>
        <tr>
            <td>INTERVAL DAY TO MONTH</td>
            <td>random</td>
            <td></td>
        </tr>
        <tr>
            <td>ROW</td>
            <td>random</td>
            <td>Generates a row with random subfields.</td>
        </tr>
        <tr>
            <td>ARRAY</td>
            <td>random</td>
            <td>Generates an array with random entries.</td>
        </tr>
        <tr>
            <td>MAP</td>
            <td>random</td>
            <td>Generates a map with random entries.</td>
        </tr>
        <tr>
            <td>MULTISET</td>
            <td>random</td>
            <td>Generates a multiset with random entries.</td>
        </tr>
    </tbody>
</table>

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
      <td>Minimum value of random generator, work for numeric types.</td>
    </tr>
    <tr>
      <td><h5>fields.#.max</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(Maximum value of type)</td>
      <td>(Type of field)</td>
      <td>Maximum value of random generator, work for numeric types.</td>
    </tr>
    <tr>
      <td><h5>fields.#.max-past</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Duration</td>
      <td>Maximum past of timestamp random generator, only works for timestamp types.</td>
    </tr>
    <tr>
      <td><h5>fields.#.length</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">100</td>
      <td>Integer</td>
      <td>Size or length of the collection for generating char/varchar/binary/varbinary/string/array/map/multiset types.</td>
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
    <tr>
      <td><h5>fields.#.null-rate</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>(Type of field)</td>
      <td>The proportion of null values.</td>
    </tr>
    </tbody>
</table>
