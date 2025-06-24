---
title: BlackHole
weight: 15
type: docs
aliases:
  - /dev/table/connectors/blackhole.html
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

# BlackHole SQL Connector

{{< label "Sink: Bounded" >}}
{{< label "Sink: UnBounded" >}}

The BlackHole connector allows for swallowing all input records. It is designed for:

- high performance testing.
- UDF to output, not substantive sink.

Just like /dev/null device on Unix-like operating systems.

The BlackHole connector is built-in.

How to create a BlackHole table
----------------

```sql
CREATE TABLE blackhole_table (
  f0 INT,
  f1 INT,
  f2 STRING,
  f3 DOUBLE
) WITH (
  'connector' = 'blackhole'
);
```

Alternatively, it may be based on an existing schema using the [LIKE Clause]({{< ref "docs/dev/table/sql/create" >}}#create-table).


```sql
CREATE TABLE blackhole_table WITH ('connector' = 'blackhole')
LIKE source_table (EXCLUDING ALL)
```

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
      <td>Specify what connector to use, here should be 'blackhole'.</td>
    </tr>
    </tbody>
</table>
