---
title: Print
weight: 14
type: docs
aliases:
  - /dev/table/connectors/print.html
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

# Print SQL Connector

{{< label "Sink" >}}

The Print connector allows for writing every row to the standard output or standard error stream.

It is designed for:

- Easy test for streaming job.
- Very useful in production debugging.

Four possible format options:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 40%">Print</th>
        <th class="text-center" style="width: 30%">Condition1</th>
        <th class="text-center" style="width: 30%">Condition2</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>PRINT_IDENTIFIER:taskId> output</h5></td>
      <td>PRINT_IDENTIFIER provided</td>
      <td>parallelism > 1</td>
    </tr>
    <tr>
      <td><h5>PRINT_IDENTIFIER> output</h5></td>
      <td>PRINT_IDENTIFIER provided</td>
      <td>parallelism == 1</td>
    </tr>
    <tr>
      <td><h5>taskId> output</h5></td>
      <td>no PRINT_IDENTIFIER provided</td>
      <td>parallelism > 1</td>
    </tr>
    <tr>
      <td><h5>output</h5></td>
      <td>no PRINT_IDENTIFIER provided</td>
      <td>parallelism == 1</td>
    </tr>
    </tbody>
</table>

The output string format is "$row_kind(f0,f1,f2...)", row_kind is the short string of `RowKind`, example is: "+I(1,1)".

The Print connector is built-in.

<span class="label label-danger">Attention</span> Print sinks print records in runtime tasks, you need to observe the task log.

How to create a Print table
----------------

```sql
CREATE TABLE print_table (
  f0 INT,
  f1 INT,
  f2 STRING,
  f3 DOUBLE
) WITH (
  'connector' = 'print'
);
```

Alternatively, it may be based on  an existing schema using the [LIKE Clause]({{< ref "docs/dev/table/sql/create" >}}#create-table).


```sql
CREATE TABLE print_table WITH ('connector' = 'print')
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
      <td>Specify what connector to use, here should be 'print'.</td>
    </tr>
    <tr>
      <td><h5>print-identifier</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Message that identify print and is prefixed to the output of the value.</td>
    </tr>
    <tr>
      <td><h5>standard-error</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>True, if the format should print to standard error instead of standard out.</td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Defines the parallelism of the Print sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.</td>
    </tr>
    </tbody>
</table>
