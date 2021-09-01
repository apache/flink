---
title: PubSub
weight: 20
type: docs
aliases:
  - /dev/table/connectors/pubsub.html
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

# Google Cloud PubSub SQL Connector

{{< label "Scan Source: Unbounded" >}}

This connector allows reading from Google Cloud PubSub.

Dependencies
------------

{{< sql_download_table "pubsub" >}}

The PubSub connector is not currently part of the binary distribution.
See how to link with it for cluster execution [here]({{< ref "docs/dev/datastream/project-configuration" >}}).

How to create a PubSub table
----------------

The example below shows how to create a PubSub table:

```sql
CREATE TABLE PubSubTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'pubsub',
  'format' = 'json',
  'projectName' = 'gcpProject',
  'subscription' = 'mySubscription'
)
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
      <td>Specify what connector to use, for PubSub use <code>'pubsub'</code>.</td>
    </tr>
    <tr>
      <td><h5>projectName</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the GCP project that contains the subscription.</td>
    </tr>
    <tr>
      <td><h5>subscription</h5></td>
      <td>required by source</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the PubSub subscription to read from.</td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize the value part of PubSub messages.
      Please refer to the <a href="{{< ref "docs/connectors/table/formats/overview" >}}">formats</a> page for
      more details and more format options.
      </td>
    </tr>
 
    </tbody>
</table>
