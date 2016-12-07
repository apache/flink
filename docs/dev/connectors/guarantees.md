---
title: "Fault Tolerance Guarantees of Data Sources and Sinks"
nav-title: Fault Tolerance Guarantees
nav-parent_id: connectors
nav-pos: 0
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

Flink's fault tolerance mechanism recovers programs in the presence of failures and
continues to execute them. Such failures include machine hardware failures, network failures,
transient program failures, etc.

Flink can guarantee exactly-once state updates to user-defined state only when the source participates in the
snapshotting mechanism. The following table lists the state update guarantees of Flink coupled with the bundled connectors.

Please read the documentation of each connector to understand the details of the fault tolerance guarantees.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Source</th>
      <th class="text-left" style="width: 25%">Guarantees</th>
      <th class="text-left">Notes</th>
    </tr>
   </thead>
   <tbody>
        <tr>
            <td>Apache Kafka</td>
            <td>exactly once</td>
            <td>Use the appropriate Kafka connector for your version</td>
        </tr>
        <tr>
            <td>AWS Kinesis Streams</td>
            <td>exactly once</td>
            <td></td>
        </tr>
        <tr>
            <td>RabbitMQ</td>
            <td>at most once (v 0.10) / exactly once (v 1.0) </td>
            <td></td>
        </tr>
        <tr>
            <td>Twitter Streaming API</td>
            <td>at most once</td>
            <td></td>
        </tr>
        <tr>
            <td>Collections</td>
            <td>exactly once</td>
            <td></td>
        </tr>
        <tr>
            <td>Files</td>
            <td>exactly once</td>
            <td></td>
        </tr>
        <tr>
            <td>Sockets</td>
            <td>at most once</td>
            <td></td>
        </tr>
  </tbody>
</table>

To guarantee end-to-end exactly-once record delivery (in addition to exactly-once state semantics), the data sink needs
to take part in the checkpointing mechanism. The following table lists the delivery guarantees (assuming exactly-once
state updates) of Flink coupled with bundled sinks:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Sink</th>
      <th class="text-left" style="width: 25%">Guarantees</th>
      <th class="text-left">Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>HDFS rolling sink</td>
        <td>exactly once</td>
        <td>Implementation depends on Hadoop version</td>
    </tr>
    <tr>
        <td>Elasticsearch</td>
        <td>at least once</td>
        <td></td>
    </tr>
    <tr>
        <td>Kafka producer</td>
        <td>at least once</td>
        <td></td>
    </tr>
    <tr>
        <td>Cassandra sink</td>
        <td>at least once / exactly once</td>
        <td>exactly once only for idempotent updates</td>
    </tr>
    <tr>
        <td>AWS Kinesis Streams</td>
        <td>at least once</td>
        <td></td>
    </tr>
    <tr>
        <td>File sinks</td>
        <td>at least once</td>
        <td></td>
    </tr>
    <tr>
        <td>Socket sinks</td>
        <td>at least once</td>
        <td></td>
    </tr>
    <tr>
        <td>Standard output</td>
        <td>at least once</td>
        <td></td>
    </tr>
    <tr>
        <td>Redis sink</td>
        <td>at least once</td>
        <td></td>
    </tr>
  </tbody>
</table>

{% top %}
