---
title: "Data Source 和 Sink 的容错保证"
nav-title: 容错保证
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

当程序出现错误的时候，Flink 的容错机制能恢复并继续运行程序。这种错误包括机器硬件故障、网络故障、瞬态程序故障等等。

只有当 source 参与了快照机制的时候，Flink 才能保证对自定义状态的精确一次更新。下表列举了 Flink 与其自带连接器的状态更新的保证。

请阅读各个连接器的文档来了解容错保证的细节。

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
            <td>精确一次</td>
            <td>根据你的版本用恰当的 Kafka 连接器</td>
        </tr>
        <tr>
            <td>AWS Kinesis Streams</td>
            <td>精确一次</td>
            <td></td>
        </tr>
        <tr>
            <td>RabbitMQ</td>
            <td>至多一次 (v 0.10) / 精确一次 (v 1.0) </td>
            <td></td>
        </tr>
        <tr>
            <td>Twitter Streaming API</td>
            <td>至多一次</td>
            <td></td>
        </tr>
        <tr>
            <td>Google PubSub</td>
            <td>至少一次</td>
            <td></td>
        </tr>
        <tr>
            <td>Collections</td>
            <td>精确一次</td>
            <td></td>
        </tr>
        <tr>
            <td>Files</td>
            <td>精确一次</td>
            <td></td>
        </tr>
        <tr>
            <td>Sockets</td>
            <td>至多一次</td>
            <td></td>
        </tr>
  </tbody>
</table>

为了保证端到端精确一次的数据交付（在精确一次的状态语义上更进一步），sink需要参与 checkpointing 机制。下表列举了 Flink 与其自带 sink 的交付保证（假设精确一次状态更新）。

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
        <td>HDFS BucketingSink</td>
        <td>精确一次</td>
        <td>实现方法取决于 Hadoop 的版本</td>
    </tr>
    <tr>
        <td>Elasticsearch</td>
        <td>至少一次</td>
        <td></td>
    </tr>
    <tr>
        <td>Kafka producer</td>
        <td>至少一次 / 精确一次</td>
        <td>当使用事务生产者时，保证精确一次 (v 0.11+)</td>
    </tr>
    <tr>
        <td>Cassandra sink</td>
        <td>至少一次 / 精确一次</td>
        <td>只有当更新是幂等时，保证精确一次</td>
    </tr>
    <tr>
        <td>AWS Kinesis Streams</td>
        <td>至少一次</td>
        <td></td>
    </tr>
    <tr>
        <td>File sinks</td>
        <td>精确一次</td>
        <td></td>
    </tr>
    <tr>
        <td>Socket sinks</td>
        <td>至少一次</td>
        <td></td>
    </tr>
    <tr>
        <td>Standard output</td>
        <td>至少一次</td>
        <td></td>
    </tr>
    <tr>
        <td>Redis sink</td>
        <td>至少一次</td>
        <td></td>
    </tr>
  </tbody>
</table>

{% top %}
