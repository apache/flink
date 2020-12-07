---
title: "Formats"
nav-id: sql-formats
nav-parent_id: sql-connectors
nav-pos: 1
nav-show_overview: true
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

Flink 提供了一套与表连接器（table connector）一起使用的表格式（table format）。表格式是一种存储格式，定义了如何把二进制数据映射到表的列上。

Flink 支持以下格式：

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">格式</th>
        <th class="text-left">支持的连接器</th>
      </tr>
    </thead>
    <tbody>
        <tr>
          <td><a href="{% link dev/table/connectors/formats/csv.zh.md %}">CSV</a></td>
          <td><a href="{% link dev/table/connectors/kafka.zh.md %}">Apache Kafka</a>,
          <a href="{% link dev/table/connectors/upsert-kafka.zh.md %}">Upsert Kafka</a>,
          <a href="{% link dev/table/connectors/kinesis.zh.md %}">Amazon Kinesis Data Streams</a>,
          <a href="{% link dev/table/connectors/filesystem.zh.md %}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{% link dev/table/connectors/formats/json.zh.md %}">JSON</a></td>
         <td><a href="{% link dev/table/connectors/kafka.zh.md %}">Apache Kafka</a>,
          <a href="{% link dev/table/connectors/upsert-kafka.zh.md %}">Upsert Kafka</a>,
          <a href="{% link dev/table/connectors/kinesis.zh.md %}">Amazon Kinesis Data Streams</a>,
          <a href="{% link dev/table/connectors/filesystem.zh.md %}">Filesystem</a>,
          <a href="{% link dev/table/connectors/elasticsearch.zh.md %}">Elasticsearch</a></td>
        </tr>
        <tr>
          <td><a href="{% link dev/table/connectors/formats/avro.zh.md %}">Apache Avro</a></td>
          <td><a href="{% link dev/table/connectors/kafka.zh.md %}">Apache Kafka</a>,
           <a href="{% link dev/table/connectors/upsert-kafka.zh.md %}">Upsert Kafka</a>,
           <a href="{% link dev/table/connectors/kinesis.zh.md %}">Amazon Kinesis Data Streams</a>,
           <a href="{% link dev/table/connectors/filesystem.zh.md %}">Filesystem</a></td>
        </tr>
        <tr>
          <td><a href="{% link dev/table/connectors/formats/avro-confluent.zh.md %}">Confluent Avro</a></td>
          <td><a href="{% link dev/table/connectors/kafka.zh.md %}">Apache Kafka</a>,
           <a href="{% link dev/table/connectors/upsert-kafka.zh.md %}">Upsert Kafka</a></td>
        </tr>
        <tr>
         <td><a href="{% link dev/table/connectors/formats/debezium.zh.md %}">Debezium CDC</a></td>
          <td><a href="{% link dev/table/connectors/kafka.zh.md %}">Apache Kafka</a>,
           <a href="{% link dev/table/connectors/filesystem.zh.md %}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{% link dev/table/connectors/formats/canal.zh.md %}">Canal CDC</a></td>
          <td><a href="{% link dev/table/connectors/kafka.zh.md %}">Apache Kafka</a>,
           <a href="{% link dev/table/connectors/filesystem.zh.md %}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{% link dev/table/connectors/formats/maxwell.zh.md %}">Maxwell CDC</a></td>
          <td><a href="{% link dev/table/connectors/kafka.zh.md %}">Apache Kafka</a>,
           <a href="{% link dev/table/connectors/filesystem.zh.md %}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{% link dev/table/connectors/formats/parquet.zh.md %}">Apache Parquet</a></td>
         <td><a href="{% link dev/table/connectors/filesystem.zh.md %}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{% link dev/table/connectors/formats/orc.zh.md %}">Apache ORC</a></td>
         <td><a href="{% link dev/table/connectors/filesystem.zh.md %}">Filesystem</a></td>
        </tr>
        <tr>
        <td><a href="{% link dev/table/connectors/formats/raw.zh.md %}">Raw</a></td>
        <td><a href="{% link dev/table/connectors/kafka.zh.md %}">Apache Kafka</a>,
          <a href="{% link dev/table/connectors/upsert-kafka.zh.md %}">Upsert Kafka</a>,
          <a href="{% link dev/table/connectors/kinesis.zh.md %}">Amazon Kinesis Data Streams</a>,
          <a href="{% link dev/table/connectors/filesystem.zh.md %}">Filesystem</a></td>
        </tr>
    </tbody>
</table>
