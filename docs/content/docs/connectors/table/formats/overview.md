---
title: "Formats"
weight: 1
type: docs
aliases:
  - /dev/table/connectors/formats/
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

# Formats

Flink provides a set of table formats that can be used with table connectors. A table format is a storage format defines how to map binary data onto table columns.

Flink supports the following formats:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Formats</th>
        <th class="text-left">Supported Connectors</th>
      </tr>
    </thead>
    <tbody>
        <tr>
          <td><a href="{{< ref "docs/connectors/table/formats/csv" >}}">CSV</a></td>
          <td><a href="{{< ref "docs/connectors/table/kafka" >}}">Apache Kafka</a>,
          <a href="{{< ref "docs/connectors/table/upsert-kafka" >}}">Upsert Kafka</a>,
          <a href="{{< ref "docs/connectors/table/kinesis" >}}">Amazon Kinesis Data Streams</a>,
          <a href="{{< ref "docs/connectors/table/firehose" >}}">Amazon Kinesis Data Firehose</a>,
          <a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{{< ref "docs/connectors/table/formats/json" >}}">JSON</a></td>
         <td><a href="{{< ref "docs/connectors/table/kafka" >}}">Apache Kafka</a>,
          <a href="{{< ref "docs/connectors/table/upsert-kafka" >}}">Upsert Kafka</a>,
          <a href="{{< ref "docs/connectors/table/kinesis" >}}">Amazon Kinesis Data Streams</a>,
          <a href="{{< ref "docs/connectors/table/firehose" >}}">Amazon Kinesis Data Firehose</a>,
          <a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a>,
          <a href="{{< ref "docs/connectors/table/elasticsearch" >}}">Elasticsearch</a></td>
        </tr>
        <tr>
          <td><a href="{{< ref "docs/connectors/table/formats/avro" >}}">Apache Avro</a></td>
          <td><a href="{{< ref "docs/connectors/table/kafka" >}}">Apache Kafka</a>,
           <a href="{{< ref "docs/connectors/table/upsert-kafka" >}}">Upsert Kafka</a>,
           <a href="{{< ref "docs/connectors/table/kinesis" >}}">Amazon Kinesis Data Streams</a>,
           <a href="{{< ref "docs/connectors/table/firehose" >}}">Amazon Kinesis Data Firehose</a>,
           <a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a></td>
        </tr>
        <tr>
          <td><a href="{{< ref "docs/connectors/table/formats/avro-confluent" >}}">Confluent Avro</a></td>
          <td><a href="{{< ref "docs/connectors/table/kafka" >}}">Apache Kafka</a>,
           <a href="{{< ref "docs/connectors/table/upsert-kafka" >}}">Upsert Kafka</a></td>
        </tr>
        <tr>
         <td><a href="{{< ref "docs/connectors/table/formats/debezium" >}}">Debezium CDC</a></td>
          <td><a href="{{< ref "docs/connectors/table/kafka" >}}">Apache Kafka</a>,
           <a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{{< ref "docs/connectors/table/formats/canal" >}}">Canal CDC</a></td>
          <td><a href="{{< ref "docs/connectors/table/kafka" >}}">Apache Kafka</a>,
           <a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{{< ref "docs/connectors/table/formats/maxwell" >}}">Maxwell CDC</a></td>
          <td><a href="{{< ref "docs/connectors/table/kafka" >}}">Apache Kafka</a>,
           <a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{{< ref "docs/connectors/table/formats/ogg" >}}">OGG CDC</a></td>
          <td><a href="{{< ref "docs/connectors/table/kafka" >}}">Apache Kafka</a>,
           <a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{{< ref "docs/connectors/table/formats/parquet" >}}">Apache Parquet</a></td>
         <td><a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{{< ref "docs/connectors/table/formats/orc" >}}">Apache ORC</a></td>
         <td><a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a></td>
        </tr>
        <tr>
        <td><a href="{{< ref "docs/connectors/table/formats/raw" >}}">Raw</a></td>
        <td><a href="{{< ref "docs/connectors/table/kafka" >}}">Apache Kafka</a>,
          <a href="{{< ref "docs/connectors/table/upsert-kafka" >}}">Upsert Kafka</a>,
          <a href="{{< ref "docs/connectors/table/kinesis" >}}">Amazon Kinesis Data Streams</a>,
          <a href="{{< ref "docs/connectors/table/firehose" >}}">Amazon Kinesis Data Firehose</a>,
          <a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a></td>
        </tr>
    </tbody>
</table>
