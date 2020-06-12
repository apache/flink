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
          <td><a href="{% link dev/table/connectors/formats/csv.md %}">CSV</a></td>
          <td><a href="{% link dev/table/connectors/kafka.md %}">Apache Kafka</a>,
          <a href="{% link dev/table/connectors/filesystem.md %}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{% link dev/table/connectors/formats/json.md %}">JSON</a></td>
         <td><a href="{% link dev/table/connectors/kafka.md %}">Apache Kafka</a>,
          <a href="{% link dev/table/connectors/filesystem.md %}">Filesystem</a>,
          <a href="{% link dev/table/connectors/elasticsearch.md %}">Elasticsearch</a></td>
       </tr>
        <tr>
          <td><a href="{% link dev/table/connectors/formats/avro.md %}">Apache Avro</a></td>
          <td><a href="{% link dev/table/connectors/kafka.md %}">Apache Kafka</a>,
           <a href="{% link dev/table/connectors/filesystem.md %}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{% link dev/table/connectors/formats/debezium.md %}">Debezium CDC</a></td>
         <td><a href="{% link dev/table/connectors/kafka.md %}">Apache Kafka</a></td>
        </tr>
        <tr>
         <td><a href="{% link dev/table/connectors/formats/canal.md %}">Canal CDC</a></td>
         <td><a href="{% link dev/table/connectors/kafka.md %}">Apache Kafka</a></td>
        </tr>
        <tr>
         <td><a href="{% link dev/table/connectors/formats/parquet.md %}">Apache Parquet</a></td>
         <td><a href="{% link dev/table/connectors/filesystem.md %}">Filesystem</a></td>
        </tr>
        <tr>
         <td><a href="{% link dev/table/connectors/formats/orc.md %}">Apache ORC</a></td>
         <td><a href="{% link dev/table/connectors/filesystem.md %}">Filesystem</a></td>
        </tr>
    </tbody>
</table>