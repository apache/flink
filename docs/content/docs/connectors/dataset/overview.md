---
title:  "DataSet Connectors"
weight: 3
type: docs
aliases:
- /dev/batch/connectors/
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

# DataSet Connectors

## Reading from and writing to file systems

The Apache Flink project supports multiple [file systems]({{< ref "docs/deployment/filesystems/overview" >}}) that can be used as backing stores
for input and output connectors. 

## Using formats

Flink supports the following formats:
<ul>
    <li><a href="/dev/batch/connectors/formats/avro.html">Avro formats</a></li>
    <li><a href="/dev/batch/connectors/hadoop.html">Hadoop formats</a></li>
    <li><a href="/dev/batch/connectors/formats/azure_table_storage.html">Microsoft Azure tale storage format</a></li>
    <li><a href="/dev/batch/connectors/formats/mongodb.html">MongoDb format</a></li>
</ul>
These formats are gradually replaced with new Flink Source API starting with Flink 1.14.0. 

{{< top >}}
