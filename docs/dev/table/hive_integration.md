---
title: "Hive Integration"
is_beta: true
nav-parent_id: tableapi
nav-pos: 9
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

With its wide adoption in streaming processing, Flink has also shown its potentials in batch processing. Improving Flink’s batch processing, especially in terms of SQL, would offer user a complete set of solutions for both their streaming and batch processing needs.

On the other hand, Hive has established its focal point in big data technology and its complete ecosystem. For most of big data users, Hive is not only a SQL engine for big data analytics and ETL, but also a data management platform, on which data are discovered, defined, and evolved. In another words, Hive is a de facto standard for big data on Hadoop.

Therefore, it’s imperative for Flink to integrate with Hive ecosystem to further its reach to batch and SQL users. In doing that, integration with Hive metadata and data is necessary. 

The goal here is neither to replace nor to copy Hive. Rather, we leverage Hive as much as we can. Flink is an alternative batch engine to Hive's batch engine. With Flink and Flink SQL, both Hive and Flink users can enjoy Hive’s rich SQL functionality and ecosystem as well as Flink's outstanding batch processing performance.


Supported Hive version
----------------------

The target versions are Hive `2.3.4` and `1.2.1`


Hive Metastore Integration
--------------------------

There are two aspects of Hive metadata integration:

1. Make Hive’s meta-objects such as tables and views available to Flink and Flink is also able to create such meta-objects for and in Hive. This is achieved through `HiveCatalog`.

2. Persist Flink’s meta-objects (tables, views, and UDFs) using Hive Metastore as an persistent storage. This is achieved through `HiveCatalog`, which is under active development.

For how to use and configure `HiveCatalog` in Flink, see [Catalogs]({{ site.baseurl }}/dev/table/catalog.html)

Hive Data Integration
---------------------

Please refer to [Connecting to other systems]({{ site.baseurl }}/dev/batch/connectors.html) for how to query Hive data using Flink's Hive data connector. 


Examples
--------

For more detailed examples using Table API or SQL, please refer to [Hive Integration Example] ({{ site.baseurl }}/dev/table/hive_integration_example.html)


Trouble Shoot
-------------

Limitations & Future
--------------------

Currently Flink's Hive data connector does not support writing into partitions. The feature of writing into partitions is under active development now.
