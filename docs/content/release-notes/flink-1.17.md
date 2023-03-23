---
title: "Release Notes - Flink 1.17"
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

# Release notes - Flink 1.17

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 1.16 and Flink 1.17. Please read these notes carefully if you are
planning to upgrade your Flink version to 1.17.


### Clusters & Deployment

#### Only one Zookeeper version is bundled in `flink-dist`
##### [FLINK-30237](https://issues.apache.org/jira/browse/FLINK-30237)
The Flink distribution no longer bundles 2 different Zookeeper client jars (one in lib, one in lib/opt 
respectively). Instead, only 1 client will be bundled within the flink-dist jar. This has no 
effect on the supported Zookeeper server versions.


### Table API & SQL

#### Incompatible plan change of event time temporal join on an upsert source
##### [FLINK-29849](https://issues.apache.org/jira/browse/FLINK-29849)
A correctness issue when do event time temporal join with a versioned table backed by an upsert 
source was resolved. When the right input of the join is an upsert source, it no longer generates 
a ChangelogNormalize node for it. This is an incompatible plan change compared to 1.16.0.

#### Incompatible plan change of filter after temporal join
##### [FLINK-28988](https://issues.apache.org/jira/browse/FLINK-28988)
After the patch is applied, the filter will no longer be pushed down into both inputs of the event time 
temporal join. Note this may cause incompatible plan changes compared to 1.16.0, e.g., when left 
input is an upsert source (use upsert-kafka connector), the query plan will remove the 
ChangelogNormalize node which appeared in 1.16.0.

### Connectors & Libraries

#### Remove cassandra connector from master branch
##### [FLINK-30312](https://issues.apache.org/jira/browse/FLINK-30312)
The Cassandra connector has been externalized and is no longer released as part of the main Flink 
release. Downloads can be found at https://flink.apache.org/downloads.html and the
source code at https://github.com/apache/flink-connector-cassandra.

#### Remove Pulsar connector from master branch 
##### [FLINK-30397](https://issues.apache.org/jira/browse/FLINK-30397)
The Pulsar connector has been externalized and is no longer bundled and released as part of the 
main Flink release. Downloads can be found at https://flink.apache.org/downloads.html and the 
source code at https://github.com/apache/flink-connector-pulsar.

#### Remove HCatalog
##### [FLINK-29669](https://issues.apache.org/jira/browse/FLINK-29669)
The HCatalog connector has been removed from Flink. You can use the Hive connector as a replacement.

#### Remove Gelly
##### [FLINK-29668](https://issues.apache.org/jira/browse/FLINK-29668)
Gelly has been removed from Flink. Current users of Gelly should not upgrade to Flink 1.17 but 
stay on an older version. If you're looking for iterations support, you could investigate 
[Flink ML Iteration](https://nightlies.apache.org/flink/flink-ml-docs-stable/docs/development/iteration/) 
as a potential successor.

#### Support watermark alignment of source splits
##### [FLINK-28853](https://issues.apache.org/jira/browse/FLINK-28853)
Since Flink 1.17, source connectors have to implement watermark alignment of source split in order 
to use the watermark alignment feature. The required methods to implement are: 
`SourceReader#pauseOrResumeSplits` and `SplitReader#pauseOrResumeSplits`. 

If you are migrating from 
Flink <= 1.16.x, and you are using watermark alignment, but at the same time you are not able to 
upgrade/modify your connector, you can disable per split alignment via setting 
`pipeline.watermark-alignment.allow-unaligned-source-splits` to true. Note that by doing so, 
watermark alignment will be working properly only when your number of splits equals to the 
parallelism of the source operator.

#### Remove deprecated MiniClusterResource
##### [FLINK-29548](https://issues.apache.org/jira/browse/FLINK-29548)
The deprecated `MiniClusterResource` in `flink-test-utils` has been removed. The 
`MiniClusterWithClientResource` is a drop-in replacement.

#### Kinesis connector doesn't shade jackson dependency
##### [FLINK-14896](https://issues.apache.org/jira/browse/FLINK-14896)
Kinesis connector now shades and relocates transitive Jackson dependencies of `flink-connector-kinesis`. If your Flink job 
was transitively relying on the these, you may need to include additional Jackson dependencies into 
your project.

### Runtime & Coordination

#### Speculative execution take input data amount into account when detecting slow tasks
##### [FLINK-30707](https://issues.apache.org/jira/browse/FLINK-30707)
The slow task detecting is improved for speculative execution. Previously, it only considered the 
execution time of tasks when deciding which tasks are slow. It now takes the input data volume of 
tasks into account. Tasks which have a longer execution time but consumes more data may not be 
considered as slow. This improvement helps to eliminate the negative impacts of data skew on slow 
task detecting.

#### Use adaptive batch scheduler as default scheduler for batch jobs
##### [FLINK-30682](https://issues.apache.org/jira/browse/FLINK-30682)
Adaptive batch scheduler is now used for batch jobs by default. It will automatically decide the 
parallelism of operators. The keys and values of related configuration items are improved for ease 
of use. More details can be found in the 
[document](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/elastic_scaling/#adaptive-batch-scheduler).

#### Simplify network memory configurations for TaskManager
##### [FLINK-30469](https://issues.apache.org/jira/browse/FLINK-30469)
The default value of `taskmanager.memory.network.max` has changed from `1g` to `Long#MAX_VALUE`, 
to reduce the number of config options the user needs to tune when trying to increase the network 
memory size. This may affect the performance when this option is not explicitly configured, due to 
potential changes of network memory size, heap and managed memory size when the total memory
size is fixed. To go back to the previous behavior, the user can explicitly configure this option to 
the previous default value of `1g`.

A threshold is introduced for controlling the number of required buffers among all buffers needed 
for reading data from upstream tasks. Reducing the number of required buffers helps reduce the 
chance of failures due to insufficient network buffers, at the price of potential performance 
impact. By default, the number of required buffers is only reduced for batch workloads, while stay 
unchanged for streaming workloads. This can be tuned via 
`taskmanager.network.memory.read-buffer.required-per-gate.max`. See the description of the config 
option for more details.

### Metric Reporters

#### Only support reporter factories for instantiation
##### [FLINK-24235](https://issues.apache.org/jira/browse/FLINK-24235)
Configuring reporters by their class is no longer supported. Reporter implementations must provide 
a MetricReporterFactory, and all configurations must be migrated to such a factory.

#### `UseLogicalIdentifier` makes datadog consider metric as custom
##### [FLINK-30383](https://issues.apache.org/jira/browse/FLINK-30383)
The Datadog reporter now adds a "flink." prefix to metric identifiers if "useLogicalIdentifier" is 
enabled. This is required for these metrics to be recognized as Flink metrics, not custom ones.

#### Use separate Prometheus CollectorRegistries
##### [FLINK-30020](https://issues.apache.org/jira/browse/FLINK-30020)
The PrometheusReporters now use a separate CollectorRegistry for each reporter instance instead of 
the singleton default registry. This generally shouldn't impact setups, but it may break code that 
indirectly interacts with the reporter via the singleton instance (e.g., a test trying to assert 
what metrics are reported).

### Checkpoints

#### Drop TypeSerializerConfigSnapshot and savepoint support from Flink versions < 1.8.0
##### [FLINK-29807](https://issues.apache.org/jira/browse/FLINK-29807)
Savepoints using `TypeSerializerConfigSnapshot` are no longer supported. That means that all savepoints 
from Flink < 1.8.0 are no longer supported. Furthermore, savepoints from Flink < 1.17.0 created 
with custom serializer using deprecated since Flink 1.8.0 class `TypeSerializerConfigSnapshot` are 
also no longer supported.

If you are only using built-in serializers (Pojo, Kryo, Avro, 
Tuple, ...), and your savepoint is from Flink >= 1.8.0, you don't have to do anything.

If you are only using built-in serializers (Pojo, Kryo, Avro, Tuple, ...), and your savepoint is 
from Flink < 1.8.0, please first upgrade your job to 1.8.x <= Flink <= 1.16.x, before upgrading in 
a second step to Flink >= 1.17.x.

If previously you were using a custom serializer that depends on `TypeSerializerConfigSnapshot`, 
please upgrade your serializer to `TypeSerializerSnapshot` while still using 1.8.x <= Flink <= 1.16.x, 
take a savepoint and restore from that savepoint in Flink >= 1.7.0.

### Python

#### Remove duplicated operators in the StreamGraph for Python DataStream API jobs
##### [FLINK-31272](https://issues.apache.org/jira/browse/FLINK-31272)
This duplicated operator issue has been addressed since 1.15.4, 1.16.2 and 1.17.0. For jobs which 
are not affected by this issue, there are no backward compatibility issues. However, for jobs which 
are affected, it may not be possible to restore from savepoints generated from versions 
1.15.0 ~ 1.15.3 and 1.16.0 ~ 1.16.1.

#### Adds support of Python 3.10 and removes support of Python 3.6
##### [FLINK-29421](https://issues.apache.org/jira/browse/FLINK-29421)
PyFlink 1.17 will support Python 3.10 and remove the support of Python 3.6.

### Dependency upgrades

#### Upgrade FRocksDB to 6.20.3-ververica-2.0
##### [FLINK-30836](https://issues.apache.org/jira/browse/FLINK-30836)
Upgrade FRocksDB to 6.20.3-ververica-2.0.

#### Upgrade the minimal supported hadoop version to 2.10.2
##### [FLINK-29710](https://issues.apache.org/jira/browse/FLINK-29710)
The minimum Hadoop version supported by Apache Flink has been updated to version 2.10.2. For Hadoop 
3, the minimum Hadoop version that is now supported is version 3.2.3.

#### Upgrade Calcite version to 1.29.0
##### [FLINK-20873](https://issues.apache.org/jira/browse/FLINK-20873)
##### [FLINK-21239](https://issues.apache.org/jira/browse/FLINK-21239)
##### [FLINK-29932](https://issues.apache.org/jira/browse/FLINK-29932)
Calcite upgrade brings optimizations that change logical plans for some queries involving `Sarg` and 
queries with `count` of non-distinct values. Please note that the execution plan changes for 
certain SQL queries after the upgrade, so savepoints are not backward compatible in these cases. 

#### Update dependency versions for PyFlink
##### [FLINK-29421](https://issues.apache.org/jira/browse/FLINK-29421)
For support of Python 3.10, PyFlink updates some dependencies: 
- apache-beam: 2.43.0
- pemja: 0.3.0
- cloudpickle: 2.2.0
