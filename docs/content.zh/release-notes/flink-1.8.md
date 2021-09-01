---
title: "Release Notes - Flink 1.8"
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

# Release Notes - Flink 1.8


These release notes discuss important aspects, such as configuration, behavior,
or dependencies, that changed between Flink 1.7 and Flink 1.8. Please read
these notes carefully if you are planning to upgrade your Flink version to 1.8.



### State

#### Continuous incremental cleanup of old Keyed State with TTL

We introduced TTL (time-to-live) for Keyed state in Flink 1.6
([FLINK-9510](https://issues.apache.org/jira/browse/FLINK-9510)).  This feature
allowed to clean up and make inaccessible keyed state entries when accessing
them. In addition state would now also being cleaned up when writing a
savepoint/checkpoint.

Flink 1.8 introduces continous cleanup of old entries for both the RocksDB
state backend
([FLINK-10471](https://issues.apache.org/jira/browse/FLINK-10471)) and the heap
state backend
([FLINK-10473](https://issues.apache.org/jira/browse/FLINK-10473)). This means
that old entries (according to the ttl setting) are continously being cleanup
up.

#### New Support for Schema Migration when restoring Savepoints

With Flink 1.7.0 we added support for changing the schema of state when using
the `AvroSerializer`
([FLINK-10605](https://issues.apache.org/jira/browse/FLINK-10605)). With Flink
1.8.0 we made great progress migrating all built-in `TypeSerializers` to a new
serializer snapshot abstraction that theoretically allows schema migration. Of
the serializers that come with Flink, we now support schema migration for the
`PojoSerializer`
([FLINK-11485](https://issues.apache.org/jira/browse/FLINK-11485)), and Java
`EnumSerializer`
([FLINK-11334](https://issues.apache.org/jira/browse/FLINK-11334)), As well as
for Kryo in limited cases
([FLINK-11323](https://issues.apache.org/jira/browse/FLINK-11323)).

#### Savepoint compatibility

Savepoints from Flink 1.2 that contain a Scala `TraversableSerializer`
are not compatible with Flink 1.8 anymore because of an update in this
serializer
([FLINK-11539](https://issues.apache.org/jira/browse/FLINK-11539)). You
can get around this restriction by first upgrading to a version
between Flink 1.3 and Flink 1.7 and then updating to Flink 1.8.

#### RocksDB version bump and switch to FRocksDB ([FLINK-10471](https://issues.apache.org/jira/browse/FLINK-10471))

We needed to switch to a custom build of RocksDB called FRocksDB because we
need certain changes in RocksDB for supporting continuous state cleanup with
TTL. The used build of FRocksDB is based on the upgraded version 5.17.2 of
RocksDB. For Mac OS X, RocksDB version 5.17.2 is supported only for OS X
version >= 10.13. See also: https://github.com/facebook/rocksdb/issues/4862.

### Maven Dependencies

#### Changes to bundling of Hadoop libraries with Flink ([FLINK-11266](https://issues.apache.org/jira/browse/FLINK-11266))

Convenience binaries that include hadoop are no longer released.

If a deployment relies on `flink-shaded-hadoop2` being included in
`flink-dist`, then you must manually download a pre-packaged Hadoop
jar from the optional components section of the [download
page]({{< downloads >}}) and copy it into the
`/lib` directory.  Alternatively, a Flink distribution that includes
hadoop can be built by packaging `flink-dist` and activating the
`include-hadoop` maven profile.

As hadoop is no longer included in `flink-dist` by default, specifying
`-DwithoutHadoop` when packaging `flink-dist` no longer impacts the build.

### Configuration

#### TaskManager configuration ([FLINK-11716](https://issues.apache.org/jira/browse/FLINK-11716))

`TaskManagers` now bind to the host IP address instead of the hostname
by default . This behaviour can be controlled by the configuration
option `taskmanager.network.bind-policy`. If your Flink cluster should
experience inexplicable connection problems after upgrading, try to
set `taskmanager.network.bind-policy: name` in your `flink-conf.yaml`
to return to the pre-1.8 behaviour.

### Table API

#### Deprecation of direct `Table` constructor usage ([FLINK-11447](https://issues.apache.org/jira/browse/FLINK-11447))

Flink 1.8 deprecates direct usage of the constructor of the `Table` class in
the Table API. This constructor would previously be used to perform a join with
a _lateral table_. You should now use `table.joinLateral()` or
`table.leftOuterJoinLateral()` instead.

This change is necessary for converting the Table class into an interface,
which will make the API more maintainable and cleaner in the future.

#### Introduction of new CSV format descriptor ([FLINK-9964](https://issues.apache.org/jira/browse/FLINK-9964))

This release introduces a new format descriptor for CSV files that is compliant
with RFC 4180. The new descriptor is available as
`org.apache.flink.table.descriptors.Csv`. For now, this can only be used
together with the Kafka connector. The old descriptor is available as
`org.apache.flink.table.descriptors.OldCsv` for use with file system
connectors.

#### Deprecation of static builder methods on TableEnvironment ([FLINK-11445](https://issues.apache.org/jira/browse/FLINK-11445))

In order to separate API from actual implementation, the static methods
`TableEnvironment.getTableEnvironment()` are deprecated. You should now use
`Batch/StreamTableEnvironment.create()` instead.

#### Change in the Maven modules of Table API ([FLINK-11064](https://issues.apache.org/jira/browse/FLINK-11064))

Users that had a `flink-table` dependency before, need to update their
dependencies to `flink-table-planner` and the correct dependency of
`flink-table-api-*`, depending on whether Java or Scala is used: one of
`flink-table-api-java-bridge` or `flink-table-api-scala-bridge`.

#### Change to External Catalog Table Builders ([FLINK-11522](https://issues.apache.org/jira/browse/FLINK-11522))

`ExternalCatalogTable.builder()` is deprecated in favour of
`ExternalCatalogTableBuilder()`.

#### Change to naming of Table API connector jars ([FLINK-11026](https://issues.apache.org/jira/browse/FLINK-11026))

The naming scheme for kafka/elasticsearch6 sql-jars has been changed.

In maven terms, they no longer have the `sql-jar` qualifier and the artifactId
is now prefixed with `flink-sql` instead of `flink`, e.g.,
`flink-sql-connector-kafka...`.

#### Change to how Null Literals are specified ([FLINK-11785](https://issues.apache.org/jira/browse/FLINK-11785))

Null literals in the Table API need to be defined with `nullOf(type)` instead
of `Null(type)` from now on. The old approach is deprecated.

### Connectors

#### Introduction of a new KafkaDeserializationSchema that give direct access to ConsumerRecord ([FLINK-8354](https://issues.apache.org/jira/browse/FLINK-8354))

For the Flink `KafkaConsumers`, we introduced a new `KafkaDeserializationSchema`
that gives direct access to the Kafka `ConsumerRecord`. This subsumes the
`KeyedSerializationSchema` functionality, which is deprecated but still available
for now.

#### FlinkKafkaConsumer will now filter restored partitions based on topic specification ([FLINK-10342](https://issues.apache.org/jira/browse/FLINK-10342))

Starting from Flink 1.8.0, the `FlinkKafkaConsumer` now always filters out
restored partitions that are no longer associated with a specified topic to
subscribe to in the restored execution. This behaviour did not exist in
previous versions of the `FlinkKafkaConsumer`. If you wish to retain the
previous behaviour, please use the
`disableFilterRestoredPartitionsWithSubscribedTopics()` configuration method on
the `FlinkKafkaConsumer`.

Consider this example: if you had a Kafka Consumer that was consuming
from topic `A`, you did a savepoint, then changed your Kafka consumer
to instead consume from topic `B`, and then restarted your job from
the savepoint. Before this change, your consumer would now consume
from both topic `A` and `B` because it was stored in state that the
consumer was consuming from topic `A`. With the change, your consumer
would only consume from topic `B` after restore because we filter the
topics that are stored in state using the configured topics.

### Miscellaneous Interface changes

#### The canEqual() method was dropped from the TypeSerializer interface ([FLINK-9803](https://issues.apache.org/jira/browse/FLINK-9803))

The `canEqual()` methods are usually used to make proper equality checks across
hierarchies of types. The `TypeSerializer` actually doesn't require this
property, so the method is now removed.

#### Removal of the CompositeSerializerSnapshot utility class ([FLINK-11073](https://issues.apache.org/jira/browse/FLINK-11073))

The `CompositeSerializerSnapshot` utility class has been removed. You should
now use `CompositeTypeSerializerSnapshot` instead, for snapshots of composite
serializers that delegate serialization to multiple nested serializers. Please
see
[here](http://nightlies.apache.org/flink/flink-docs-release-1.8/dev/stream/state/custom_serialization.html#implementing-a-compositetypeserializersnapshot)
for instructions on using `CompositeTypeSerializerSnapshot`.

### Memory management

In Fink 1.8.0 and prior version, the managed memory fraction of taskmanager is controlled by `taskmanager.memory.fraction`,
and with 0.7 as the default value. However, sometimes this will cause OOMs due to the fact that the default value of JVM
parameter `NewRatio` is 2, which means the old generation occupied only 2/3 (0.66) of the heap memory. So if you run into
this case, please manually change this value to a lower value.

{{< top >}}
