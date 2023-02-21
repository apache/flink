---
title: "Release Notes - Flink 1.7"
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

# Release Notes - Flink 1.7

These release notes discuss important aspects, such as configuration, behavior, or dependencies, that changed between Flink 1.6 and Flink 1.7. Please read these notes carefully if you are planning to upgrade your Flink version to 1.7.

### Scala 2.12 support

When using Scala `2.12` you might have to add explicit type annotations in places where they were not required when using Scala `2.11`.
This is an excerpt from the `TransitiveClosureNaive.scala` example in the Flink code base that shows the changes that could be required.

Previous code:
```
val terminate = prevPaths
 .coGroup(nextPaths)
 .where(0).equalTo(0) {
   (prev, next, out: Collector[(Long, Long)]) => {
     val prevPaths = prev.toSet
     for (n <- next)
       if (!prevPaths.contains(n)) out.collect(n)
   }
}
```

With Scala `2.12` you have to change it to:
```
val terminate = prevPaths
 .coGroup(nextPaths)
 .where(0).equalTo(0) {
   (prev: Iterator[(Long, Long)], next: Iterator[(Long, Long)], out: Collector[(Long, Long)]) => {
       val prevPaths = prev.toSet
       for (n <- next)
         if (!prevPaths.contains(n)) out.collect(n)
     }
}
```

The reason for this is that Scala `2.12` changes how lambdas are implemented.
They now use the lambda support using SAM interfaces introduced in Java 8.
This makes some method calls ambiguous because now both Scala-style lambdas and SAMs are candidates for methods were it was previously clear which method would be invoked.

### State evolution

Before Flink 1.7, serializer snapshots were implemented as a `TypeSerializerConfigSnapshot` (which is now deprecated, and will eventually be removed in the future to be fully replaced by the new `TypeSerializerSnapshot` interface introduced in 1.7).
Moreover, the responsibility of serializer schema compatibility checks lived within the `TypeSerializer`, implemented in the `TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)` method. 

To be future-proof and to have flexibility to migrate your state serializers and schema, it is highly recommended to migrate from the old abstractions. 
Details and migration guides can be found [here](https://nightlies.apache.org/flink/flink-docs-master/dev/stream/state/custom_serialization.html).

### Removal of the legacy mode

Flink no longer supports the legacy mode.
If you depend on this, then please use Flink `1.6.x`.

### Savepoints being used for recovery

Savepoints are now used while recovering.
Previously when using exactly-once sink one could get into problems with duplicate output data when a failure occurred after a savepoint was taken but before the next checkpoint occurred.
This results in the fact that savepoints are no longer exclusively under the control of the user.
Savepoint should not be moved nor deleted if there was no newer checkpoint or savepoint taken.

### MetricQueryService runs in separate thread pool

The metric query service runs now in its own `ActorSystem`.
It needs consequently to open a new port for the query services to communicate with each other.
The [query service port]({{< ref "docs/deployment/config" >}}#metrics-internal-query-service-port) can be configured in `flink-conf.yaml`.

### Granularity of latency metrics

The default granularity for latency metrics has been modified.
To restore the previous behavior users have to explicitly set the [granularity]({{< ref "docs/deployment/config" >}}#metrics-latency-granularity) to `subtask`.

### Latency marker activation

Latency metrics are now disabled by default, which will affect all jobs that do not explicitly set the `latencyTrackingInterval` via `ExecutionConfig#setLatencyTrackingInterval`.
To restore the previous default behavior users have to configure the [latency interval]({{< ref "docs/deployment/config" >}}#metrics-latency-interval) in `flink-conf.yaml`.

### Relocation of Hadoop's Netty dependency

We now also relocate Hadoop's Netty dependency from `io.netty` to `org.apache.flink.hadoop.shaded.io.netty`.
You can now bundle your own version of Netty into your job but may no longer assume that `io.netty` is present in the `flink-shaded-hadoop2-uber-*.jar` file.

### Local recovery fixed

With the improvements to Flink's scheduling, it can no longer happen that recoveries require more slots than before if local recovery is enabled.
Consequently, we encourage our users to enable [local recovery]({{< ref "docs/deployment/config" >}}#state-backend-local-recovery) in `flink-conf.yaml`.

### Support for multi slot TaskManagers

Flink now properly supports `TaskManagers` with multiple slots.
Consequently, `TaskManagers` can now be started with an arbitrary number of slots and it is no longer recommended to start them with a single slot.

### StandaloneJobClusterEntrypoint generates JobGraph with fixed JobID

The `StandaloneJobClusterEntrypoint`, which is launched by the script `standalone-job.sh` and used for the job-mode container images, now starts all jobs with a fixed `JobID`.
Thus, in order to run a cluster in HA mode, one needs to set a different [cluster id]({{< ref "docs/deployment/config" >}}#high-availability-cluster-id) for each job/cluster. 

<!-- Should be removed once FLINK-10911 is fixed -->
### Scala shell does not work with Scala 2.12

Flink's Scala shell does not work with Scala 2.12.
Therefore, the module `flink-scala-shell` is not being released for Scala 2.12.

See [FLINK-10911](https://issues.apache.org/jira/browse/FLINK-10911) for more details.  

<!-- Remove once FLINK-10712 has been fixed -->
### Limitations of failover strategies
Flink's non-default failover strategies are still a very experimental feature which come with a set of limitations.
You should only use this feature if you are executing a stateless streaming job.
In any other cases, it is highly recommended to remove the config option `jobmanager.execution.failover-strategy` from your `flink-conf.yaml` or set it to `"full"`.

In order to avoid future problems, this feature has been removed from the documentation until it will be fixed.
See [FLINK-10880](https://issues.apache.org/jira/browse/FLINK-10880) for more details.

### SQL over window preceding clause

The over window `preceding` clause is now optional.
It defaults to `UNBOUNDED` if not specified.

### OperatorSnapshotUtil writes v2 snapshots

Snapshots created with `OperatorSnapshotUtil` are now written in the savepoint format `v2`.

### SBT projects and the MiniClusterResource

If you have a `sbt` project which uses the `MiniClusterResource`, you now have to add the `flink-runtime` test-jar dependency explicitly via:

`libraryDependencies += "org.apache.flink" %% "flink-runtime" % flinkVersion % Test classifier "tests"`

The reason for this is that the `MiniClusterResource` has been moved from `flink-test-utils` to `flink-runtime`.
The module `flink-test-utils` has correctly a `test-jar` dependency on `flink-runtime`.
However, `sbt` does not properly pull in transitive `test-jar` dependencies as described in this [sbt issue](https://github.com/sbt/sbt/issues/2964).
Consequently, it is necessary to specify the `test-jar` dependency explicitly.

{{< top >}}
