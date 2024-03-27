---
title: "Release Notes - Flink 1.19"
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

# Release notes - Flink 1.19

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 1.18 and Flink 1.19. Please read these notes carefully if you are 
planning to upgrade your Flink version to 1.19.

## Dependency upgrades

#### Drop support for python 3.7

##### [FLINK-33029](https://issues.apache.org/jira/browse/FLINK-33029)

#### Add support for python 3.11

##### [FLINK-33030](https://issues.apache.org/jira/browse/FLINK-33030)

## Build System

#### Support Java 21

##### [FLINK-33163](https://issues.apache.org/jira/browse/FLINK-33163)
Apache Flink was made ready to compile and run with Java 21. This feature is still in beta mode.
Issues should be reported in Flink's bug tracker.

## Checkpoints

#### Deprecate RestoreMode#LEGACY

##### [FLINK-34190](https://issues.apache.org/jira/browse/FLINK-34190)

`RestoreMode#LEGACY` is deprecated. Please use `RestoreMode#CLAIM` or `RestoreMode#NO_CLAIM` mode
instead to get a clear state file ownership when restoring.

#### CheckpointsCleaner clean individual checkpoint states in parallel

##### [FLINK-33090](https://issues.apache.org/jira/browse/FLINK-33090)

Now when disposing of no longer needed checkpoints, every state handle/state file will be disposed
in parallel by the ioExecutor, vastly improving the disposing speed of a single checkpoint (for
large checkpoints, the disposal time can be improved from 10 minutes to < 1 minute). The old
behavior can be restored by setting `state.checkpoint.cleaner.parallel-mode` to false.

#### Support using larger checkpointing interval when source is processing backlog

##### [FLINK-32514](https://issues.apache.org/jira/browse/FLINK-32514)

`ProcessingBacklog` is introduced to demonstrate whether a record should be processed with low latency
or high throughput. `ProcessingBacklog` can be set by source operators and can be used to change the
checkpoint interval of a job during runtime.

#### Allow triggering Checkpoints through command line client

##### [FLINK-6755](https://issues.apache.org/jira/browse/FLINK-6755)

The command line interface supports triggering a checkpoint manually. Usage:
```
./bin/flink checkpoint $JOB_ID [-full]
```
By specifying the '-full' option, a full checkpoint is triggered. Otherwise an incremental
checkpoint is triggered if the job is configured to take incremental ones periodically.


## Runtime & Coordination

#### Migrate TypeSerializerSnapshot#resolveSchemaCompatibility

##### [FLINK-30613](https://issues.apache.org/jira/browse/FLINK-30613)

In Flink 1.19, the old method of resolving schema compatibility has been deprecated and the new one
is introduced. See [FLIP-263](https://cwiki.apache.org/confluence/display/FLINK/FLIP-263%3A+Improve+resolving+schema+compatibility?src=contextnavpagetreemode) for more details.
Please migrate to the new method following [link](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/fault-tolerance/serialization/custom_serialization/#migrating-from-deprecated-typeserializersnapshotresolveschemacompatibilitytypeserializer-newserializer-before-flink-119).

#### Deprecate old serialization config methods and options

##### [FLINK-34122](https://issues.apache.org/jira/browse/FLINK-34122)

Configuring serialization behavior through hard codes is deprecated, because you need to modify the
codes when upgrading the job version. You should configure this via options
`pipeline.serialization-config`, `pipeline.force-avro`, `pipeline.force-kryo`, and `pipeline.generic-types`.
Registration of instance-level serializers is deprecated, using class-level serializers instead.
For more information and code examples, please refer to [link](https://cwiki.apache.org/confluence/display/FLINK/FLIP-398:+Improve+Serialization+Configuration+And+Usage+In+Flink).

#### Migrate string configuration key to ConfigOption

##### [FLINK-34079](https://issues.apache.org/jira/browse/FLINK-34079)

We have deprecated all setXxx and getXxx methods except `getString(String key, String defaultValue)`
and `setString(String key, String value)`, such as: `setInteger`, `setLong`, `getInteger` and `getLong` etc.
We strongly recommend that users and developers use the ConfigOption-based get and set methods directly.

#### Support System out and err to be redirected to LOG or discarded

##### [FLINK-33625](https://issues.apache.org/jira/browse/FLINK-33625)

`System.out` and `System.err` output the content to the `taskmanager.out` and `taskmanager.err` files.
In a production environment, if flink users use them to print a lot of content, the limits of yarn
or kubernetes may be exceeded, eventually causing the TaskManager to be killed. Flink supports
redirecting the `System.out` and `System.err` to the log file, and the log file can be rolled to
avoid unlimited disk usage.

#### Support standard YAML for FLINK configuration

##### [FLINK-33297](https://issues.apache.org/jira/browse/FLINK-33297)

Starting with Flink 1.19, Flink has officially introduced full support for the standard YAML 1.2
syntax ([FLIP-366](https://cwiki.apache.org/confluence/display/FLINK/FLIP-366%3A+Support+standard+YAML+for+FLINK+configuration?src=contextnavpagetreemode)). The default configuration file has been changed to `config.yaml` and placed in the
`conf/` directory. Users should directly modify this file to configure Flink.
If users want to use the legacy configuration file `flink-conf.yaml`, they need to copy this file
into the `conf/` directory. Once the legacy configuration file `flink-conf.yaml` is detected, Flink
will prioritize using it as the configuration file. In the upcoming Flink 2.0, the `flink-conf.yaml`
configuration file will no longer work.
More details could be found at [flink-configuration-file](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/config/#flink-configuration-file).

#### Add config options for administrator JVM options

##### [FLINK-33221](https://issues.apache.org/jira/browse/FLINK-33221)

A set of administrator JVM options are available, which prepend the user-set extra JVM options for
platform-wide JVM tuning.

#### Flink Job stuck in suspend state after losing leadership in HA Mode

##### [FLINK-34007](https://issues.apache.org/jira/browse/FLINK-34007)

Fixed a bug where the leader election wasn't able to pick up leadership again after renewing the
lease token caused a leadership loss. This required `fabric8io:kubernetes-client` to be upgraded
from v6.6.2 to v6.9.0.

#### Support dynamic source parallelism inference for batch jobs

##### [FLINK-33768](https://issues.apache.org/jira/browse/FLINK-33768)

In Flink 1.19, we have supported dynamic source parallelism inference for batch jobs, which allows
source connectors to dynamically infer the parallelism based on the actual amount of data to consume.
This feature is a significant improvement over previous versions, which only assigned a fixed default
parallelism to source vertices.
Source connectors need to implement the inference interface to enable dynamic parallelism inference.
Currently, the FileSource connector has already been developed with this functionality in place.
Additionally, the configuration `execution.batch.adaptive.auto-parallelism.default-source-parallelism`
will be used as the upper bound of source parallelism inference. And now it will be set to 1 by default.
If it is not set, the upper bound of allowed parallelism set via
`execution.batch.adaptive.auto-parallelism.max-parallelism` will be used instead. If that
configuration is also not set, the default parallelism set via `parallelism.default` or
`StreamExecutionEnvironment#setParallelism()` will be used instead.

#### Improve the exponential-delay restart-strategy

##### [FLINK-33735](https://issues.apache.org/jira/browse/FLINK-33735)

Flink 1.19 makes a series of improvements to exponential-delay restart-strategy, including:
optimizing the default values of related options, support for max attempts, and solving the issue of
inaccurate attempts in region failover. After these improvements, Flink 1.19 uses exponential-delay
restart-strategy as the default restart-strategy.

#### Renaming AkkaOptions into RpcOptions

##### [FLINK-32684](https://issues.apache.org/jira/browse/FLINK-32684)

`AkkaOptions` are deprecated and replaced with `RpcOptions`.

#### Add min number of slots configuration to limit total number of slots

##### [FLINK-15959](https://issues.apache.org/jira/browse/FLINK-15959)

Flink now supports defining the minimum resource requirements that the Flink cluster allocates using
the configuration options `slotmanager.min-total-resource.cpu`, `slotmanager.min-total-resource.memory`,
and `slotmanager.number-of-slots.min`. These options are intended to ensure that a certain minimum
level of resources is allocated to initialize specific workers during startup, thereby speeding up
the job startup process. Please note that these configuration options do not have any effect on
standalone clusters, as resource allocation in such clusters is not controlled by Flink.

#### Support adding custom metrics in Recovery Spans

##### [FLINK-33697](https://issues.apache.org/jira/browse/FLINK-33697)

A breaking change has been introduced to the `StateBackend` interface. This is relevant only to users
that are implementing their own custom state backends.
Newly added methods `org.apache.flink.runtime.state.StateBackend#createKeyedStateBackend(KeyedStateBackendParameters<K> parameters)`
and `org.apache.flink.runtime.state.StateBackend#createOperatorStateBackend(OperatorStateBackendParameters parameters)`
have replaced previous versions of the `createKeyedStateBackend` and `createOperatorStateBackend` methods.
The new parameters POJO classes contain as fields all of the arguments that were passed directly to those methods.

#### Unify the Representation of TaskManager Location in REST API and Web UI

##### [FLINK-33146](https://issues.apache.org/jira/browse/FLINK-33146)

Unify the representation of TaskManager location in REST API and Web UI. The `host` field is
deprecated in favor of the newly introduced `endpoint` field that includes both the host and port
information to distinguish multiple TaskManagers on the same host.

#### Supports profiling JobManager/TaskManager with Async-profiler on Flink Web

##### [FLINK-33325](https://issues.apache.org/jira/browse/FLINK-33325)

In Flink 1.19, we support triggering profiling at the JobManager/TaskManager level, allowing users to
create a profiling instance with arbitrary intervals and event modes (supported by [async-profiler](https://github.com/async-profiler/async-profiler)).
Users can easily submit profiles and export results in the Flink Web UI.

For example,
- First, users should identify the candidate TaskManager/JobManager with performance bottleneck for profiling and switch to the corresponding TaskManager/JobManager page (profiler tab).
- The user simply clicks on the `Create Profiling Instance` button to submit a profiling instance with specified period and mode. (The description of the profiling mode will be displayed when hovering over the corresponding mode.)
- Once the profiling instance is complete, the user can easily download the interactive HTML file by clicking on the link.

**More Information**
- [Documents](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/debugging/profiler/)
- [FLIP-375: Built-in cross-platform powerful java profiler](https://cwiki.apache.org/confluence/x/64lEE)

### SDK

#### Deprecate RuntimeContext#getExecutionConfig

##### [FLINK-33712](https://issues.apache.org/jira/browse/FLINK-33712)

`RuntimeContext#getExecutionConfig` is now being deprecated in Flink 1.19. And this method is planned
to be removed in Flink 2.0. More details can be found at [FLIP-391](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=278465937).

#### Deprecate RichFunction#open(Configuration parameters)

##### [FLINK-32978](https://issues.apache.org/jira/browse/FLINK-32978)

The `RichFunction#open(Configuration parameters)` method has been deprecated and will be removed in
future versions. Users are encouraged to migrate to the new `RichFunction#open(OpenContext openContext)`
method, which provides a more comprehensive context for initialization.
Here are the key changes and recommendations for migration:
The `open(Configuration parameters)` method is now marked as deprecated. A new method `open(OpenContext openContext)`
has been added as a default method to the `RichFunction` interface. Users should implement the new
`open(OpenContext openContext)` method for function initialization tasks. The new method will be
called automatically before the execution of any processing methods(map, join, etc.). If the new
`open(OpenContext openContext)` method is not implemented, Flink will fall back to invoking the
deprecated `open(Configuration parameters)` method.

#### Deprecate API that uses Flink's Time implementation (related to FLINK-14638)

##### [FLINK-32570](https://issues.apache.org/jira/browse/FLINK-32570)

Flink's Time classes are deprecated now and will be subject to deletion with the release of Flink 2.0.
Please start to use Java's own `Duration` class, instead. Methods supporting the `Duration` class
that replace the deprecated Time-based methods were introduced.

#### Add new interfaces for SinkV2 to synchronize the API with the Source API

##### [FLINK-33973](https://issues.apache.org/jira/browse/FLINK-33973)

According to [FLIP-372](https://cwiki.apache.org/confluence/display/FLINK/FLIP-372%3A+Enhance+and+synchronize+Sink+API+to+match+the+Source+API) the SinkV2 API has been changed.
The following interfaces are deprecated: `TwoPhaseCommittingSink`, `StatefulSink`, `WithPreWriteTopology`, `WithPreCommitTopology`, `WithPostCommitTopology`.
The following new interfaces have been introduced: `CommitterInitContext`, `CommittingSinkWriter`, `WriterInitContext`, `StatefulSinkWriter`.
The following interface method's parameter has been changed: `Sink#createWriter`
The original interfaces will remain available during the 1.19 release line, but they will be removed
in consecutive releases. For the changes required when migrating, please consult the Migration Plan detailed in the FLIP.


#### Deprecate configuration getters/setters that return/set complex Java objects

##### [FLINK-33581](https://issues.apache.org/jira/browse/FLINK-33581)

The non-ConfigOption objects in the `StreamExecutionEnvironment`, `CheckpointConfig`, and 
`ExecutionConfig` and their corresponding getter/setter interfaces are now be deprecated in [FLINK-33581](https://issues.apache.org/jira/browse/FLINK-33581).
And these objects and methods are planned to be removed in Flink 2.0. The deprecated interfaces
include the getter and setter methods of `RestartStrategy`, `CheckpointStorage`, and `StateBackend`.
More details can be found at [FLIP-381](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=278464992).


### Table SQL / API

#### Support Setting Parallelism for Table/SQL Sources

##### [FLINK-33261](https://issues.apache.org/jira/browse/FLINK-33261)

Scan table sources can now be set a custom parallelism for performance tuning via the `scan.parallelism`
option. Currently, only the DataGen connector has been adapted to support that, Kafka connector is
on the way. Please check [scan-table-source](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sourcessinks/#scan-table-source) on how to adapt your custom connectors to it.

#### Adding a separate configuration for specifying Java Options of the SQL Gateway

##### [FLINK-33203](https://issues.apache.org/jira/browse/FLINK-33203)

Flink introduces `env.java.opts.sql-gateway` for specifying the Java options for the SQL Gateway,
which allows you to fine-tune the memory settings, garbage collection behavior, and other relevant Java parameters.

#### Support Configuring Different State TTLs using SQL Hint

##### [FLINK-33397](https://issues.apache.org/jira/browse/FLINK-33397)

This is a new feature in Apache Flink 1.19 that enhances the flexibility and user experience when
managing SQL state time-to-live (TTL) settings. Users can now specify custom TTL values for regular
joins and group aggregations directly within their queries by [utilizing the STATE_TTL hint](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/queries/hints/#state-ttl-hints).
This improvement means that you no longer need to alter your compiled plan to set specific TTLs for
these operators. With the introduction of `STATE_TTL` hints, you can streamline your workflow and
dynamically adjust the TTL based on your operational requirements.

#### MiniBatch Optimization for Regular Joins

##### [FLINK-34219](https://issues.apache.org/jira/browse/FLINK-34219)

Support mini-batch regular join to reduce intermediate result and resolve record amplification in
cascading join scenarios. More details can be found at [minibatch-regular-joins](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/tuning/#minibatch-regular-joins).

#### Support named parameters for functions and procedures

##### [FLINK-34054](https://issues.apache.org/jira/browse/FLINK-34054)

When calling a function or stored procedure now, named parameters can be used. With named parameters,
we do not need to strictly specify the parameter position, just specify the parameter name and its
corresponding value. At the same time, if non-essential parameters are not specified, they will default to being filled with null.

#### Window TVF Aggregation Supports Changelog Inputs

##### [FLINK-20281](https://issues.apache.org/jira/browse/FLINK-20281)

The Window aggregation operator (produced by Window TVF) can consume a changelog stream generated by
nodes such as a CDC connector.

#### Supports SESSION Window TVF in Streaming Mode

##### [FLINK-24024](https://issues.apache.org/jira/browse/FLINK-24024)

Users can use SESSION Window TVF in streaming mode. More details can be found at [session window-tvf](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/queries/window-tvf/#session).

### Connectors

#### Add committer metrics to track the status of committables

##### [FLINK-25857](https://issues.apache.org/jira/browse/FLINK-25857)

The `TwoPhaseCommittingSink#createCommitter` method parameterization has been changed, a new
`CommitterInitContext` parameter has been added.
The original method will remain available during the 1.19 release line, but they will be removed in
consecutive releases.
When migrating please also consider changes introduced by [FLINK-33973](https://issues.apache.org/jira/browse/FLINK-33973) and [FLIP-372]([https://cwiki.apache.org/confluence/display/FLINK/FLIP-372%3A+Enhance+and+synchronize+Sink+API+to+match+the+Source+API).

### FileSystems

#### GCS filesystem does not respect gs.storage.root.url config option

##### [FLINK-33694](https://issues.apache.org/jira/browse/FLINK-33694)

This fix resolves the issue where the `gs.storage.root.url` setting in the Hadoop configuration was
not being acknowledged by the Sink. Warning: If you have been using this property to configure the
GCS Source, please ensure that your tests or pipelines are not adversely affected by the GCS Sink
now also correctly adhering to this configuration.
