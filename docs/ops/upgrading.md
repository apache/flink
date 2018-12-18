---
title: "Upgrading Applications and Flink Versions"
nav-parent_id: ops
nav-pos: 15
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

* ToC
{:toc}

Flink DataStream programs are typically designed to run for long periods of time such as weeks, months, or even years. As with all long-running services, Flink streaming applications need to be maintained, which includes fixing bugs, implementing improvements, or migrating an application to a Flink cluster of a later version.

This document describes how to update a Flink streaming application and how to migrate a running streaming application to a different Flink cluster.

## Restarting Streaming Applications

The line of action for upgrading a streaming application or migrating an application to a different cluster is based on Flink's [Savepoint]({{ site.baseurl }}/ops/state/savepoints.html) feature. A savepoint is a consistent snapshot of the state of an application at a specific point in time. 

There are two ways of taking a savepoint from a running streaming application.

* Taking a savepoint and continue processing.
{% highlight bash %}
> ./bin/flink savepoint <jobID> [pathToSavepoint]
{% endhighlight %}
It is recommended to periodically take savepoints in order to be able to restart an application from a previous point in time.

* Taking a savepoint and stopping the application as a single action. 
{% highlight bash %}
> ./bin/flink cancel -s [pathToSavepoint] <jobID>
{% endhighlight %}
This means that the application is canceled immediately after the savepoint completed, i.e., no other checkpoints are taken after the savepoint.

Given a savepoint taken from an application, the same or a compatible application (see [Application State Compatibility](#application-state-compatibility) section below) can be started from that savepoint. Starting an application from a savepoint means that the state of its operators is initialized with the operator state persisted in the savepoint. This is done by starting an application using a savepoint.
{% highlight bash %}
> ./bin/flink run -d -s [pathToSavepoint] ~/application.jar
{% endhighlight %}

The operators of the started application are initialized with the operator state of the original application (i.e., the application the savepoint was taken from) at the time when the savepoint was taken. The started application continues processing from exactly this point on. 

**Note**: Even though Flink consistently restores the state of an application, it cannot revert writes to external systems. This can be an issue if you resume from a savepoint that was taken without stopping the application. In this case, the application has probably emitted data after the savepoint was taken. The restarted application might (depending on whether you changed the application logic or not) emit the same data again. The exact effect of this behavior can be very different depending on the `SinkFunction` and storage system. Data that is emitted twice might be OK in case of idempotent writes to a key-value store like Cassandra but problematic in case of appends to a durable log such as Kafka. In any case, you should carefully check and test the behavior of a restarted application.

## Application State Compatibility

When upgrading an application in order to fix a bug or to improve the application, usually the goal is to replace the application logic of the running application while preserving its state. We do this by starting the upgraded application from a savepoint which was taken from the original application. However, this does only work if both applications are *state compatible*, meaning that the operators of upgraded application are able to initialize their state with the state of the operators of original application. 

In this section, we discuss how applications can be modified to remain state compatible.

### Matching Operator State

When an application is restarted from a savepoint, Flink matches the operator state stored in the savepoint to stateful operators of the started application. The matching is done based on operator IDs, which are also stored in the savepoint. Each operator has a default ID that is derived from the operator's position in the application's operator topology. Hence, an unmodified application can always be restarted from one of its own savepoints. However, the default IDs of operators are likely to change if an application is modified. Therefore, modified applications can only be started from a savepoint if the operator IDs have been explicitly specified. Assigning IDs to operators is very simple and done using the `uid(String)` method as follows:

{% highlight scala%}
val mappedEvents: DataStream[(Int, Long)] = events
  .map(new MyStatefulMapFunc()).uid("mapper-1")
{% endhighlight %}

**Note:** Since the operator IDs stored in a savepoint and IDs of operators in the application to start must be equal, it is highly recommended to assign unique IDs to all operators of an application that might be upgraded in the future. This advice applies to all operators, i.e., operators with and without explicitly declared operator state, because some operators have internal state that is not visible to the user. Upgrading an application without assigned operator IDs is significantly more difficult and may only be possible via a low-level workaround using the `setUidHash()` method.

**Important:** As of 1.3.x this also applies to operators that are part of a chain.

By default all state stored in a savepoint must be matched to the operators of a starting application. However, users can explicitly agree to skip (and thereby discard) state that cannot be matched to an operator when starting a application from a savepoint. Stateful operators for which no state is found in the savepoint are initialized with their default state.

### Stateful Operators and User Functions

When upgrading an application, user functions and operators can be freely modified with one restriction. It is not possible to change the data type of the state of an operator. This is important because, state from a savepoint can (currently) not be converted into a different data type before it is loaded into an operator. Hence, changing the data type of operator state when upgrading an application breaks application state consistency and prevents the upgraded application from being restarted from the savepoint. 

Operator state can be either user-defined or internal. 

* **User-defined operator state:** In functions with user-defined operator state the type of the state is explicitly defined by the user. Although it is not possible to change the data type of operator state, a workaround to overcome this limitation can be to define a second state with a different data type and to implement logic to migrate the state from the original state into the new state. This approach requires a good migration strategy and a solid understanding of the behavior of [key-partitioned state]({{ site.baseurl }}/dev/stream/state/state.html).

* **Internal operator state:** Operators such as window or join operators hold internal operator state which is not exposed to the user. For these operators the data type of the internal state depends on the input or output type of the operator. Consequently, changing the respective input or output type breaks application state consistency and prevents an upgrade. The following table lists operators with internal state and shows how the state data type relates to their input and output types. For operators which are applied on a keyed stream, the key type (KEY) is always part of the state data type as well.

| Operator                                            | Data Type of Internal Operator State |
|:----------------------------------------------------|:-------------------------------------|
| ReduceFunction[IOT]                                 | IOT (Input and output type) [, KEY]  |
| FoldFunction[IT, OT]                                | OT (Output type) [, KEY]             |
| WindowFunction[IT, OT, KEY, WINDOW]                 | IT (Input type), KEY                 |
| AllWindowFunction[IT, OT, WINDOW]                   | IT (Input type)                      |
| JoinFunction[IT1, IT2, OT]                          | IT1, IT2 (Type of 1. and 2. input), KEY |
| CoGroupFunction[IT1, IT2, OT]                       | IT1, IT2 (Type of 1. and 2. input), KEY |
| Built-in Aggregations (sum, min, max, minBy, maxBy) | Input Type [, KEY]                   |

### Application Topology

Besides changing the logic of one or more existing operators, applications can be upgraded by changing the topology of the application, i.e., by adding or removing operators, changing the parallelism of an operator, or modifying the operator chaining behavior.

When upgrading an application by changing its topology, a few things need to be considered in order to preserve application state consistency.

* **Adding or removing a stateless operator:** This is no problem unless one of the cases below applies.
* **Adding a stateful operator:** The state of the operator will be initialized with the default state unless it takes over the state of another operator.
* **Removing a stateful operator:** The state of the removed operator is lost unless another operator takes it over. When starting the upgraded application, you have to explicitly agree to discard the state.
* **Changing of input and output types of operators:** When adding a new operator before or behind an operator with internal state, you have to ensure that the input or output type of the stateful operator is not modified to preserve the data type of the internal operator state (see above for details).
* **Changing operator chaining:** Operators can be chained together for improved performance. When restoring from a savepoint taken since 1.3.x it is possible to modify chains while preserving state consistency. It is possible a break the chain such that a stateful operator is moved out of the chain. It is also possible to append or inject a new or existing stateful operator into a chain, or to modify the operator order within a chain. However, when upgrading a savepoint to 1.3.x it is paramount that the topology did not change in regards to chaining. All operators that are part of a chain should be assigned an ID as described in the [Matching Operator State](#matching-operator-state) section above.

## Upgrading the Flink Framework Version

This section describes the general way of upgrading Flink across versions and migrating your jobs between the versions.

In a nutshell, this procedure consists of 2 fundamental steps:

1. Take a savepoint in the previous, old Flink version for the jobs you want to migrate.
2. Resume your jobs under the new Flink version from the previously taken savepoints.

Besides those two fundamental steps, some additional steps can be required that depend on the way you want to change the
Flink version. In this guide we differentiate two approaches to upgrade across Flink versions: **in-place** upgrade and 
**shadow copy** upgrade.

For **in-place** update, after taking savepoints, you need to:

  1. Stop/cancel all running jobs.
  2. Shutdown the cluster that runs the old Flink version.
  3. Upgrade Flink to the newer version on the cluster.
  4. Restart the cluster under the new version.

For **shadow copy**, you need to:

  1. Before resuming from the savepoint, setup a new installation of the new Flink version besides your old Flink installation.
  2. Resume from the savepoints with the new Flink installation.
  3. If everything runs ok, stop and shutdown the old Flink cluster.

In the following, we will first present the preconditions for successful job migration and then go into more detail 
about the steps that we outlined before.

### Preconditions

Before starting the migration, please check that the jobs you are trying to migrate are following the
best practices for [savepoints]({{ site.baseurl }}/ops/state/savepoints.html). Also, check out the 
[API Migration Guides]({{ site.baseurl }}/dev/migration.html) to see if there is any API changes related to migrating
savepoints to newer versions.

In particular, we advise you to check that explicit `uid`s were set for operators in your job. 

This is a *soft* precondition, and restore *should* still work in case you forgot about assigning `uid`s. 
If you run into a case where this is not working, you can *manually* add the generated legacy vertex ids from previous
Flink versions to your job using the `setUidHash(String hash)` call. For each operator (in operator chains: only the
head operator) you must assign the 32 character hex string representing the hash that you can see in the web ui or logs
for the operator.

Besides operator uids, there are currently two *hard* preconditions for job migration that will make migration fail: 

1. We do not support migration for state in RocksDB that was checkpointed using 
`semi-asynchronous` mode. In case your old job was using this mode, you can still change your job to use 
`fully-asynchronous` mode before taking the savepoint that is used as the basis for the migration.

2. Another **important** precondition is that for savepoints taken before Flink 1.3.x, all the savepoint data must be
accessible from the new installation and reside under the same absolute path. Before Flink 1.3.x, the savepoint data is
typically not self-contained in just the created savepoint file. Additional files can be referenced from inside the
savepoint file (e.g. the output from state backend snapshots). Since Flink 1.3.x, this is no longer a limitation;
savepoints can be relocated using typical filesystem operations..


### STEP 1: Take a savepoint in the old Flink version.

First major step in job migration is taking a savepoint of your job running in the older Flink version.
You can do this with the command:

{% highlight shell %}
$ bin/flink savepoint :jobId [:targetDirectory]
{% endhighlight %}

For more details, please read the [savepoint documentation]({{ site.baseurl }}/ops/state/savepoints.html).

### STEP 2: Update your cluster to the new Flink version.

In this step, we update the framework version of the cluster. What this basically means is replacing the content of
the Flink installation with the new version. This step can depend on how you are running Flink in your cluster (e.g. 
standalone, on Mesos, ...).

If you are unfamiliar with installing Flink in your cluster, please read the [deployment and cluster setup documentation]({{ site.baseurl }}/ops/deployment/cluster_setup.html).

### STEP 3: Resume the job under the new Flink version from savepoint.

As the last step of job migration, you resume from the savepoint taken above on the updated cluster. You can do
this with the command:

{% highlight shell %}
$ bin/flink run -s :savepointPath [:runArgs]
{% endhighlight %}

Again, for more details, please take a look at the [savepoint documentation]({{ site.baseurl }}/ops/state/savepoints.html).

## Compatibility Table

Savepoints are compatible across Flink versions as indicated by the table below:

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Created with \ Resumed with</th>
      <th class="text-center">1.1.x</th>
      <th class="text-center">1.2.x</th>
      <th class="text-center">1.3.x</th>
      <th class="text-center">1.4.x</th>
      <th class="text-center">1.5.x</th>
      <th class="text-center">1.6.x</th>
      <th class="text-center">1.7.x</th>
      <th class="text-center">Limitations</th>
    </tr>
  </thead>
  <tbody>
    <tr>
          <td class="text-center"><strong>1.1.x</strong></td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-left">The maximum parallelism of a job that was migrated from Flink 1.1.x to 1.2.x+ is
          currently fixed as the parallelism of the job. This means that the parallelism can not be increased after
          migration. This limitation might be removed in a future bugfix release.</td>
    </tr>
    <tr>
          <td class="text-center"><strong>1.2.x</strong></td>
          <td class="text-center"></td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-left">When migrating from Flink 1.2.x to Flink 1.3.x+, changing parallelism at the same
          time is not supported. Users have to first take a savepoint after migrating to Flink 1.3.x+, and then change
          parallelism. Savepoints created for CEP applications cannot be restored in 1.4.x+.</td>
    </tr>
    <tr>
          <td class="text-center"><strong>1.3.x</strong></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-left">Migrating from Flink 1.3.0 to Flink 1.4.[0,1] will fail if the savepoint contains Scala case classes. Users have to directly migrate to 1.4.2+ instead.</td>
    </tr>
    <tr>
          <td class="text-center"><strong>1.4.x</strong></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-left"></td>
    </tr>
    <tr>
          <td class="text-center"><strong>1.5.x</strong></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-left">There is a known issue with resuming broadcast state created with 1.5.x in versions
          1.6.x up to 1.6.2, and 1.7.0: <a href="https://issues.apache.org/jira/browse/FLINK-11087">FLINK-11087</a>. Users
          upgrading to 1.6.x or 1.7.x series need to directly migrate to minor versions higher than 1.6.2 and 1.7.0,
          respectively.</td>
    </tr>
    <tr>
          <td class="text-center"><strong>1.6.x</strong></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center">O</td>
          <td class="text-center">O</td>
          <td class="text-left"></td>
    </tr>
    <tr>
          <td class="text-center"><strong>1.7.x</strong></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center"></td>
          <td class="text-center">O</td>
          <td class="text-left"></td>
    </tr>
  </tbody>
</table>

{% top %}
