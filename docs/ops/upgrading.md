---
title: "Upgrading Jobs and Flink Versions"
nav-parent_id: setup
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

## Upgrading Flink Streaming Applications

  - Savepoint, stop/cancel, start from savepoint
  - Atomic Savepoint and Stop (link to JIRA issue)

  - Limitations: Breaking chaining behavior (link to Savepoint section)
  - Encourage using `uid(...)` explicitly for every operator

## Upgrading the Flink Framework Version

This section describes the general way of upgrading Flink framework version from version 1.1.x to 1.2.x and migrating your
jobs between the two versions.

In a nutshell, this procedure consists of 2 fundamental steps:

1. Take a savepoint in Flink 1.1.x for the jobs you want to migrate.
2. Resume your jobs under Flink 1.2.x from the previously taken savepoints.

Besides those two fundamental steps, some additional steps can be required that depend on the way you want to change the
Flink version. In this guide we differentiate two approaches to upgrade from Flink 1.1.x to 1.2.x: **in-place** upgrade and 
**shadow copy** upgrade.

For **in-place** update, after taking savepoints, you need to:

  1. Stop/cancel all running jobs.
  2. Shutdown the cluster that runs Flink 1.1.x.
  3. Upgrade Flink to 1.2.x. on the cluster.
  4. Restart the cluster under the new version.

For **shadow copy**, you need to:

  1. Before resuming from the savepoint, setup a new installation of Flink 1.2.x besides your old Flink 1.1.x installation.
  2. Resume from the savepoints with the new Flink 1.2.x installation.
  3. If everything runs ok, stop and shutdown the old Flink 1.1.x cluster.

In the following, we will first present the preconditions for successful job migration and then go into more detail 
about the steps that we outlined before.

### Preconditions

Before starting the migration, please check that the jobs you are trying to migrate are following the
best practises for [savepoints]({{ site.baseurl }}/setup/savepoints.html). In particular, we advise you to check that 
explicit `uid`s were set for operators in your job. 

This is a *soft* precondition, and restore *should* still work in case you forgot about assigning `uid`s. 
If you run into a case where this is not working, you can *manually* add the generated legacy vertex ids from Flink 1.1 
to your job using the `setUidHash(String hash)` call. For each operator (in operator chains: only the head operator) you 
must assign the 32 character hex string representing the hash that you can see in the web ui or logs for the operator.

Besides operator uids, there are currently three *hard* preconditions for job migration that will make migration fail: 

1. as mentioned in earlier release notes, we do not support migration for state in RocksDB that was checkpointed using 
`semi-asynchronous` mode. In case your old job was using this mode, you can still change your job to use 
`fully-asynchronous` mode before taking the savepoint that is used as the basis for the migration.

2. The CEP operator is currently not supported for migration. If your job uses this operator you can (curently) not 
migrate it. We are planning to provide migration support for the CEP operator in a later bugfix release.

3. Another **important** precondition is that all the savepoint data must be accessible from the new installation and 
reside under the same absolute path. Please notice that the savepoint data is typically not self contained in just the created 
savepoint file. Additional files can be referenced from inside the savepoint file (e.g. the output from state backend 
snapshots)! There is currently no simple way to identify and move all data that belongs to a savepoint.


### STEP 1: Taking a savepoint in Flink 1.1.x.

First major step in job migration is taking a savepoint of your job running in Flink 1.1.x. You can do this with the
command:

```sh
$ bin/flink savepoint :jobId [:targetDirectory]
```

For more details, please read the [savepoint documentation]({{ site.baseurl }}/setup/savepoints.html).

### STEP 2: Updating your cluster to Flink 1.2.x.

In this step, we update the framework version of the cluster. What this basically means is replacing the content of
the Flink installation with the new version. This step can depend on how you are running Flink in your cluster (e.g. 
standalone, on Mesos, ...).

If you are unfamiliar with installing Flink in your cluster, please read the [deployment and cluster setup documentation]({{ site.baseurl }}/setup/index.html).

### STEP 3: Resuming the job under Flink 1.2.x from Flink 1.1.x savepoint.

As the last step of job migration, you resume from the savepoint taken above on the updated cluster. You can do
this with the command:

```sh
$ bin/flink run -s :savepointPath [:runArgs]
```

Again, for more details, please take a look at the [savepoint documentation]({{ site.baseurl }}/setup/savepoints.html).

## Compatibility Table

Savepoints are compatible across Flink versions as indicated by the table below:
                             
| Created with \ Resumed with | 1.1.x | 1.2.x |
| ---------------------------:|:-----:|:-----:|
| 1.1.x                       |   X   |   X   |
| 1.2.x                       |       |   X   |



## Limitations and Special Considerations for Upgrades from Flink 1.1.x to Flink 1.2.x
  
  - The maximum parallelism of a job that was migrated from Flink 1.1.x to 1.2.x is currently fixed as the parallelism of 
  the job. This means that the parallelism can not be increased after migration. This limitation might be removed in a 
  future bugfix release.


