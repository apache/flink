---
title: "Checkpoints vs. Savepoints"
weight: 10
type: docs
aliases:
  - /ops/state/checkpoints_vs_savepoints.html
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

# Checkpoints vs. Savepoints

## Overview

Conceptually, Flink's [savepoints]({{< ref "docs/ops/state/savepoints" >}}) are different from [checkpoints]({{< ref "docs/ops/state/checkpoints" >}}) 
in a way that's analogous to how backups are different from recovery logs in traditional database systems.

The primary purpose of checkpoints is to provide a recovery mechanism in case of unexpected job failures. 
A [checkpoint's lifecycle]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}) is managed by Flink, 
i.e. a checkpoint is created, owned, and released by Flink - without user interaction. 
Because checkpoints are being triggered often, and are relied upon for failure recovery, the two main design goals for the checkpoint implementation are 
i) being as lightweight to create and ii) being as fast to restore from as possible. 
Optimizations towards those goals can exploit certain properties, e.g., that the job code doesn't change between the execution attempts. 

{{< hint info >}}
- Checkpoints are automatically deleted if the application is terminated by the user 
(except if checkpoints are explicitly configured to be retained).
- Checkpoints are stored in state backend-specific (native) data format (may be incremental depending on the specific backend).
{{< /hint >}}

Although [savepoints]({{< ref "docs/ops/state/savepoints" >}}) are created internally with the same mechanisms as
checkpoints, they are conceptually different and can be a bit more expensive to produce and restore from. Their design focuses
more on portability and operational flexibility, especially with respect to changes to the job. 
The use case for savepoints is for planned, manual operations. For example, this could be an update of your Flink version, changing your job graph, and so on.

{{< hint info >}}
- Savepoints are created, owned and deleted solely by the user. 
That means, Flink does not delete savepoints neither after job termination nor after
restore.
- Savepoints are stored in a state backend independent (canonical) format (Note: Since Flink 1.15, savepoints can be also stored in
the backend-specific [native]({{< ref "docs/ops/state/savepoints" >}}#savepoint-format) format which is faster to create
and restore but comes with some limitations.
{{< /hint >}}

### Capabilities and limitations
The following table gives an overview of capabilities and limitations for the various types of savepoints and
checkpoints.
- ✓ - Flink fully support this type of the snapshot
- x - Flink doesn't support this type of the snapshot
- ! - While these operations currently work, Flink doesn't officially guarantee support for them, so there is a certain level of risk associated with them

| Operation                         | Canonical Savepoint | Native Savepoint | Aligned Checkpoint | Unaligned Checkpoint |
|:----------------------------------|:--------------------|:-----------------|:-------------------|:---------------------|
| State backend change              | ✓                   | x                | x                  | x                    |
| State Processor API (writing)     | ✓                   | x                | x                  | x                    |
| State Processor API (reading)     | ✓                   | !                | !                  | x                    |
| Self-contained and relocatable    | ✓                   | ✓                | x                  | x                    |
| Schema evolution                  | ✓                   | !                | !                  | !                    |
| Arbitrary job upgrade             | ✓                   | ✓                | ✓                  | x                    |
| Non-arbitrary job upgrade         | ✓                   | ✓                | ✓                  | x                    |
| Flink minor version upgrade       | ✓                   | ✓                | ✓                  | x                    |
| Flink bug/patch version upgrade   | ✓                   | ✓                | ✓                  | ✓                    |
| Rescaling                         | ✓                   | ✓                | ✓                  | ✓                    |

- [State backend change]({{< ref "docs/ops/state/state_backends" >}})  - configuring a different State Backend than was used when taking the snapshot.
- [State Processor API (writing)]({{< ref "docs/libs/state_processor_api" >}}#writing-new-savepoints) - the ability to create a new snapshot of this type via the State Processor API.
- [State Processor API (reading)]({{< ref "docs/libs/state_processor_api" >}}#reading-state) - the ability to read states from an existing snapshot of this type via the State Processor API.
- Self-contained and relocatable - the one snapshot folder contains everything it needs for recovery
and it doesn't depend on other snapshots which means it can be easily moved to another place if needed.
- [Schema evolution]({{< ref "docs/dev/datastream/fault-tolerance/serialization/schema_evolution" >}}) - the *state* data type can be changed if it uses a serializer that supports schema evolution (e.g., POJOs and Avro types)
- Arbitrary job upgrade - the snapshot can be restored even if the [partitioning types]({{< ref "docs/dev/datastream/operators/overview" >}}#physical-partitioning)(rescale, rebalance, map, etc.)
or in-flight record types for the existing operators have changed.
- Non-arbitrary job upgrade - restoring the snapshot is possible with updated operators if the job graph topology and in-flight record types remain unchanged.
- Flink minor version upgrade - restoring a snapshot taken with an older minor version of Flink (1.x → 1.y).
- Flink bug/patch version upgrade - restoring a snapshot taken with an older patch version of Flink (1.14.x → 1.14.y).
- Rescaling - restoring the snapshot with a different parallelism than was used during the snapshot creation.


{{< top >}}
