---
title: Working Directory
weight: 3
type: docs
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

# Working Directory

Flink supports to configure a working directory ([FLIP-198](https://cwiki.apache.org/confluence/x/ZZiqCw)) for Flink processes (JobManager and TaskManager).
The working directory is used by the processes to store information that can be recovered upon a process restart.
The requirement for this to work is that the process is started with the same identity and has access to the volume on which the working directory is stored.

## Configuring the Working Directory

The working directories for the Flink processes are: 

* JobManager working directory: `<WORKING_DIR_BASE>/jm_<JM_RESOURCE_ID>` 
* TaskManager working directory: `<WORKING_DIR_BASE>/tm_<TM_RESOURCE_ID>`

with `<WORKING_DIR_BASE>` being the working directory base, `<JM_RESOURCE_ID>` being the resource id of the JobManager process and `<TM_RESOURCE_ID>` being the resource id of the TaskManager process.

The `<WORKING_DIR_BASE>` can be configured by `process.working-dir`.
It needs to point to a local directory.
If not explicitly configured, then it defaults to a randomly picked directory from `io.tmp.dirs`.

It is also possible to configure a JobManager and TaskManager specific `<WORKING_DIR_BASE>` via `process.jobmanager.working-dir` and `process.taskmanager.working-dir` respectively.

The JobManager resource id can be configured via `jobmanager.resource-id`.
If not explicitly configured, then it will be a random UUID.

Similarly, the TaskManager resource id can be configured via `taskmanager.resource-id`.
If not explicitly configured, then it will be a random value containing the host and port of the running process. 

## Artifacts Stored in the Working Directory

Flink processes will use the working directory to store the following artifacts:

* Blobs stored by the `BlobServer` and `BlobCache`
* Local state if `state.backend.local-recovery` is enabled
* RocksDB's working directory

## Local Recovery Across Process Restarts

The working directory can be used to enable local recovery across process restarts ([FLIP-201](https://cwiki.apache.org/confluence/x/wJuqCw)).
This means that Flink does not have to recover state information from remote storage. 

In order to use this feature, local recovery has to be enabled via `state.backend.local-recovery`.
Moreover, the TaskManager processes need to get a deterministic resource id assigned via `taskmanager.resource-id`.
Last but not least, a failed TaskManager process needs to be restarted with the same working directory.

```bash
process.working-dir: /path/to/working/dir/base
state.backend.local-recovery: true
taskmanager.resource-id: TaskManager_1 # important: Change for every TaskManager process
```

{{< top >}}
