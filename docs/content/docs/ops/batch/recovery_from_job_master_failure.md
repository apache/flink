---
title: "Recovery job progress from job master failures"
weight: 4
type: docs
aliases:

- /docs/ops/batch/recovery_from_job_master_failure.html
- /docs/ops/batch/recovery_from_job_master_failure

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

# Batch jobs progress recovery from job master failures

## Background

Previously, if the JobMaster fails and is terminated, one of the following two situations will occur:

- If high availability (HA) is disabled, the job will fail.
- If HA is enabled, a JobMaster failover will happen and the job will be restarted. Streaming jobs can resume from the 
  latest successful checkpoints. Batch jobs, however, do not have checkpoints and have to start over from the beginning, 
  losing all previously made progress. This represents a significant regression for long-running batch jobs.

To address this issue, a batch job recovery mechanism is introduced to enable batch jobs to recover as much progress as 
possible after a JobMaster failover, avoiding the need to rerun tasks that have already been finished.

To implement this feature, a JobEventStore component is introduced to record state change events of the JobMaster 
(such as ExecutionGraph, OperatorCoordinator, etc.) to an external filesystem. During the crash and subsequent restart 
of the JobMaster, TaskManagers will retain the intermediate result data produced by the job and attempt to reconnect 
continuously. Once the JobMaster restarts, it will re-establish connections with TaskManagers and recover the job state 
based on the retained intermediate results and the events previously recorded in the JobEventStore, thereby resuming 
the job's execution progress.

## Usage

This section explains how to enable recovery of batch jobs from JobMaster failures, how to tune it, and how to develop 
sources to work with batch jobs progress recovery.

### How to enable batch jobs progress recovery from job master failures

- Enable cluster high availability:

  To enable the recovery of batch jobs from JobMaster failures, it is essential to first ensure that cluster
  high availability (HA) is enabled. Flink supports HA services backed by ZooKeeper or Kubernetes.
  More details of the configuration can be found in the [High Availability]({{< ref "docs/deployment/ha/overview#high-availability" >}}) page.
- Configure [execution.batch.job-recovery.enabled]({{< ref "docs/deployment/config" >}}#execution-batch-job-recovery-enabled): true

Note that currently only [Adaptive Batch Scheduler]({{< ref "docs/deployment/elastic_scaling" >}}#adaptive-batch-scheduler) 
supports this feature. And Flink batch jobs will use this scheduler by default unless another scheduler is explicitly configured.

### Optimization

To enable batch jobs to recover as much progress as possible after a JobMaster failover, and avoid rerunning tasks 
that have already been finished, you can configure the following options for optimization:

- [execution.batch.job-recovery.snapshot.min-pause]({{< ref "docs/deployment/config" >}}#execution-batch-job-recovery-snapshot-min-pause):
  This setting determines the minimum pause time allowed between snapshots for the OperatorCoordinator and ShuffleMaster.
  This parameter could be adjusted based on the expected I/O load of your cluster and the tolerable amount of state regression. 
  Reduce this interval if smaller state regressions are preferred and a higher I/O load is acceptable.
- [execution.batch.job-recovery.previous-worker.recovery.timeout]({{< ref "docs/deployment/config" >}}#execution-batch-job-recovery-previous-worker-recovery-timeout):
  This setting determines the timeout duration allowed for Shuffle workers to reconnect. During the recovery process, Flink 
  requests the retained intermediate result data information from the Shuffle Master. If the timeout is reached, 
  Flink will use all the acquired intermediate result data to recover the state.
- [job-event.store.write-buffer.flush-interval]({{< ref "docs/deployment/config" >}}#job-event-store-write-buffer-flush-interval):
  This setting determines the flush interval for the JobEventStore's write buffers.
- [job-event.store.write-buffer.size]({{< ref "docs/deployment/config" >}}#job-event-store-write-buffer-size): This 
  setting determines the write buffer size in the JobEventStore. When the buffer is full, its contents are flushed to the external
  filesystem.

### Enable batch jobs progress recovery for sources

Currently, only the new source (FLIP-27) supports progress recovery for batch jobs. To achieve this functionality,
the {{< gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/source/SplitEnumerator.java" name="SplitEnumerator" >}}
of the new source (FLIP-27) must be able to take state snapshots in batch processing scenarios (where the checkpointId
is set to -1) and implement the
{{< gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/source/SupportsBatchSnapshot.java" name="SupportsBatchSnapshot" >}}
interface. This allows it to recover to the progress before the job master failure.
Otherwise, to ensure data accuracy, one of the following two situations will occur after a job master failover:
1. If not all tasks of this source are finished, we will reset and re-run all these tasks.
2. If all tasks of this source are finished, no additional action is required, and the job can continue to run.
However, if any of these tasks need to be restarted at some point in the future (for example, due to a 
PartitionNotFound exception), then all subtasks of this source will need to be reset and rerun.

## Limitations

- Only working with the new source (FLIP-27): Since the legacy source has been deprecated, this feature only supports the new source.
- Exclusive to the [Adaptive Batch Scheduler]({{< ref "docs/deployment/elastic_scaling" >}}#adaptive-batch-scheduler): 
  Currently, only the Adaptive Batch Scheduler supports the recovery of batch jobs after a
  JobMaster failover. As a result, the feature inherits all the
  [limitations of the Adaptive Batch Scheduler]({{<ref "docs/deployment/elastic_scaling" >}}#limitations-2).
- Not working when using remote shuffle services.