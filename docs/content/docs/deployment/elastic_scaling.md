---
title: Elastic Scaling
weight: 5
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

# Elastic Scaling

Historically, the parallelism of a job has been static throughout its lifecycle and defined once during its submission. Batch jobs couldn't be rescaled at all, while Streaming jobs could have been stopped with a savepoint and restarted with a different parallelism.

This page describes a new class of schedulers that allow Flink to adjust job's parallelism at runtime, which pushes Flink one step closer to a truly cloud-native stream processor. The new schedulers are [Adaptive Scheduler](#adaptive-scheduler) (streaming) and [Adaptive Batch Scheduler]({{< ref "docs/deployment/adaptive_batch" >}}) (batch).

## Adaptive Scheduler

The Adaptive Scheduler can adjust the parallelism of a job based on available slots. It will automatically reduce the parallelism if not enough slots are available to run the job with the originally configured parallelism; be it due to not enough resources being available at the time of submission, or TaskManager outages during the job execution. If new slots become available the job will be scaled up again, up to the configured parallelism.

In Reactive Mode (see below) the configured parallelism is ignored and treated as if it was set to infinity, letting the job always use as many resources as possible.

One benefit of the Adaptive Scheduler over the default scheduler is that it can handle TaskManager losses gracefully, since it would just scale down in these cases.

{{< img src="/fig/adaptive_scheduler.png" >}}

Adaptive Scheduler builds on top of a feature called [Declarative Resource Management](https://cwiki.apache.org/confluence/display/FLINK/FLIP-138%3A+Declarative+Resource+management). As you can see, instead of asking for the exact number of slots, JobMaster declares its desired resources (for reactive mode the maximum is set to infinity) to the ResourceManager, which then tries to fulfill those resources.

{{< img src="/fig/adaptive_scheduler_rescale.png" >}}

When JobMaster gets more resources during the runtime, it will automatically rescale the job using the latest available savepoint, eliminating the need for an external orchestration.

Starting from **Flink 1.18.x**, you can re-declare the resource requirements of a running job using [Externalized Declarative Resource Management](#externalized-declarative-resource-management), otherwise the Adaptive Scheduler won't be able to handle cases where the job needs to be rescaled due to a change in the input rate, or a change in the performance of the workload. 

### Externalized Declarative Resource Management

{{< hint warning >}}
Externalized Declarative Resource Management is an MVP ("minimum viable product") feature. The Flink community is actively looking for feedback by users through our mailing lists. Please check the limitations listed on this page.
{{< /hint >}}

{{< hint info >}}
You can use Externalized Declarative Resource Management with the [Apache Flink Kubernetes operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.6/docs/custom-resource/autoscaler/#flink-118-and-in-place-scaling-support) for a fully-fledged auto-scaling experience.
{{< /hint >}}

Externalized Declarative Resource Management aims to address two deployment scenarios:
1. Adaptive Scheduler on Session Cluster, where multiple jobs can compete for resources, and you need a finer-grained control over the distribution of resources between jobs.
2. Adaptive Scheduler on Application Cluster in combination with Active Resource Manager (e.g. [Native Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}})), where you rely on Flink to "greedily" spawn new TaskManagers, but you still want to leverage rescaling capabilities as with [Reactive Mode](#reactive-mode).

by introducing a new [REST API endpoint]({{< ref "docs/ops/rest_api" >}}#jobs-jobid-resource-requirements-1), that allows you to re-declare resource requirements of a running job, by setting per-vertex parallelism boundaries.

```
PUT /jobs/<job-id>/resource-requirements
 
REQUEST BODY:
{
    "<first-vertex-id>": {
        "parallelism": {
            "lowerBound": 3,
            "upperBound": 5
        }
    },
    "<second-vertex-id>": {
        "parallelism": {
            "lowerBound": 2,
            "upperBound": 3
        }
    }
}
```

To a certain extent, the above endpoint could be thought about as a "re-scaling endpoint" and it introduces an important building block for building an auto-scaling experience for Flink.

You can manually try this feature out, by navigating the Job overview in the Flink UI and using up-scale/down-scale buttons in the task list.

### Usage

{{< hint info >}}
If you are using Adaptive Scheduler on a [session cluster]({{< ref "docs/deployment/overview" >}}/#session-mode), there are no guarantees regarding the distribution of slots between multiple running jobs in the same session, in case the cluster doesn't have enough resources. The [External Declarative Resource Management](#externalized-declarative-resource-management) can partially mitigate this issue, but it is still recommended to use Adaptive Scheduler on a [application cluster]({{< ref "docs/deployment/overview" >}}/#application-mode).
{{< /hint >}}

The `jobmanager.scheduler` needs to be set to on the cluster level for the adaptive scheduler to be used instead of default scheduler.

```yaml
jobmanager.scheduler: adaptive
```

The behavior of Adaptive Scheduler is configured by [all configuration options prefixed with `jobmanager.adaptive-scheduler`]({{< ref "docs/deployment/config">}}#advanced-scheduling-options) in their name.

### Limitations

- **Streaming jobs only**: The Adaptive Scheduler runs with streaming jobs only. When submitting a batch job, Flink will use the default scheduler of batch jobs, i.e. [Adaptive Batch Scheduler]({{< ref "docs/deployment/adaptive_batch" >}})
- **No support for partial failover**: Partial failover means that the scheduler is able to restart parts ("regions" in Flink's internals) of a failed job, instead of the entire job. This limitation impacts only recovery time of embarrassingly parallel jobs: Flink's default scheduler can restart failed parts, while Adaptive Scheduler will restart the entire job.
- Scaling events trigger job and task restarts, which will increase the number of Task attempts.

## Reactive Mode

Reactive Mode is a special mode for Adaptive Scheduler, that assumes a single job per-cluster (enforced by the [Application Mode]({{< ref "docs/deployment/overview" >}}#application-mode)). Reactive Mode configures a job so that it always uses all resources available in the cluster. Adding a TaskManager will scale up your job, removing resources will scale it down. Flink will manage the parallelism of the job, always setting it to the highest possible values.

Reactive Mode restarts a job on a rescaling event, restoring it from the latest completed checkpoint. This means that there is no overhead of creating a savepoint (which is needed for manually rescaling a job). Also, the amount of data that is reprocessed after rescaling depends on the checkpointing interval, and the restore time depends on the state size.

The Reactive Mode allows Flink users to implement a powerful autoscaling mechanism, by having an external service monitor certain metrics, such as consumer lag, aggregate CPU utilization, throughput or latency. As soon as these metrics are above or below a certain threshold, additional TaskManagers can be added or removed from the Flink cluster. This could be implemented through changing the [replica factor](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#replicas) of a Kubernetes deployment, or an [autoscaling group](https://docs.aws.amazon.com/autoscaling/ec2/userguide/AutoScalingGroup.html) on AWS. This external service only needs to handle the resource allocation and deallocation. Flink will take care of keeping the job running with the resources available.

### Getting started

If you just want to try out Reactive Mode, follow these instructions. They assume that you are deploying Flink on a single machine.

```bash

# these instructions assume you are in the root directory of a Flink distribution.

# Put Job into lib/ directory
cp ./examples/streaming/TopSpeedWindowing.jar lib/
# Submit Job in Reactive Mode
./bin/standalone-job.sh start -Dscheduler-mode=reactive -Dexecution.checkpointing.interval="10s" -j org.apache.flink.streaming.examples.windowing.TopSpeedWindowing
# Start first TaskManager
./bin/taskmanager.sh start
```

Let's quickly examine the used submission command:
- `./bin/standalone-job.sh start` deploys Flink in [Application Mode]({{< ref "docs/deployment/overview" >}}#application-mode)
- `-Dscheduler-mode=reactive` enables Reactive Mode.
- `-Dexecution.checkpointing.interval="10s"` configure checkpointing and restart strategy.
- the last argument is passing the Job's main class name.

You have now started a Flink job in Reactive Mode. The [web interface](http://localhost:8081) shows that the job is running on one TaskManager. If you want to scale up the job, simply add another TaskManager to the cluster:
```bash
# Start additional TaskManager
./bin/taskmanager.sh start
```

To scale down, remove a TaskManager instance.
```bash
# Remove a TaskManager
./bin/taskmanager.sh stop
```

### Usage

#### Configuration

To enable Reactive Mode, you need to configure `scheduler-mode` to `reactive`.

The **parallelism of individual operators in a job will be determined by the scheduler**. It is not configurable
and will be ignored if explicitly set, either on individual operators or the entire job.

The only way of influencing the parallelism is by setting a max parallelism for an operator
(which will be respected by the scheduler). The maxParallelism is bounded by 2^15 (32768).
If you do not set a max parallelism for individual operators or the entire job, the
[default parallelism rules]({{< ref "docs/dev/datastream/execution/parallel" >}}#setting-the-maximum-parallelism) will be applied,
potentially applying lower bounds than the max possible value. As with the default scheduling mode, please take
the [best practices for parallelism]({{< ref "docs/ops/production_ready" >}}#set-an-explicit-max-parallelism) into consideration.

Note that such a high max parallelism might affect performance of the job, since more internal structures are needed to maintain [some internal structures](https://flink.apache.org/features/2017/07/04/flink-rescalable-state.html) of Flink.

When enabling Reactive Mode, the [`jobmanager.adaptive-scheduler.resource-wait-timeout`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-resource-wait-timeout) configuration key will default to `-1`. This means that the JobManager will run forever waiting for sufficient resources.
If you want the JobManager to stop after a certain time without enough TaskManagers to run the job, configure `jobmanager.adaptive-scheduler.resource-wait-timeout`.

With Reactive Mode enabled, the [`jobmanager.adaptive-scheduler.resource-stabilization-timeout`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-resource-stabilization-timeout) configuration key will default to `0`: Flink will start running the job, as soon as there are sufficient resources available.
In scenarios where TaskManagers are not connecting at the same time, but slowly one after another, this behavior leads to a job restart whenever a TaskManager connects. Increase this configuration value if you want to wait for the resources to stabilize before scheduling the job.
Additionally, one can configure [`jobmanager.adaptive-scheduler.min-parallelism-increase`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-min-parallelism-increase): This configuration option specifics the minimum amount of additional, aggregate parallelism increase before triggering a scale-up. For example if you have a job with a source (parallelism=2) and a sink (parallelism=2), the aggregate parallelism is 4. By default, the configuration key is set to 1, so any increase in the aggregate parallelism will trigger a restart.

One can force scaling operations to happen by setting [`jobmanager.adaptive-scheduler.scaling-interval.max`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-scaling-interval-max). It is disabled by default. If set, then when new resources are added to the cluster, a rescale is scheduled after [`jobmanager.adaptive-scheduler.scaling-interval.max`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-scaling-interval-max) even if [`jobmanager.adaptive-scheduler.min-parallelism-increase`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-min-parallelism-increase) is not satisfied.

To avoid too frequent scaling operations, one can configure [`jobmanager.adaptive-scheduler.scaling-interval.min`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-scaling-interval-min) to set the minimum time between 2 scaling operations. The default is 30s.


#### Recommendations

- **Configure periodic checkpointing for stateful jobs**: Reactive mode restores from the latest completed checkpoint on a rescale event. If no periodic checkpointing is enabled, your program will lose its state. Checkpointing also configures a **restart strategy**. Reactive Mode will respect the configured restarting strategy: If no restarting strategy is configured, reactive mode will fail your job, instead of scaling it.

- Downscaling in Reactive Mode might take longer if the TaskManager is not properly shutdown (i.e., if a SIGKILL signal is used instead of a SIGTERM signal). In this case, Flink waits for the heartbeat between JobManager and the stopped TaskManager(s) to time out. You will see that your Flink job is stuck for roughly 50 seconds before redeploying your job with a lower parallelism.

  The default timeout is configured to 50 seconds. Adjust the [`heartbeat.timeout`]({{< ref "docs/deployment/config">}}#heartbeat-timeout) configuration to a lower value, if your infrastructure permits this. Setting a low heartbeat timeout can lead to failures if a TaskManager fails to respond to a heartbeat, for example due to a network congestion or a long garbage collection pause. Note that the [`heartbeat.interval`]({{< ref "docs/deployment/config">}}#heartbeat-interval) always needs to be lower than the timeout.


### Limitations

Since Reactive Mode is a new, experimental feature, not all features supported by the default scheduler are also available with Reactive Mode (and its adaptive scheduler). The Flink community is working on addressing these limitations.

- **Deployment is only supported as a standalone application deployment**. Active resource providers (such as native Kubernetes, YARN) are explicitly not supported. Standalone session clusters are not supported either. The application deployment is limited to single job applications.

  The only supported deployment options are [Standalone in Application Mode]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}#application-mode) ([described](#getting-started) on this page), [Docker in Application Mode]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#application-mode-on-docker) and [Standalone Kubernetes Application Cluster]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}#deploy-application-cluster).

The [limitations of Adaptive Scheduler](#limitations-1) also apply to Reactive Mode.

{{< top >}}
