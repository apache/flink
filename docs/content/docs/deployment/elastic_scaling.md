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

Apache Flink allows you to rescale your jobs. You can do this manually by stopping the job and restarting from the savepoint created during shutdown with a different parallelism.

This page describes options where Flink automatically adjusts the parallelism instead.

## Reactive Mode

{{< hint danger >}}
Reactive mode is a MVP ("minimum viable product") feature. The Flink community is actively looking for feedback by users through our mailing lists. Please check the limitations listed on this page.
{{< /hint >}}

Reactive Mode configures a job so that it always uses all resources available in the cluster. Adding a TaskManager will scale up your job, removing resources will scale it down. Flink will manage the parallelism of the job, always setting it to the highest possible values.

Reactive Mode restarts a job on a rescaling event, restoring it from the latest completed checkpoint. This means that there is no overhead of creating a savepoint (which is needed for manually rescaling a job). Also, the amount of data that is reprocessed after rescaling depends on the checkpointing interval, and the restore time depends on the state size. 

The Reactive Mode allows Flink users to implement a powerful autoscaling mechanism, by having an external service monitor certain metrics, such as consumer lag, aggregate CPU utilization, throughput or latency. As soon as these metrics are above or below a certain threshold, additional TaskManagers can be added or removed from the Flink cluster. This could be implemented through changing the [replica factor](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#replicas) of a Kubernetes deployment, or an [autoscaling](https://docs.aws.amazon.com/autoscaling/ec2/userguide/AutoScalingGroup.html) group. This external service only needs to handle the resource allocation and deallocation. Flink will take care of keeping the job running with the resources available.
 
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

The **parallelism of individual operators in a job will be determined by the scheduler**. It is not configurable.

The only way of influencing the parallelism is by setting a max parallelism for a operator (which will be respected by the scheduler). The maxParallelism is bounded by 2^15 (32768), which is the value that Reactive Mode uses if nothing else is configured. If there is no maxParallelism defined for an operator, `32768` will be used as a default.
If you manually set a parallelism in your job for individual operators or the entire job, this setting will be ignored.

Note that such a high maxParallelism might affect performance of the job, since more internal structures are needed to maintain [some internal structures](https://flink.apache.org/features/2017/07/04/flink-rescalable-state.html) of Flink.

#### Recommendations

- **Configure periodic checkpointing for stateful jobs**: Reactive mode restores from the latest completed checkpoint on a rescale event. If no periodic checkpointing is enabled, your program will loose its state. Checkpointing also configures a **restart strategy**. Reactive mode will respect the configured restarting strategy: If no restarting strategy is configured, reactive mode will fail your job, instead of scaling it.


### Limitations

Since Reactive Mode is a new, experimental feature, not all features supported by the default scheduler are also available with Reactive Mode (and its adaptive scheduler). The Flink community is working on addressing these limitations.

- **Deployment is only supported as a standalone application deployment**. Active resource providers (such as native Kubernetes, YARN or Mesos) are explicitly not supported. Standalone session clusters are not supported either. The application deployment is limited to single job applications. 

  The only supported deployment options are [Standalone in Application Mode]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}#application-mode) ([described](#getting-started) on this page), [Docker in Application Mode]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#application-mode-on-docker) and [Standalone Kubernetes Application Cluster]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}#deploy-application-cluster).
- **Streaming jobs only**: The first version of Reactive Mode runs with streaming jobs only. When submitting a batch job, then the default scheduler will be used.
- **No support for [local recovery]({{< ref "docs/ops/state/large_state_tuning">}}#task-local-recovery)**: Local recovery is a feature that schedules tasks to machines so that the state on that machine gets re-used if possible. The lack of this feature means that Reactive Mode will always need to download the entire state from the checkpoint storage.
- **No support for local failover**: Local failover means that the scheduler is able to restart parts ("regions" in Flink's internals) of a failed job, instead of the entire job. This limitation impacts only recovery time of embarrassingly parallel jobs: Flink's default scheduler can restart failed parts, while Reactive Mode will restart the entire job.
- **Limited integration with Flink's Web UI**: Reactive Mode allows that a job's parallelism can change over its lifetime. The web UI only shows the current parallelism the job.




{{< top >}}
