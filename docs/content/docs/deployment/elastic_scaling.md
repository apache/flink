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

Flink allows you to adjust your cluster size dynamically to your workloads. This is possible by stopping a job with a savepoint and restarting it with a different parallelism. This page describes options where Flink automatically adjusts the parallelism.

## Reactive Mode

{{< hint danger >}}
Reactive mode is an experimental feature. The Flink community is actively looking for feedback by users through our mailing lists.
{{< /hint >}}

The Reactive Mode allows Flink users to implement a powerful autoscaling mechanism, by having an external service monitor certain metrics, such as consumer lag, aggregate CPU utilization, throughput or latency. As soon as these metrics are above or below a certain threshold, additional TaskManagers can be added or removed from the Flink cluster. This could be implemented through changing the [replica factor](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#replicas) of a Kubernetes deployment, or an [autoscaling](https://docs.aws.amazon.com/autoscaling/ec2/userguide/AutoScalingGroup.html) group.
 
### Getting started

If you just want to try out Reactive Mode, follow these instructions. They assume that you are deploying Flink on one machine.

```bash
# Put Job into lib/ directory
cp ./examples/streaming/TopSpeedWindowing.jar lib/
# Submit Job in Application Mode
./bin/standalone-job.sh start -Drestart-strategy=fixeddelay -Drestart-strategy.fixed-delay.attempts=100000 -Djobmanager.scheduler=adaptive -Dscheduler-mode=reactive -j org.apache.flink.streaming.examples.windowing.TopSpeedWindowing
# Start first TaskManager
./bin/taskmanager.sh start
```

Let's quickly examine the used configuration parameters:
- `-Drestart-strategy=fixeddelay` and `-Drestart-strategy.fixed-delay.attempts=100000` configure the job to restart on failure. This is needed for supporting scale-down.
- `-Djobmanager.scheduler=adaptive` enables the Adaptive Scheduler, which is needed to use Reactive Mode.
- `-Dscheduler-mode=reactive` enables Reactive Mode.

You have now started a Flink job in Application Mode. The [web interface on localhost:8081](http://localhost:8081) now shows that the job is running on one TaskManager. If you want to scale up the job, add another TaskManager to the cluster:
```bash
# Start additional TaskManager
./bin/taskmanager.sh start
```

To scale down, remove a TaskManager instance.
```bash
# Remove a TaskManager
./bin/taskmanager.sh stop
```

### Limitations

Since Reactive Mode is a new, experimental feature, not all features supported by the default scheduler are also available with Reactive Mode (and its adaptive scheduler). The Flink community is working on addressing these limitations.

- **Deployment is only supported as a standalone application** deployment. Active resource managers (such as native Kubernetes, YARN or Mesos) are explicitly not supported. Standalone session clusters are not supported either. The application deployment is limited to single job applications.
- **Streaming jobs only**: The first version of Reactive Mode runs with streaming jobs only. When submitting a batch job, then the default scheduler will be used.
- **No support for local recovery**: Local recovery is a feature that schedules tasks to machines so that the state on that machine gets re-used if possible. The lack of this feature means that Reactive Mode will always need to restore the entire state from the checkpoint storage.
- **No support for local failovers**: Local failover means that the scheduler is able to restart parts ("regions" in Flink's internals) of a failed job, instead of the entire job. This limitations impacts only recovery time of embarrassingly parallel jobs -- Flink's default scheduler can restart failed parts, while Reactive Mode will restart the entire job.
- **Limited integration with Flink's Web UI**: Reactive Mode allows that a job's parallelism can change over its lifetime. The web UI only shows the current parallelism of a job, not the historic evolution of the job. There might be other inconveniences such as a lack of the exception history, incorrect job status time-stamps and incorrect metrics.
- **No support for fine grained resource specifications**: Fine-grained resource specifications are ignored by Reactive Mode.
- **Rescaling causes downtime**: Rescaling of a job happens by restarting the job, thus jobs with large state might need a lot of resources and time to rescale. Rescaling a job causes downtime of your job, but no data loss.

### Usage Notes

#### Parallelism

The **parallelism of individual operators in a job will be determined by the scheduler**. It is not configurable. 

The only way of influencing the parallelism is by setting a max parallelism for a operator (which will be respected by the scheduler). The maxParallelism is bounded by 2^15 (32768), which is the value that Reactive Mode uses if nothing else is configured.
Note that such a high maxParallelism might affect performance of the job, since more internal structures are needed to maintain [some internal structures](https://flink.apache.org/features/2017/07/04/flink-rescalable-state.html) of Flink.



{{< top >}}
