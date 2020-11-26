---
title: "Release Notes - Flink 1.5"
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

These release notes discuss important aspects, such as configuration, behavior, or dependencies, that changed between Flink 1.4 and Flink 1.5. Please read these notes carefully if you are planning to upgrade your Flink version to 1.5.

### Update Configuration for Reworked Job Deployment

Flink’s reworked cluster and job deployment component improves the integration with resource managers and enables dynamic resource allocation. One result of these changes is, that you no longer have to specify the number of containers when submitting applications to YARN and Mesos. Flink will automatically determine the number of containers from the parallelism of the application.

Although the deployment logic was completely reworked, we aimed to not unnecessarily change the previous behavior to enable a smooth transition. Nonetheless, there are a few options that you should update in your `conf/flink-conf.yaml` or know about. 

* The allocation of TaskManagers with multiple slots is not fully supported yet. Therefore, we recommend to configure TaskManagers with a single slot, i.e., set `taskmanager.numberOfTaskSlots: 1`
* If you observed any problems with the new deployment mode, you can always switch back to the pre-1.5 behavior by configuring `mode: legacy`. 

Please report any problems or possible improvements that you notice to the Flink community, either by posting to a mailing list or by opening a JIRA issue.

*Note*: We plan to remove the legacy mode in the next release. 

### Update Configuration for Reworked Network Stack

The changes on the networking stack for credit-based flow control and improved latency affect the configuration of network buffers. In a nutshell, the networking stack can require more memory to run applications. Hence, you might need to adjust the network configuration of your Flink setup. 

There are two ways to address problems of job submissions that fail due to lack of network buffers.

* Reduce the number of buffers per channel, i.e., `taskmanager.network.memory.buffers-per-channel` or
* Increase the amount of TaskManager memory that is used by the network stack, i.e., increase `taskmanager.network.memory.fraction` and/or `taskmanager.network.memory.max`.

Please consult the section about [network buffer configuration]({% link deployment/config.md %}#configuring-the-network-buffers) in the Flink documentation for details. In case you experience issues with the new credit-based flow control mode, you can disable flow control by setting `taskmanager.network.credit-model: false`. 

*Note*: We plan to remove the old model and this configuration in the next release.

### Hadoop Classpath Discovery

We removed the automatic Hadoop classpath discovery via the Hadoop binary. If you want Flink to pick up the Hadoop classpath you have to export `HADOOP_CLASSPATH`. On cloud environments and most Hadoop distributions you would do 

```
export HADOOP_CLASSPATH=`hadoop classpath`.
```

### Breaking Changes of the REST API

In an effort to harmonize, extend, and improve the REST API, a few handlers and return values were changed.

* The jobs overview handler is now registered under `/jobs/overview` (before `/joboverview`) and returns a list of job details instead of the pre-grouped view of running, finished, cancelled and failed jobs. 
* The REST API to cancel a job was changed.
* The REST API to cancel a job with savepoint was changed. 

Please check the [REST API documentation]({% link ops/rest_api.md %}#available-requests) for details.

### Kafka Producer Flushes on Checkpoint by Default

The Flink Kafka Producer now flushes on checkpoints by default. Prior to version 1.5, the behaviour was disabled by default and users had to explicitly call `setFlushOnCheckpoints(true)` on the producer to enable it.

### Updated Kinesis Dependency

The Kinesis dependencies of Flink’s Kinesis connector have been updated to the following versions.

```
<aws.sdk.version>1.11.319</aws.sdk.version>
<aws.kinesis-kcl.version>1.9.0</aws.kinesis-kcl.version>
<aws.kinesis-kpl.version>0.12.9</aws.kinesis-kcl.version>
```

<!-- Remove once FLINK-10712 has been fixed -->
### Limitations of failover strategies
Flink's non-default failover strategies are still a very experimental feature which come with a set of limitations.
You should only use this feature if you are executing a stateless streaming job.
In any other cases, it is highly recommended to remove the config option `jobmanager.execution.failover-strategy` from your `flink-conf.yaml` or set it to `"full"`.

In order to avoid future problems, this feature has been removed from the documentation until it will be fixed.
See [FLINK-10880](https://issues.apache.org/jira/browse/FLINK-10880) for more details.

{% top %}
