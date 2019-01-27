---
title: "JobManager Failover"
nav-title: JobManager Failover
nav-parent_id: ha
nav-pos: 2
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

With [JobManager High Availability]({{ site.baseurl}}/ops/ha/jobmanager_high_availability.html), you can recover from JobManager failures and thereby eliminate the *SPOF*. However, when the new leader takes over the job, it first needs to make a full restart.

With the support of JobManager failover, a job can continue running without any interruption during the recovery of JobManager failures. This is especially useful for batch jobs.

* Toc
{:toc}

## Operation log of JobManager

The general idea to support JobManager failover is that for each modification of **ExecutionGraph**, the JobManager will write an operation log. The operation log will be persisted to external storage, so that when a new leader takes over the job, it first replays the operation log to rebuild the **ExecutionGraph**, and then it reconciles with the TaskManagers to make sure the real status of tasks are the same with that in the **ExecutionGraph**.If all the task status in **ExecutionGraph** are consistent with the status reported by TaskManagers, the JobManager will continue running the job without any interruption. Or else, it will fail the tasks whose status are conflict with report of TaskManagers and restart them.

### Configuration

To enable JobManager failover you have to set the **jobmanager.failover.operation-log-store** to *filesystem* and config the **high-availability.storageDir** to the path where you want to store the operation log. The path can be a local path or HDFS path according to the running mode of your cluster.

{% top %}
