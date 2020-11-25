---
title: "Kubernetes HA Services"
nav-title: Kubernetes HA Services
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

## Kubernetes Cluster High Availability
Kubernetes high availability service could support both [standalone Flink on Kubernetes]({% link deployment/resource-providers/standalone/kubernetes.md %}) and [native Kubernetes integration]({% link deployment/resource-providers/native_kubernetes.md %}).

When running Flink JobManager as a Kubernetes deployment, the replica count should be configured to 1 or greater.
* The value `1` means that a new JobManager will be launched to take over leadership if the current one terminates exceptionally.
* The value `N` (greater than 1) means that multiple JobManagers will be launched simultaneously while one is active and others are standby. Starting more than one JobManager will make the recovery faster.

### Configuration
{% highlight yaml %}
kubernetes.cluster-id: <ClusterId>
high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
high-availability.storageDir: hdfs:///flink/recovery
{% endhighlight %}

#### Example: Highly Available Standalone Flink Cluster on Kubernetes
Both session and job/application clusters support using the Kubernetes high availability service. Users just need to add the following Flink config options to [flink-configuration-configmap.yaml]({% link deployment/resource-providers/standalone/kubernetes.md %}#common-cluster-resource-definitions). All other yamls do not need to be updated.

<span class="label label-info">Note</span> The filesystem which corresponds to the scheme of your configured HA storage directory must be available to the runtime. Refer to [custom Flink image]({% link deployment/resource-providers/standalone/docker.md %}#customize-flink-image) and [enable plugins]({% link deployment/resource-providers/standalone/docker.md %}#using-plugins) for more information.

{% highlight yaml %}
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-config
  labels:
    app: flink
data:
  flink-conf.yaml: |+
  ...
    kubernetes.cluster-id: <ClusterId>
    high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
    high-availability.storageDir: hdfs:///flink/recovery
    restart-strategy: fixed-delay
    restart-strategy.fixed-delay.attempts: 10
  ...
{% endhighlight %}

#### Example: Highly Available Native Kubernetes Cluster
Using the following command to start a native Flink application cluster on Kubernetes with high availability configured.
{% highlight bash %}
$ ./bin/flink run-application -p 8 -t kubernetes-application \
  -Dkubernetes.cluster-id=<ClusterId> \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dkubernetes.container.image=<CustomImageName> \
  -Dhigh-availability=org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory \
  -Dhigh-availability.storageDir=s3://flink/flink-ha \
  -Drestart-strategy=fixed-delay -Drestart-strategy.fixed-delay.attempts=10 \
  -Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-{{site.version}}.jar \
  -Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-{{site.version}}.jar \
  local:///opt/flink/examples/streaming/StateMachineExample.jar
{% endhighlight %}

### High Availability Data Clean Up
Currently, when a Flink job reached the terminal state (`FAILED`, `CANCELED`, `FINISHED`), all the HA data, including metadata in Kubernetes ConfigMap and HA state on DFS, will be cleaned up.

So the following command will only shut down the Flink session cluster and leave all the HA related ConfigMaps, state untouched.
{% highlight bash %}
$ echo 'stop' | ./bin/kubernetes-session.sh -Dkubernetes.cluster-id=<ClusterId> -Dexecution.attached=true
{% endhighlight %}

The following commands will cancel the job in application or session cluster and effectively remove all its HA data.
{% highlight bash %}
# Cancel a Flink job in the existing session
$ ./bin/flink cancel -t kubernetes-session -Dkubernetes.cluster-id=<ClusterID> <JobID>
# Cancel a Flink application
$ ./bin/flink cancel -t kubernetes-application -Dkubernetes.cluster-id=<ClusterID> <JobID>
{% endhighlight %}

To keep HA data while restarting the Flink cluster, simply delete the deployment (via `kubectl delete deploy <ClusterID>`). 
All the Flink cluster related resources will be deleted (e.g. JobManager Deployment, TaskManager pods, services, Flink conf ConfigMap). 
HA related ConfigMaps will be retained because they do not set the owner reference. 
When restarting the session / application using `kubernetes-session.sh` or `flink run-application`,  all previously running jobs will be recovered and restarted from the latest successful checkpoint.
