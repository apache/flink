---
title:  "Native Kubernetes Setup"
nav-title: Native Kubernetes
nav-parent_id: deployment
is_beta: true
nav-pos: 7
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

This page describes how to deploy a Flink session cluster natively on [Kubernetes](https://kubernetes.io).

* This will be replaced by the TOC
{:toc}

<div class="alert alert-warning">
Flink's native Kubernetes integration is still experimental. There may be changes in the configuration and CLI flags in latter versions.
</div>

## Requirements

- Kubernetes 1.9 or above.
- KubeConfig, which has access to list, create, delete pods and services, configurable via `~/.kube/config`. You can verify permissions by running `kubectl auth can-i <list|create|edit|delete> pods`.
- Kubernetes DNS enabled.
- A service Account with [RBAC](#rbac) permissions to create, delete pods.

## Flink Kubernetes Session

### Start Flink Session

Follow these instructions to start a Flink Session within your Kubernetes cluster.

A session will start all required Flink services (JobManager and TaskManagers) so that you can submit programs to the cluster.
Note that you can run multiple programs per session.

{% highlight bash %}
$ ./bin/kubernetes-session.sh
{% endhighlight %}

All the Kubernetes configuration options can be found in our [configuration guide]({{ site.baseurl }}/ops/config.html#kubernetes).

**Example**: Issue the following command to start a session cluster with 4 GB of memory and 2 CPUs with 4 slots per TaskManager:

In this example we override the `resourcemanager.taskmanager-timeout` setting to make
the pods with task managers remain for a longer period than the default of 30 seconds.
Although this setting may cause more cloud cost it has the effect that starting new jobs is in some scenarios
faster and during development you have more time to inspect the logfiles of your job.

{% highlight bash %}
$ ./bin/kubernetes-session.sh \
  -Dkubernetes.cluster-id=<ClusterId> \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dresourcemanager.taskmanager-timeout=3600000
{% endhighlight %}

The system will use the configuration in `conf/flink-conf.yaml`.
Please follow our [configuration guide]({{ site.baseurl }}/ops/config.html) if you want to change something.

If you do not specify a particular name for your session by `kubernetes.cluster-id`, the Flink client will generate a UUID name.

### Custom Flink Docker image

If you want to use a custom Docker image to deploy Flink containers, check [the Flink Docker image documentation](docker.html),
[its tags](docker.html#image-tags), [how to customize the Flink Docker image](docker.html#customize-flink-image) and [enable plugins](docker.html#using-plugins).
If you created a custom Docker image you can provide it by setting the [`kubernetes.container.image`](../config.html#kubernetes-container-image) configuration option:

{% highlight bash %}
$ ./bin/kubernetes-session.sh \
  -Dkubernetes.cluster-id=<ClusterId> \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dresourcemanager.taskmanager-timeout=3600000 \
  -Dkubernetes.container.image=<CustomImageName>
{% endhighlight %}

### Submitting jobs to an existing Session

Use the following command to submit a Flink Job to the Kubernetes cluster.

{% highlight bash %}
$ ./bin/flink run -d -t kubernetes-session -Dkubernetes.cluster-id=<ClusterId> examples/streaming/WindowJoin.jar
{% endhighlight %}

### Accessing Job Manager UI

There are several ways to expose a Service onto an external (outside of your cluster) IP address.
This can be configured using [`kubernetes.rest-service.exposed.type`]({% link ops/config.md %}#kubernetes-rest-service-exposed-type).

- `ClusterIP`: Exposes the service on a cluster-internal IP.
The Service is only reachable within the cluster. If you want to access the Job Manager ui or submit job to the existing session, you need to start a local proxy.
You can then use `localhost:8081` to submit a Flink job to the session or view the dashboard.

{% highlight bash %}
$ kubectl port-forward service/<ServiceName> 8081
{% endhighlight %}

- `NodePort`: Exposes the service on each Node’s IP at a static port (the `NodePort`). `<NodeIP>:<NodePort>` could be used to contact the Job Manager Service. `NodeIP` could be easily replaced with Kubernetes ApiServer address.
You could find it in your kube config file.

- `LoadBalancer`: Exposes the service externally using a cloud provider’s load balancer.
Since the cloud provider and Kubernetes needs some time to prepare the load balancer, you may get a `NodePort` JobManager Web Interface in the client log.
You can use `kubectl get services/<ClusterId>` to get EXTERNAL-IP and then construct the load balancer JobManager Web Interface manually `http://<EXTERNAL-IP>:8081`.

  <span class="label label-warning">Warning!</span> Your JobManager (which can run arbitary jar files) might be exposed to the public internet, without authentication.

- `ExternalName`: Map a service to a DNS name, not supported in current version.

Please reference the official documentation on [publishing services in Kubernetes](https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types) for more information.

### Attach to an existing Session

The Kubernetes session is started in detached mode by default, meaning the Flink client will exit after submitting all the resources to the Kubernetes cluster. Use the following command to attach to an existing session.

{% highlight bash %}
$ ./bin/kubernetes-session.sh -Dkubernetes.cluster-id=<ClusterId> -Dexecution.attached=true
{% endhighlight %}

### Stop Flink Session

To stop a Flink Kubernetes session, attach the Flink client to the cluster and type `stop`.

{% highlight bash %}
$ echo 'stop' | ./bin/kubernetes-session.sh -Dkubernetes.cluster-id=<ClusterId> -Dexecution.attached=true
{% endhighlight %}

#### Manual Resource Cleanup

Flink uses [Kubernetes OwnerReference's](https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/) to cleanup all cluster components.
All the Flink created resources, including `ConfigMap`, `Service`, `Pod`, have been set the OwnerReference to `deployment/<ClusterId>`.
When the deployment is deleted, all other resources will be deleted automatically.

{% highlight bash %}
$ kubectl delete deployment/<ClusterID>
{% endhighlight %}

## Log Files

By default, the JobManager and TaskManager only store logs under `/opt/flink/log` in each pod.
If you want to use `kubectl logs <PodName>` to view the logs, you must perform the following:

1. Add a new appender to the log4j.properties in the Flink client.
2. Add the following 'appenderRef' the rootLogger in log4j.properties `rootLogger.appenderRef.console.ref = ConsoleAppender`.
3. Remove the redirect args by adding config option `-Dkubernetes.container-start-command-template="%java% %classpath% %jvmmem% %jvmopts% %logging% %class% %args%"`.
4. Stop and start your session again. Now you could use `kubectl logs` to view your logs.

{% highlight bash %}
# Log all infos to the console
appender.console.name = ConsoleAppender
appender.console.type = CONSOLE
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
{% endhighlight %}

If the pod is running, you can use `kubectl exec -it <PodName> bash` to tunnel in and view the logs or debug the process.

## Flink Kubernetes Application

### Start Flink Application

Application mode allows users to create a single image containing their Job and the Flink runtime, which will automatically create and destroy cluster components as needed. The Flink community provides base docker images [customized](docker.html#customize-flink-image) for any use case.

{% highlight dockerfile %}
FROM flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY /path/of/my-flink-job-*.jar $FLINK_HOME/usrlib/my-flink-job.jar
{% endhighlight %}

Use the following command to start a Flink application.
{% highlight bash %}
$ ./bin/flink run-application -p 8 -t kubernetes-application \
  -Dkubernetes.cluster-id=<ClusterId> \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dkubernetes.container.image=<CustomImageName> \
  local:///opt/flink/usrlib/my-flink-job.jar
{% endhighlight %}

Note: Only "local" is supported as schema for application mode. This assumes that the jar is located in the image, not the Flink client.

Note: All the jars in the "$FLINK_HOME/usrlib" directory in the image will be added to user classpath.

### Stop Flink Application

When an application is stopped, all Flink cluster resources are automatically destroyed.
As always, Jobs may stop when manually canceled or, in the case of bounded Jobs, complete.

{% highlight bash %}
$ ./bin/flink cancel -t kubernetes-application -Dkubernetes.cluster-id=<ClusterID> <JobID>
{% endhighlight %}

## Kubernetes concepts

### Namespaces

[Namespaces in Kubernetes](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/) are a way to divide cluster resources between multiple users (via resource quota).
It is similar to the queue concept in Yarn cluster. Flink on Kubernetes can use namespaces to launch Flink clusters.
The namespace can be specified using the `-Dkubernetes.namespace=default` argument when starting a Flink cluster.

[ResourceQuota](https://kubernetes.io/docs/concepts/policy/resource-quotas/) provides constraints that limit aggregate resource consumption per namespace.
It can limit the quantity of objects that can be created in a namespace by type, as well as the total amount of compute resources that may be consumed by resources in that project.

### RBAC

Role-based access control ([RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)) is a method of regulating access to compute or network resources based on the roles of individual users within an enterprise.
Users can configure RBAC roles and service accounts used by JobManager to access the Kubernetes API server within the Kubernetes cluster. 

Every namespace has a default service account, however, the `default` service account may not have the permission to create or delete pods within the Kubernetes cluster.
Users may need to update the permission of `default` service account or specify another service account that has the right role bound.

{% highlight bash %}
$ kubectl create clusterrolebinding flink-role-binding-default --clusterrole=edit --serviceaccount=default:default
{% endhighlight %}

If you do not want to use `default` service account, use the following command to create a new `flink` service account and set the role binding.
Then use the config option `-Dkubernetes.jobmanager.service-account=flink` to make the JobManager pod using the `flink` service account to create and delete TaskManager pods.

{% highlight bash %}
$ kubectl create serviceaccount flink
$ kubectl create clusterrolebinding flink-role-binding-flink --clusterrole=edit --serviceaccount=default:flink
{% endhighlight %}

Please reference the official Kubernetes documentation on [RBAC Authorization](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) for more information.

## Background / Internals

This section briefly explains how Flink and Kubernetes interact.

<img src="{{ site.baseurl }}/fig/FlinkOnK8s.svg" class="img-responsive">

When creating a Flink Kubernetes session cluster, the Flink client will first connect to the Kubernetes ApiServer to submit the cluster description, including ConfigMap spec, Job Manager Service spec, Job Manager Deployment spec and Owner Reference.
Kubernetes will then create the JobManager deployment, during which time the Kubelet will pull the image, prepare and mount the volume, and then execute the start command.
After the JobManager pod has launched, the Dispatcher and KubernetesResourceManager are available and the cluster is ready to accept one or more jobs.

When users submit jobs through the Flink client, the job graph will be generated by the client and uploaded along with users jars to the Dispatcher.

The JobManager requests resources, known as slots, from the KubernetesResourceManager.
If no slots are available, the resource manager will bring up TaskManager pods and registering them with the cluster.

{% top %}
