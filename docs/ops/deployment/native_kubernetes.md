---
title:  "Native Kubernetes Setup"
nav-title: Native Kubernetes
nav-parent_id: deployment
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

This page describes how to deploy a Flink job and session cluster natively on [Kubernetes](https://kubernetes.io).

* This will be replaced by the TOC
{:toc}

<div class="alert alert-warning">
Running Flink natively on kubernetes is an experimental feature. There may be behavioral changes of configuration and cli arguments.
</div>

## Requirements

- Kubernetes 1.9 or above, using [MiniKube](https://kubernetes.io/docs/setup/minikube/) if want to have a taste.
- KubeConfig, which has access to list, create, delete pods and services (can be configured in `~/.kube/config`). You can verify that by `kubectl auth can-i <list|create|edit|delete> pods`.
- Kubernetes DNS enabled.
- Service Account, which has permissions to create, delete pods. Navigate to [RBAC](#rbac) for more information.

## Flink Kubernetes Session

### How it works?

<img src="{{ site.baseurl }}/fig/FlinkOnK8s.svg" class="img-responsive">

1. When creating a Flink Kubernetes session cluster, Flink client will first contact to Kubernetes ApiServer to submit the cluster description, including ConfigMap spec, Job Manager Service spec, Job Manager Deployment spec and Owner Reference.
2. Kubernetes creates the Flink master deployment. The Kubelet will pull the image, prepare and mount the volume and then execute the start command. After Flink master pod launched, the Dispatcher and KubernetesResourceManager have been started. So far, the flink session cluster is ready to accept one or more jobs.
3. Users submit a job through Flink client. The job graph will be generated on Flink client side and then uploaded with user jars through RestClient together. The Kubernetes service is used for Flink client contacting to Flink master.
4. Once the job has been submitted successfully, JobSubmitHandler receives the request and submit job to Dispatcher. Then JobMaster is spawned. 
5. JobMaster requests slots from KubernetesResourceManager.
6. KubernetesResourceManager allocates TaskManager from Kubernetes cluster. Each TaskManager is a pod with unique id so that we could monitor and release a specific one.
7. TaskManager is launched.
8. TaskManager registers at ResourceManager.
9. ResourceManager requests slots from TaskManager.
10. TaskManager offers slots to the JobMaster. Then the tasks will be deployed and running.

### Start a session

Use the following command to start a session

{% highlight bash %}
./bin/kubernetes-session.sh
{% endhighlight %}

Example: Issue the following command to start a session cluster with <4GB Mem, 2CPU> and 4 slots per TaskManager:
{% highlight bash %}
./bin/kubernetes-session.sh \
-Dkubernetes.cluster-id=<ClusterId> \
-Dtaskmanager.memory.process.size=4096m \
-Dkubernetes.taskmanager.cpu=2 \
-Dtaskmanager.numberOfTaskSlots=4
{% endhighlight %}

If you do not specify a certain name for your session by `kubernetes.cluster-id`, Flink client will generate a UUID name. All the Kubernetes config options could be found at [config#Kubernetes]({{ site.baseurl }}/ops/config.html#kubernetes).

### Accessing Job Manager UI

There are several ways to expose a Service onto an external (outside of your cluster) IP address. This could be changed by `kubernetes.service.exposed.type`.

- `ClusterIP`: Exposes the service on a cluster-internal IP. The Service is only reachable within the cluster. If you want to access the Job Manager ui or submit job to the existing session, you need to start a local proxy by the following command. `kubectl port-forward service/<ServiceName> 8081`. Then you could use `localhost:8081` to submit a Flink job to the session or view the dashboard.
- `NodePort`: Exposes the service on each Node’s IP at a static port (the `NodePort`). `<NodeIP>:<NodePort>` could be used to contact the Job Manager Service. `NodeIP` could be easily replaced with Kubernetes ApiServer address. You could find it in your kube config file.
- `LoadBalancer`: Default value, exposes the service externally using a cloud provider’s load balancer. Since the cloud provider and Kubernetes needs some time to prepare the load balancer, you may get a `NodePort` JobManager Web Interface in the client log. You could use `kubectl get services/<ClusterId>` to get EXTERNAL-IP. And then construct the load balancer JobManager Web Interface manually `http://<EXTERNAL-IP>:8081`. 
- `ExternalName`: Map a service to a DNS name, not supported in current version.

Navigate to [publishing services in Kubernetes](https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types) for more information.

### Submitting job to an existing Session

Use the following command to submit job
{% highlight bash %}
bin/flink run -d -e kubernetes-session -Dkubernetes.cluster-id=<ClusterId> examples/streaming/WindowJoin.jar
{% endhighlight %}

The ClusterId is specified by config option `kubernetes.cluster-id` when starting the session.

### Attach to an existing Session
The Kubernetes session is started in detached mode by default. So the Flink client will exit after submitting all the resources to the Kubernetes cluster. Use the following command to attach to an existing session.
{% highlight bash %}
bin/kubernetes-session.sh -Dkubernetes.cluster-id=<ClusterId> -Dexecution.attached=true
{% endhighlight %}

### Stop Kubernetes Session
To stop a Kubernetes session, you could attach to it and then type `stop`.
Also, you could issue the following one command.
{% highlight bash %}
echo 'stop' | ./bin/kubernetes-session.sh -Dkubernetes.cluster-id=<ClusterId> -Dexecution.attached=true
{% endhighlight %}

## Run a single Flink job on Kubernetes
Running a single Flink job natively on Kubernetes could not be supported now.

## Garbage Collection
Flink use [Kubernetes ownerReference](https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/) to cleanup the cluster. All the Flink created resources, including `ConfigMap`,`Service`,`Deployment`,`Pod`, have been set the ownerReference to `service/<ClusterId>`. When the service is deleted, all other resource will be deleted automatically. So if you want to stop the Flink cluster, you could use the Kubernetes utilities(`kubectl delete service/<ClusterID>`). It works for both session and per-job cluster.

## Debug a failed Flink Kubernetes cluster

By default, the jobmanager and taskmanager only store the log under /opt/flink/log in pod. If you want to use `kubectl logs <PodName>` to view the logs, do as the following instructions.
1. Add a new appender to the log4j.properties in the Flink client
2. Update the rootLogger in log4j.properties to `log4j.rootLogger=INFO, file, console`
3. Remove the redirect args by adding config option `-Dkubernetes.container-start-command-template="%java% %classpath% %jvmmem% %jvmopts% %logging% %class% %args%"`
4. Stop and start your session again. Now you could use `kubectl logs` to view your logs.
{% highlight bash %}
# Log all infos to the console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
{% endhighlight %}

If the pod is running, you could use `kubectl exec -it <PodName> bash` to get in. Then you could view the logs or debug the process. 

## Kubernetes concepts

### NameSpaces

[Namespaces in Kubernetes](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/) are a way to divide cluster resources between multiple users (via resource quota). It is similar to queue concept in Yarn cluster. Flink on Kubernetes can use namespaces to launch Flink clusters. The namespace could be specified by `-Dkubernetes.namespace=default` argument when starting a Flink cluster.

[ResourceQuota](https://kubernetes.io/docs/concepts/policy/resource-quotas/) provides constraints that limit aggregate resource consumption per namespace. It can limit the quantity of objects that can be created in a namespace by type, as well as the total amount of compute resources that may be consumed by resources in that project.

### RBAC

Role-based access control ([RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)) is a method of regulating access to compute or network resources based on the roles of individual users within an enterprise. So users can configure RBAC roles and service accounts used by Flink JobManager to access the Kubernetes API server within the Kubernetes cluster. 

Every namespace will have a default service account. However, the `default` service account may not have the permission to create or delete pods within the Kubernetes cluster. So users may need to update the permission of `default` service account or specify another service account that has the right role bound.
Use the following command to give enough permission to `default` service account.
{% highlight bash %}
kubectl create clusterrolebinding flink-role-binding-default --clusterrole=edit --serviceaccount=default:default
{% endhighlight %}

If you do not want to use `default` service account, use the following command to create a new `flink` service account and set the role binding. Then use config option `-Dkubernetes.jobmanager.service-account=flink` to make the JobManager pod using the `flink` service account to create and delete TaskManager pods.

{% highlight bash %}
kubectl create serviceaccount flink
kubectl create clusterrolebinding flink-role-binding-flink --clusterrole=edit --serviceaccount=default:flink
{% endhighlight %}

Navigate to [RBAC Authorization](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) for more information.
{% top %}
