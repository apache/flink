---
title:  "Native Kubernetes"
nav-title: Native Kubernetes
nav-parent_id: resource_providers
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

This page describes how to deploy Flink natively on [Kubernetes](https://kubernetes.io).

* This will be replaced by the TOC
{:toc}

## Getting Started

This *Getting Started* section guides you through setting up a fully functional Flink Cluster on Kubernetes.

### Introduction

Kubernetes is a popular container-orchestration system for automating computer application deployment, scaling, and management.
Flink's native Kubernetes integration allows you to directly deploy Flink on a running Kubernetes cluster.
Moreover, Flink is able to dynamically allocate and de-allocate TaskManagers depending on the required resources because it can directly talk to Kubernetes.

### Preparation

The *Getting Started* section assumes a running Kubernetes cluster fulfilling the following requirements:

- Kubernetes >= 1.9.
- KubeConfig, which has access to list, create, delete pods and services, configurable via `~/.kube/config`. You can verify permissions by running `kubectl auth can-i <list|create|edit|delete> pods`.
- Enabled Kubernetes DNS.
- `default` service account with [RBAC](#rbac) permissions to create, delete pods.

If you have problems setting up a Kubernetes cluster, then take a look at [how to setup a Kubernetes cluster](https://kubernetes.io/docs/setup/).

### Starting a Flink Session on Kubernetes

Once you have your Kubernetes cluster running and `kubectl` is configured to point to it, you can launch a Flink cluster in [Session Mode]({% link deployment/index.md %}#session-mode) via

{% highlight bash %}
# (1) Start Kubernetes session
$ ./bin/kubernetes-session.sh -Dkubernetes.cluster-id=my-first-flink-cluster

# (2) Submit example job
$ ./bin/flink run \
    --target kubernetes-session \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    ./examples/streaming/TopSpeedWindowing.jar

# (3) Stop Kubernetes session by deleting cluster deployment
$ kubectl delete deployment/my-first-flink-cluster

{% endhighlight %}

<span class="label label-info">Note</span> When using [Minikube](https://minikube.sigs.k8s.io/docs/), you need to call `minikube tunnel` in order to [expose Flink's LoadBalancer service on Minikube](https://minikube.sigs.k8s.io/docs/handbook/accessing/#using-minikube-tunnel).

Congratulations! You have successfully run a Flink application by deploying Flink on Kubernetes.

{% top %}

## Deployment Modes

For production use, we recommend deploying Flink Applications in the [Application Mode]({% link deployment/index.md %}#application-mode), as these modes provide a better isolation for the Applications.

### Application Mode

The [Application Mode]({% link deployment/index.md %}#application-mode) requires that the user code is bundled together with the Flink image because it runs the user code's `main()` method on the cluster.
The Application Mode makes sure that all Flink components are properly cleaned up after the termination of the application.

The Flink community provides a [base Docker image]({% link deployment/resource-providers/standalone/docker.md %}#docker-hub-flink-images) which can be used to bundle the user code:

{% highlight dockerfile %}
FROM flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY /path/of/my-flink-job.jar $FLINK_HOME/usrlib/my-flink-job.jar
{% endhighlight %}

After creating and publishing the Docker image under `custom-image-name`, you can start an Application cluster with the following command:

{% highlight bash %}
$ ./bin/flink run-application \
    --target kubernetes-application \
    -Dkubernetes.cluster-id=my-first-application-cluster \
    -Dkubernetes.container.image=custom-image-name \
    local:///opt/flink/usrlib/my-flink-job.jar
{% endhighlight %}

<span class="label label-info">Note</span> `local` is the only supported scheme in Application Mode.

The `kubernetes.cluster-id` option specifies the cluster name and must be unique.
If you do not specify this option, then Flink will generate a random name.

The `kubernetes.container.image` option specifies the image to start the pods with.

Once the application cluster is deployed you can interact with it:

{% highlight bash %}
# List running job on the cluster
$ ./bin/flink list --target kubernetes-application -Dkubernetes.cluster-id=my-first-application-cluster
# Cancel running job
$ ./bin/flink cancel --target kubernetes-application -Dkubernetes.cluster-id=my-first-application-cluster <jobId>
{% endhighlight %}

You can override configurations set in `conf/flink-conf.yaml` by passing key-value pairs `-Dkey=value` to `bin/flink`.

### Per-Job Cluster Mode

Flink on Kubernetes does not support Per-Job Cluster Mode.

### Session Mode

You have seen the deployment of a Session cluster in the [Getting Started](#getting-started) guide at the top of this page.

The Session Mode can be executed in two modes:

* **detached mode** (default): The `kubernetes-session.sh` deploys the Flink cluster on Kubernetes and then terminates.

* **attached mode** (`-Dexecution.attached=true`): The `kubernetes-session.sh` stays alive and allows entering commands to control the running Flink cluster.
  For example, `stop` stops the running Session cluster.
  Type `help` to list all supported commands.

In order to re-attach to a running Session cluster with the cluster id `my-first-flink-cluster` use the following command:

{% highlight bash %}
$ ./bin/kubernetes-session.sh \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    -Dexecution.attached=true
{% endhighlight %}

You can override configurations set in `conf/flink-conf.yaml` by passing key-value pairs `-Dkey=value` to `bin/kubernetes-session.sh`.

#### Stop a Running Session Cluster

In order to stop a running Session Cluster with cluster id `my-first-flink-cluster` you can either [delete the Flink deployment](#manual-resource-cleanup) or use:

{% highlight bash %}
$ echo 'stop' | ./bin/kubernetes-session.sh \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    -Dexecution.attached=true
{% endhighlight %}

{% top %}

## Flink on Kubernetes Reference

### Configuring Flink on Kubernetes

The Kubernetes-specific configuration options are listed on the [configuration page]({% link deployment/config.md %}#kubernetes).

### Accessing Flink's Web UI

Flink's Web UI and REST endpoint can be exposed in several ways via the [kubernetes.rest-service.exposed.type]({% link deployment/config.md %}#kubernetes-rest-service-exposed-type) configuration option.

- **ClusterIP**: Exposes the service on a cluster-internal IP.
  The Service is only reachable within the cluster.
  If you want to access the JobManager UI or submit job to the existing session, you need to start a local proxy.
  You can then use `localhost:8081` to submit a Flink job to the session or view the dashboard.

{% highlight bash %}
$ kubectl port-forward service/<ServiceName> 8081
{% endhighlight %}

- **NodePort**: Exposes the service on each Node’s IP at a static port (the `NodePort`).
  `<NodeIP>:<NodePort>` can be used to contact the JobManager service.
  `NodeIP` can also be replaced with the Kubernetes ApiServer address. 
  You can find its address in your kube config file.

- **LoadBalancer**: Exposes the service externally using a cloud provider’s load balancer.
  Since the cloud provider and Kubernetes needs some time to prepare the load balancer, you may get a `NodePort` JobManager Web Interface in the client log.
  You can use `kubectl get services/<cluster-id>-rest` to get EXTERNAL-IP and construct the load balancer JobManager Web Interface manually `http://<EXTERNAL-IP>:8081`.

Please refer to the official documentation on [publishing services in Kubernetes](https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types) for more information.

### Logging

The Kubernetes integration exposes `conf/log4j-console.properties` and `conf/logback-console.xml` as a ConfigMap to the pods.
Changes to these files will be visible to a newly started cluster.

#### Accessing the Logs

By default, the JobManager and TaskManager will output the logs to the console and `/opt/flink/log` in each pod simultaneously.
The `STDOUT` and `STDERR` output will only be redirected to the console.
You can access them via

{% highlight bash %}
$ kubectl logs <pod-name>
{% endhighlight %}

If the pod is running, you can also use `kubectl exec -it <pod-name> bash` to tunnel in and view the logs or debug the process.

#### Accessing the Logs of the TaskManagers

Flink will automatically de-allocate idling TaskManagers in order to not waste resources.
This behaviour can make it harder to access the logs of the respective pods.
You can increase the time before idling TaskManagers are released by configuring [resourcemanager.taskmanager-timeout]({% link deployment/config.md %}#resourcemanager-taskmanager-timeout) so that you have more time to inspect the log files.

#### Changing the Log Level Dynamically

If you have configured your logger to [detect configuration changes automatically]({% link deployment/advanced/logging.md %}), then you can dynamically adapt the log level by changing the respective ConfigMap (assuming that the cluster id is `my-first-flink-cluster`):

{% highlight bash %}
$ kubectl edit cm flink-config-my-first-flink-cluster
{% endhighlight %}

### Using Plugins

In order to use [plugins]({% link deployment/filesystems/plugins.md %}), you must copy them to the correct location in the Flink JobManager/TaskManager pod.
You can use the [built-in plugins]({% link deployment/resource-providers/standalone/docker.md %}#using-plugins) without mounting a volume or building a custom Docker image.
For example, use the following command to enable the S3 plugin for your Flink session cluster.

{% highlight bash %}
$ ./bin/kubernetes-session.sh
    -Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-{{site.version}}.jar \
    -Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-{{site.version}}.jar
{% endhighlight %}

### Custom Docker Image

If you want to use a custom Docker image, then you can specify it via the configuration option `kubernetes.container.image`.
The Flink community provides a rich [Flink Docker image]({% link deployment/resource-providers/standalone/docker.md %}) which can be a good starting point.
See [how to customize Flink's Docker image]({% link deployment/resource-providers/standalone/docker.md %}#customize-flink-image) for how to enable plugins, add dependencies and other options.

### Using Secrets

[Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/) is an object that contains a small amount of sensitive data such as a password, a token, or a key.
Such information might otherwise be put in a pod specification or in an image.
Flink on Kubernetes can use Secrets in two ways:

* Using Secrets as files from a pod;

* Using Secrets as environment variables;

#### Using Secrets as Files From a Pod

The following command will mount the secret `mysecret` under the path `/path/to/secret` in the started pods:

{% highlight bash %}
$ ./bin/kubernetes-session.sh -Dkubernetes.secrets=mysecret:/path/to/secret
{% endhighlight %}

The username and password of the secret `mysecret` can then be found stored in the files `/path/to/secret/username` and `/path/to/secret/password`.
For more details see the [official Kubernetes documentation](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod).

#### Using Secrets as Environment Variables

The following command will expose the secret `mysecret` as environment variable in the started pods:

{% highlight bash %}
$ ./bin/kubernetes-session.sh -Dkubernetes.env.secretKeyRef=\
    env:SECRET_USERNAME,secret:mysecret,key:username;\
    env:SECRET_PASSWORD,secret:mysecret,key:password
{% endhighlight %}

The env variable `SECRET_USERNAME` contains the username and the env variable `SECRET_PASSWORD` contains the password of the secret `mysecret`.
For more details see the [official Kubernetes documentation](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-environment-variables).

### High-Availability on Kubernetes

For high availability on Kubernetes, you can use the [existing high availability services]({% link deployment/ha/index.md %}).

### Manual Resource Cleanup

Flink uses [Kubernetes OwnerReference's](https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/) to clean up all cluster components.
All the Flink created resources, including `ConfigMap`, `Service`, and `Pod`, have the `OwnerReference` being set to `deployment/<cluster-id>`.
When the deployment is deleted, all related resources will be deleted automatically.

{% highlight bash %}
$ kubectl delete deployment/<cluster-id>
{% endhighlight %}

### Supported Kubernetes Versions

Currently, all Kubernetes versions `>= 1.9` are supported.

### Namespaces

[Namespaces in Kubernetes](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/) divide cluster resources between multiple users via [resource quotas](https://kubernetes.io/docs/concepts/policy/resource-quotas/).
Flink on Kubernetes can use namespaces to launch Flink clusters.
The namespace can be configured via [kubernetes.namespace]({% link deployment/config.md %}#kubernetes-namespace).

### RBAC

Role-based access control ([RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)) is a method of regulating access to compute or network resources based on the roles of individual users within an enterprise.
Users can configure RBAC roles and service accounts used by JobManager to access the Kubernetes API server within the Kubernetes cluster.

Every namespace has a default service account. However, the `default` service account may not have the permission to create or delete pods within the Kubernetes cluster.
Users may need to update the permission of the `default` service account or specify another service account that has the right role bound.

{% highlight bash %}
$ kubectl create clusterrolebinding flink-role-binding-default --clusterrole=edit --serviceaccount=default:default
{% endhighlight %}

If you do not want to use the `default` service account, use the following command to create a new `flink-service-account` service account and set the role binding.
Then use the config option `-Dkubernetes.service-account=flink-service-account` to make the JobManager pod use the `flink-service-account` service account to create/delete TaskManager pods and leader ConfigMaps. 
Also this will allow the TaskManager to watch leader ConfigMaps to retrieve the address of JobManager and ResourceManager.

{% highlight bash %}
$ kubectl create serviceaccount flink-service-account
$ kubectl create clusterrolebinding flink-role-binding-flink --clusterrole=edit --serviceaccount=default:flink-service-account
{% endhighlight %}

Please refer to the official Kubernetes documentation on [RBAC Authorization](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) for more information.

{% top %}
