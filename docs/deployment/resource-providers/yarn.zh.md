---
title:  "Apache Hadoop YARN"
nav-title: YARN
nav-parent_id: resource_providers
nav-pos: 4
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

* This will be replaced by the TOC
{:toc}

## Getting Started

This *Getting Started* section guides you through setting up a fully functional Flink Cluster on YARN.

### Introduction

[Apache Hadoop YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) is a resource provider popular with many data processing frameworks.
Flink services are submitted to YARN's ResourceManager, which spawns containers on machines managed by YARN NodeManagers. Flink deploys its JobManager and TaskManager instances into such containers.

Flink can dynamically allocate and de-allocate TaskManager resources depending on the number of processing slots required by the job(s) running on the JobManager.

### Preparation

This *Getting Started* section assumes a functional YARN environment, starting from version 2.4.1. YARN environments are provided most conveniently through services such as Amazon EMR, Google Cloud DataProc or products like Cloudera. [Manually setting up a YARN environment locally](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html) or [on a cluster](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html) is not recommended for following through this *Getting Started* tutorial. 


- Make sure your YARN cluster is ready for accepting Flink applications by running `yarn top`. It should show no error messages.
- Download a recent Flink distribution from the [download page]({{ site.download_url }}) and unpack it.
- **Important** Make sure that the `HADOOP_CLASSPATH` environment variable is set up (it can be checked by running `echo $HADOOP_CLASSPATH`). If not, set it up using 

{% highlight bash %}
export HADOOP_CLASSPATH=`hadoop classpath`
{% endhighlight %}


### Starting a Flink Session on YARN

Once you've made sure that the `HADOOP_CLASSPATH` environment variable is set, we can launch a Flink on YARN session, and submit an example job:

{% highlight bash %}

# we assume to be in the root directory of the unzipped Flink distribution

# (0) export HADOOP_CLASSPATH
export HADOOP_CLASSPATH=`hadoop classpath`

# (1) Start YARN Session
./bin/yarn-session.sh --detached

# (2) You can now access the Flink Web Interface through the URL printed in the last lines of the command output, or through the YARN ResourceManager web UI.

# (3) Submit example job
./bin/flink run ./examples/streaming/TopSpeedWindowing.jar

# (4) Stop YARN session (replace the application id based on the output of the yarn-session.sh command)
echo "stop" | ./bin/yarn-session.sh -yid application_XXXXX_XXX
{% endhighlight %}

Congratulations! You have successfully run a Flink application by deploying Flink on YARN.


## Deployment Modes Supported by Flink on YARN

For production use, we recommend deploying Flink Applications in the [Per-job or Application Mode]({% link deployment/index.md %}#deployment-modes), as these modes provide a better isolation for the Applications.

### Application Mode

Application Mode will launch a Flink cluster on YARN, where the main() method of the application jar gets executed on the JobManager in YARN.
The cluster will shut down as soon as the application has finished. You can manually stop the cluster using `yarn application -kill <ApplicationId>` or by cancelling the Flink job.

{% highlight bash %}
./bin/flink run-application -t yarn-application ./examples/streaming/TopSpeedWindowing.jar
{% endhighlight %}

To unlock the full potential of the application mode, consider using it with the `yarn.provided.lib.dirs` configuration option
and pre-upload your application jar to a location accessible by all nodes in your cluster. In this case, the 
command could look like: 

{% highlight bash %}
./bin/flink run-application -t yarn-application \
	-Dyarn.provided.lib.dirs="hdfs://myhdfs/my-remote-flink-dist-dir" \
	hdfs://myhdfs/jars/my-application.jar
{% endhighlight %}

The above will allow the job submission to be extra lightweight as the needed Flink jars and the application jar
are  going to be picked up by the specified remote locations rather than be shipped to the cluster by the 
client.

### Per-Job Cluster Mode

The Per-job Cluster mode will launch a Flink cluster on YARN, then run the provided application jar locally and finally submit the JobGraph to the JobManager on YARN. If you pass the `--detached` argument, the client will stop once the submission is accepted.

The YARN cluster will stop once the job has stopped.

{% highlight bash %}
./bin/flink run -m yarn-cluster --detached ./examples/streaming/TopSpeedWindowing.jar
{% endhighlight %}

With `bin/flink run -m yarn-cluster`, you can run the command line interface in YARN cluster mode, allowing to submit jobs to an ad-hoc YARN cluster. The command line options of the YARN session described below are then also available. They are prefixed with a `y` or `yarn` (for the long argument options).

### Session Mode

We describe deployment with the Session Mode in the [Getting Started](#getting-started) guide at the top of the page.

The Session Mode has two operation modes:
- **attached mode** (default): The yarn-session.sh client submits the Flink clsuter to YARN, but the client keeps running, tracking the state of the cluster. If the cluster fails, the client will show the error. If the client gets terminated, it will signal the cluster to shut down as well.
- **detached mode**: The yarn-session.sh client submits the Flink cluster to YARN, then the client returns. Another invocation of the client, or YARN tools is needed to stop the Flink cluster.

The session mode will create a hidden YARN properties file in `/tmp/.yarn-properties-<username>`, which will be picked up for cluster discovery by the command line interface when submitting a job.

You can re-attach to a YARN session using the following command:

```
./bin/yarn-session.sh -yid application_XXXX_YY
```

Besides passing [configuration]({% link deployment/config.md %}) via the `conf/flink-conf.yaml` file, you can also pass any configuration at submission time to the `./bin/yarn-session.sh` client using `-Dkey=value` arguments.

The YARN session client also has a few "shortcut arguments" for commonly used settings. They can be listed with `./bin/yarn-session.sh -h`.


## Flink on YARN Reference

### Configuring Flink on YARN

The YARN-specific configurations are listed on the [configuration page]({% link deployment/config.md %}#yarn).

The following configuration parameters have no effect in Flink on YARN, as they are overwritten by the framework at runtime:
- `jobmanager.rpc.address` (dynamically set to the address of the JobManager container by Flink on YARN)
- `io.tmp.dirs` (Flink sets the temporary directories defined by YARN)


### Resource Allocation Behavior

A JobManager submitted in YARN session mode will wait for job submissions before starting to request and allocate TaskManagers. The other deployment modes are immediately requesting the required TaskManagers from YARN.

The memory configurations for JobManager and TaskManager processes will be respected by the YARN implementation. The number of reported VCores is by default equal to the number of configured slots per TaskManager. The [yarn.containers.vcores]({% link deployment/config.md %}#yarn-containers-vcores) allows overwriting the number of vcores with a custom value. In order for this parameter to work you should enable CPU scheduling in your YARN cluster.

Failed containers (including the JobManager) are replaced by YARN. The maximum number of JobManager container restarts is configured via [yarn.application-attempts]({% link deployment/config.md %}#yarn-application-attempts) (default 1). The YARN Application will fail once all attempts are exhausted.


### High-Availability on YARN

For enabling HA on YARN, the [yarn.application-attempts]({% link deployment/config.md %}#yarn-application-attempts) configuration need to be greater 1, and a [high availability service needs to be configured]({% link deployment/ha/index.md %}).

### Supported Hadoop versions.

Flink on YARN is compiled against Hadoop 2.4.1, which is also the earliest version we are supporting. 

For providing Flink with the required Hadoop dependencies, we recommend setting the `HADOOP_CLASSPATH` environment variable already introduced in the [Getting Started / Preparation](#preparation) section. 

If that is not possible, the dependencies can also be put into the `lib/` folder of Flink. 

Flink also offers pre-bundled Hadoop fat jars for placing them in the `lib/` folder, on the [Downloads / Additional Components]({{site.download_url}}#additional-components) section of the website. These pre-bundled fat jars are shaded to avoid dependency conflicts with common libraries. The Flink community is not testing the YARN integration against these pre-bundled jars. 


### Running Flink on YARN behind Firewalls

Some YARN clusters use firewalls for controlling the network traffic between the cluster and the rest of the network.
In those setups, Flink jobs can only be submitted to a YARN session from within the cluster's network (behind the firewall).
If this is not feasible for production use, Flink allows to configure a port range for its REST endpoint, used for the client-cluster communication. With this range configured, users can also submit jobs to Flink crossing the firewall.

The configuration parameter for specifying the REST endpoint port is [rest.bind-port]({% link deployment/config.md %}#rest-bind-port). This configuration option accepts single ports (for example: "50010"), ranges ("50000-50025"), or a combination of
both.

Please make sure that the configuration option [rest.port]({% link deployment/config.md %}#rest-port) has not been specified, because it has precedence over `rest.bind-port` and accepts no ranges.


### User jars & Classpath

By default Flink will include the user jars into the system classpath when running a single job. This behavior can be controlled with the [yarn.per-job-cluster.include-user-jar]({% link deployment/config.md %}#yarn-per-job-cluster-include-user-jar) parameter.

When setting this to `DISABLED` Flink will include the jar in the user classpath instead.

The user-jars position in the classpath can be controlled by setting the parameter to one of the following:

- `ORDER`: (default) Adds the jar to the system classpath based on the lexicographic order.
- `FIRST`: Adds the jar to the beginning of the system classpath.
- `LAST`: Adds the jar to the end of the system classpath.


{% top %}
