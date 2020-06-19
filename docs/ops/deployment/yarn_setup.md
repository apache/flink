---
title:  "YARN Setup"
nav-title: YARN
nav-parent_id: deployment
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

## Quickstart

### Start a long-running Flink cluster on YARN

Start a YARN session where the job manager gets 1 GB of heap space and the task managers 4 GB of heap space assigned:

{% highlight bash %}
# If HADOOP_CLASSPATH is not set:
#   export HADOOP_CLASSPATH=`hadoop classpath`
./bin/yarn-session.sh -jm 1024m -tm 4096m
{% endhighlight %}

Specify the `-s` flag for the number of processing slots per Task Manager. We recommend to set the number of slots to the number of processors per machine.

Once the session has been started, you can submit jobs to the cluster using the `./bin/flink` tool.

### Run a Flink job on YARN

{% highlight bash %}
# If HADOOP_CLASSPATH is not set:
#   export HADOOP_CLASSPATH=`hadoop classpath`
./bin/flink run -m yarn-cluster -p 4 -yjm 1024m -ytm 4096m ./examples/batch/WordCount.jar
{% endhighlight %}

## Flink YARN Session

Apache [Hadoop YARN](http://hadoop.apache.org/) is a cluster resource management framework. It allows to run various distributed applications on top of a cluster. Flink runs on YARN next to other applications. Users do not have to setup or install anything if there is already a YARN setup.

**Requirements**

- at least Apache Hadoop 2.4.1
- HDFS (Hadoop Distributed File System) (or another distributed file system supported by Hadoop)

### Start Flink Session

Follow these instructions to learn how to launch a Flink Session within your YARN cluster.

A session will start all required Flink services (JobManager and TaskManagers) so that you can submit programs to the cluster. Note that you can run multiple programs per session.

#### Download Flink

Download a Flink package from the [download page]({{ site.download_url }}). It contains the required files.

Extract the package using:

{% highlight bash %}
tar xvzf flink-{{ site.version }}-bin-scala*.tgz
cd flink-{{site.version }}/
{% endhighlight %}


#### Start a Session

Use the following command to start a session

{% highlight bash %}
./bin/yarn-session.sh
{% endhighlight %}

This command will show you the following overview:

{% highlight bash %}
Usage:
   Optional
     -D <arg>                        Dynamic properties
     -d,--detached                   Start detached
     -jm,--jobManagerMemory <arg>    Memory for JobManager Container with optional unit (default: MB)
     -nm,--name                      Set a custom name for the application on YARN
     -at,--applicationType           Set a custom application type on YARN
     -q,--query                      Display available YARN resources (memory, cores)
     -qu,--queue <arg>               Specify YARN queue.
     -s,--slots <arg>                Number of slots per TaskManager
     -tm,--taskManagerMemory <arg>   Memory per TaskManager Container with optional unit (default: MB)
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths for HA mode
{% endhighlight %}

Please note that the Client requires the `YARN_CONF_DIR` or `HADOOP_CONF_DIR` environment variable to be set to read the YARN and HDFS configuration.

**Example:** Issue the following command to start a Yarn session cluster where each task manager is started with 8 GB of memory and 32 processing slots:

{% highlight bash %}
./bin/yarn-session.sh -tm 8192 -s 32
{% endhighlight %}

The system will use the configuration in `conf/flink-conf.yaml`. Please follow our [configuration guide]({{ site.baseurl }}/ops/config.html) if you want to change something.

Flink on YARN will overwrite the following configuration parameters `jobmanager.rpc.address` (because the JobManager is always allocated at different machines), `io.tmp.dirs` (we are using the tmp directories given by YARN) and `parallelism.default` if the number of slots has been specified.

If you don't want to change the configuration file to set configuration parameters, there is the option to pass dynamic properties via the `-D` flag. So you can pass parameters this way: `-Dfs.overwrite-files=true -Dtaskmanager.memory.network.min=536346624`.

The example invocation starts a single container for the ApplicationMaster which runs the Job Manager.

The session cluster will automatically allocate additional containers which run the Task Managers when jobs are submitted to the cluster. 

Once Flink is deployed in your YARN cluster, it will show you the connection details of the Job Manager.

Stop the YARN session by stopping the unix process (using CTRL+C) or by entering 'stop' into the client.

Flink on YARN will only start if enough resources are available for the ApplicationMaster on the cluster. Most YARN schedulers account for the requested memory of the containers,
some account also for the number of vcores. By default, the number of vcores is equal to the processing slots (`-s`) argument. The [`yarn.containers.vcores`]({{ site.baseurl }}/ops/config.html#yarn-containers-vcores) allows overwriting the
number of vcores with a custom value. In order for this parameter to work you should enable CPU scheduling in your cluster.

#### Detached YARN Session

If you do not want to keep the Flink YARN client running all the time, it's also possible to start a *detached* YARN session.
The parameter for that is called `-d` or `--detached`.

In that case, the Flink YARN client will only submit Flink to the cluster and then close itself.

In order to stop the Flink cluster gracefully use the following command: `echo "stop" | ./bin/yarn-session.sh -id <appId>`.

If this should not be possible, then you can also kill Flink via YARN's web interface or via its utilities: `yarn application -kill <appId>`.
Note, however, that killing Flink might not clean up all job artifacts and temporary files.

#### Attach to an existing Session

Use the following command to start a session

{% highlight bash %}
./bin/yarn-session.sh
{% endhighlight %}

This command will show you the following overview:

{% highlight bash %}
Usage:
   Required
     -id,--applicationId <yarnAppId> YARN application Id
{% endhighlight %}

As already mentioned, `YARN_CONF_DIR` or `HADOOP_CONF_DIR` environment variable must be set to read the YARN and HDFS configuration.

**Example:** Issue the following command to attach to running Flink YARN session `application_1463870264508_0029`:

{% highlight bash %}
./bin/yarn-session.sh -id application_1463870264508_0029
{% endhighlight %}

Attaching to a running session uses YARN ResourceManager to determine Job Manager RPC port.

Stop the YARN session by stopping the unix process (using CTRL+C) or by entering 'stop' into the client.

### Submit Job to Flink

Use the following command to submit a Flink program to the YARN cluster:

{% highlight bash %}
./bin/flink
{% endhighlight %}

Please refer to the documentation of the [command-line client]({{ site.baseurl }}/ops/cli.html).

The command will show you a help menu like this:

{% highlight bash %}
[...]
Action "run" compiles and runs a program.

  Syntax: run [OPTIONS] <jar-file> <arguments>
  "run" action arguments:
     -c,--class <classname>           Class with the program entry point ("main"
                                      method or "getPlan()" method. Only needed
                                      if the JAR file does not specify the class
                                      in its manifest.
     -m,--jobmanager <host:port>      Address of the JobManager to
                                      which to connect. Use this flag to connect
                                      to a different JobManager than the one
                                      specified in the configuration.
     -p,--parallelism <parallelism>   The parallelism with which to run the
                                      program. Optional flag to override the
                                      default value specified in the
                                      configuration
{% endhighlight %}

Use the *run* action to submit a job to YARN. The client is able to determine the address of the JobManager. In the rare event of a problem, you can also pass the JobManager address using the `-m` argument. The JobManager address is visible in the YARN console.

**Example**

{% highlight bash %}
wget -O LICENSE-2.0.txt http://www.apache.org/licenses/LICENSE-2.0.txt
hadoop fs -copyFromLocal LICENSE-2.0.txt hdfs:/// ...
./bin/flink run ./examples/batch/WordCount.jar \
       --input hdfs:///..../LICENSE-2.0.txt --output hdfs:///.../wordcount-result.txt
{% endhighlight %}

If there is the following error, make sure that all TaskManagers started:

{% highlight bash %}
Exception in thread "main" org.apache.flink.compiler.CompilerException:
    Available instances could not be determined from job manager: Connection timed out.
{% endhighlight %}

You can check the number of TaskManagers in the JobManager web interface. The address of this interface is printed in the YARN session console.

If the TaskManagers do not show up after a minute, you should investigate the issue using the log files.

## Run a single Flink job on YARN

The documentation above describes how to start a Flink cluster within a Hadoop YARN environment. It is also possible to launch Flink within YARN only for executing a single job.

***Example:***

{% highlight bash %}
./bin/flink run -m yarn-cluster ./examples/batch/WordCount.jar
{% endhighlight %}

The command line options of the YARN session are also available with the `./bin/flink` tool. They are prefixed with a `y` or `yarn` (for the long argument options).

Note: You can use a different configuration directory per job by setting the environment variable `FLINK_CONF_DIR`. To use this copy the `conf` directory from the Flink distribution and modify, for example, the logging settings on a per-job basis.

Note: It is possible to combine `-m yarn-cluster` with a detached YARN submission (`-yd`) to "fire and forget" a Flink job to the YARN cluster. In this case, your application will not get any accumulator results or exceptions from the ExecutionEnvironment.execute() call!

### User jars & Classpath

By default Flink will include the user jars into the system classpath when running a single job. This behavior can be controlled with the `yarn.per-job-cluster.include-user-jar` parameter.

When setting this to `DISABLED` Flink will include the jar in the user classpath instead.

The user-jars position in the class path can be controlled by setting the parameter to one of the following:

- `ORDER`: (default) Adds the jar to the system class path based on the lexicographic order.
- `FIRST`: Adds the jar to the beginning of the system class path.
- `LAST`: Adds the jar to the end of the system class path.

## Run an application in Application Mode

To launch an application in [Application Mode]({% link ops/deployment/index.md %}#deployment-modes), you can type:

{% highlight bash %}
./bin/flink run-application -t yarn-application ./examples/batch/WordCount.jar
{% endhighlight %}

<div class="alert alert-info" markdown="span">
  <strong>Attention:</strong> Apart from the `-t`, all other configuration parameters, such as the path 
  to the savepoint to be used to bootstrap the application's state, the application parallelism or the 
  required job manager/task manager memory sizes, can be specified by their configuration option, 
  prefixed by `-D`.
</div>
  
As an example, the command to specify the memory sizes of the JM and the TM, looks like:

{% highlight bash %}
./bin/flink run-application -t yarn-application \
-Djobmanager.memory.process.size=2048m \
-Dtaskmanager.memory.process.size=4096m \
./examples/batch/WordCount.jar

{% endhighlight %}

For a look at the available configuration options, you can have a look [here]({% link ops/config.md %}). To unlock
the full potential of the application mode, consider using it with the `yarn.provided.lib.dirs` configuration option
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

Stopping, cancelling or querying the status of a running application can be done in any of the existing ways. 

## Recovery behavior of Flink on YARN

Flink's YARN client has the following configuration parameters to control how to behave in case of container failures. These parameters can be set either from the `conf/flink-conf.yaml` or when starting the YARN session, using `-D` parameters.

- `yarn.application-attempts`: The number of ApplicationMaster (+ its TaskManager containers) attempts. If this value is set to 1 (default), the entire YARN session will fail when the Application master fails. Higher values specify the number of restarts of the ApplicationMaster by YARN.

## Setup for application priority on YARN

Flink's YARN client has the following configuration parameters to setup application priority. These parameters can be set either from the `conf/flink-conf.yaml` or when starting the YARN session, using `-D` parameters.

- `yarn.application.priority`: A non-negative integer indicating the priority for submitting a Flink YARN application. 
It will only take effect if YARN priority scheduling setting is enabled. Larger integer corresponds with higher priority. 
If priority is negative or set to '-1'(default), Flink will unset yarn priority setting and use cluster default priority. 
Please refer to YARN's official documentation for specific settings required to enable priority scheduling for the targeted YARN version.

## Debugging a failed YARN session

There are many reasons why a Flink YARN session deployment can fail. A misconfigured Hadoop setup (HDFS permissions, YARN configuration), version incompatibilities (running Flink with vanilla Hadoop dependencies on Cloudera Hadoop) or other errors.

### Log Files

In cases where the Flink YARN session fails during the deployment itself, users have to rely on the logging capabilities of Hadoop YARN. The most useful feature for that is the [YARN log aggregation](http://hortonworks.com/blog/simplifying-user-logs-management-and-access-in-yarn/).
To enable it, users have to set the `yarn.log-aggregation-enable` property to `true` in the `yarn-site.xml` file.
Once that is enabled, users can use the following command to retrieve all log files of a (failed) YARN session.

{% highlight bash %}
yarn logs -applicationId <application ID>
{% endhighlight %}

Note that it takes a few seconds after the session has finished until the logs show up.

### YARN Client console & Web interfaces

The Flink YARN client also prints error messages in the terminal if errors occur during runtime (for example if a TaskManager stops working after some time).

In addition to that, there is the YARN Resource Manager web interface (by default on port 8088). The port of the Resource Manager web interface is determined by the `yarn.resourcemanager.webapp.address` configuration value.

It allows to access log files for running YARN applications and shows diagnostics for failed apps.

## Build YARN client for a specific Hadoop version

Users using Hadoop distributions from companies like Hortonworks, Cloudera or MapR might have to build Flink against their specific versions of Hadoop (HDFS) and YARN. Please read the [build instructions]({{ site.baseurl }}/flinkDev/building.html) for more details.

## Running Flink on YARN behind Firewalls

Some YARN clusters use firewalls for controlling the network traffic between the cluster and the rest of the network.
In those setups, Flink jobs can only be submitted to a YARN session from within the cluster's network (behind the firewall).
If this is not feasible for production use, Flink allows to configure a port range for its REST endpoint, used for the client-cluster communication. With this
range configured, users can also submit jobs to Flink crossing the firewall.

The configuration parameter for specifying the REST endpoint port is the following:

 * `rest.bind-port`

This configuration option accepts single ports (for example: "50010"), ranges ("50000-50025"), or a combination of
both ("50010,50011,50020-50025,50050-50075").

Please make sure that the configuration option `rest.port` has not been specified, because it has precedence over `rest.bind-port` and accepts no ranges.

(Hadoop is using a similar mechanism, there the configuration parameter is called `yarn.app.mapreduce.am.job.client.port-range`.)

## Background / Internals

This section briefly describes how Flink and YARN interact.

<img src="{{ site.baseurl }}/fig/FlinkOnYarn.svg" class="img-responsive">

The YARN client needs to access the Hadoop configuration to connect to the YARN resource manager and HDFS. It determines the Hadoop configuration using the following strategy:

* Test if `YARN_CONF_DIR`, `HADOOP_CONF_DIR` or `HADOOP_CONF_PATH` are set (in that order). If one of these variables is set, it is used to read the configuration.
* If the above strategy fails (this should not be the case in a correct YARN setup), the client is using the `HADOOP_HOME` environment variable. If it is set, the client tries to access `$HADOOP_HOME/etc/hadoop` (Hadoop 2) and `$HADOOP_HOME/conf` (Hadoop 1).

When starting a new Flink YARN session, the client first checks if the requested resources (memory and vcores for the ApplicationMaster) are available. After that, it uploads a jar that contains Flink and the configuration to HDFS (step 1).

The next step of the client is to request (step 2) a YARN container to start the *ApplicationMaster* (step 3). Since the client registered the configuration and jar-file as a resource for the container, the NodeManager of YARN running on that particular machine will take care of preparing the container (e.g. downloading the files). Once that has finished, the *ApplicationMaster* (AM) is started.

The *JobManager* and AM are running in the same container. Once they successfully started, the AM knows the address of the JobManager (its own host). It is generating a new Flink configuration file for the TaskManagers (so that they can connect to the JobManager). The file is also uploaded to HDFS. Additionally, the *AM* container is also serving Flink's web interface. All ports the YARN code is allocating are *ephemeral ports*. This allows users to execute multiple Flink YARN sessions in parallel.

After that, the AM starts allocating the containers for Flink's TaskManagers, which will download the jar file and the modified configuration from the HDFS. Once these steps are completed, Flink is set up and ready to accept Jobs.

{% top %}
