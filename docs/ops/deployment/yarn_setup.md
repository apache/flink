---
title:  "YARN Setup"
nav-title: YARN
nav-parent_id: deployment
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

* This will be replaced by the TOC
{:toc}

## Quickstart

Please refer to the hadoop document to install hadoop cluster. After the Hadoop cluster has installed, you need to configure the environment variables.

{% highlight bash %}
export HADOOP_CONF_DIR=/etc/hadoop/conf/
{% endhighlight %}

On Linux systems, you can add such line to .bashrc file under $HOME directory to make it permanently for all future bash sessions.

### Run a single Flink job on YARN (per-job mode)

{% highlight bash %}
# get the hadoop2 package from the Flink download page at
# {{ site.download_url }}
curl -O <flink_hadoop2_download_url>
tar xvzf flink-{{ site.version }}-bin-hadoop2.tgz
cd flink-{{ site.version }}/
./bin/flink run -m yarn-cluster -yn 4 -yjm 1024 -ytm 4096 ./examples/batch/WordCount.jar
{% endhighlight %}

You can see the job you just submitted on Yarn's Resource Manager WebUI.

<img src="{{ site.baseurl }}/fig/yarn_quickstart_perjob_rm.png" class="img-responsive">

Click 'ApplicationMaster', and you can see flink's dashboard.

<img src="{{ site.baseurl }}/fig/yarn_quickstart_perjob_flink_dashboard.png" class="img-responsive">

### Run Flink jobs on YARN (session mode)
#### Create yarn session

Start a YARN session with 4 Task Managers (each with 4 GB of Heapspace):

{% highlight bash %}
# get the hadoop2 package from the Flink download page at
# {{ site.download_url }}
curl -O <flink_hadoop2_download_url>
tar xvzf flink-{{ site.version }}-bin-hadoop2.tgz
cd flink-{{ site.version }}/
./bin/yarn-session.sh -n 4 -jm 1024 -tm 4096
{% endhighlight %}

Specify the `-s` flag for the number of processing slots per Task Manager. We recommend to set the number of slots to the number of processors per machine.

If you do not want to keep the Flink YARN client running all the time, it’s also possible to start a detached YARN session. The parameter for that is called -d or --detached.

You can see the session you just submitted on Yarn's Resource Manager WebUI.

<img src="{{ site.baseurl }}/fig/yarn_quickstart_session_rm.png" class="img-responsive">

Click 'ApplicationMaster', and you can see flink's dashboard without any job submited.

<img src="{{ site.baseurl }}/fig/yarn_quickstart_session_flink_dashboard.png" class="img-responsive">

#### Submit job to session
Once the session has been started, you can submit jobs to the cluster using the `./bin/flink` tool.

{% highlight bash %}
./bin/flink run ./examples/streaming/WordCount.jar
{% endhighlight %}

Now you can see the flink job you just submited on the dashboard.

<img src="{{ site.baseurl }}/fig/yarn_quickstart_session_flink_dashboard_wordcount.png" class="img-responsive">

If the resources are sufficient, you can continue to submit jobs to this session.

#### Stop session
To stop the session, you can attach to it by entering 'stop' into the client.

{% highlight bash %}
./bin/yarn-session.sh -id application_1543205128210_0016
{% endhighlight %}

Or you can use `yarn application -kill $applicationId` to kill it.

{% highlight bash %}
yarn application -kill application_1543205128210_0016
{% endhighlight %}

## Flink YARN Session

Apache [Hadoop YARN](http://hadoop.apache.org/) is a cluster resource management framework. It allows to run various distributed applications on top of a cluster. Flink runs on YARN next to other applications. Users do not have to setup or install anything if there is already a YARN setup.

**Requirements**

- at least Apache Hadoop 2.2
- HDFS (Hadoop Distributed File System) (or another distributed file system supported by Hadoop)

If you have troubles using the Flink YARN client, have a look in the [FAQ section](http://flink.apache.org/faq.html#yarn-deployment).

### Start Flink Session

Follow these instructions to learn how to launch a Flink Session within your YARN cluster.

A session will start all required Flink services (JobManager and TaskManagers) so that you can submit programs to the cluster. Note that you can run multiple programs per session.

#### Download Flink

Download a Flink package for Hadoop >= 2 from the [download page]({{ site.download_url }}). It contains the required files.

Extract the package using:

{% highlight bash %}
tar xvzf flink-{{ site.version }}-bin-hadoop2.tgz
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
   Required
     -n,--container <arg>   Number of YARN container to allocate (=Number of Task Managers)
   Optional
     -D <arg>                        Dynamic properties
     -d,--detached                   Start detached
     -jm,--jobManagerMemory <arg>    Memory for JobManager Container [in MB]
     -nm,--name                      Set a custom name for the application on YARN
     -q,--query                      Display available YARN resources (memory, cores)
     -qu,--queue <arg>               Specify YARN queue.
     -s,--slots <arg>                Number of slots per TaskManager
     -sl,--sharedLib <path>          Upload a copy of Flink lib beforehand and specify the path
                                     to use public visibility feature of YARN NM localizing resources
     -t,--ship <arg>                 Ship jars, files and directory for cluster (t for transfer),
                                     Use ',' to separate multiple files.
                                     The files could be in local file system or distributed file system.
                                     Use URI schema to specify which file system the jar belongs.
                                     If schema is missing, would try to get the files in local file system.
                                     (eg: -t file:///tmp/dict,hdfs:///$namenode_address/tmp/dependency2.jar)
     -ta,--shipArchives <arg>        Ship archives for cluster (t for transfer),
                                     Use ',' to separate multiple files.
                                     The archives could be in local file system or distributed file system.
                                     Use URI schema to specify which file system the file belongs.
                                     If schema is missing, would try to get the archives in local file system. 
                                     Use '#' after the file path to specify a new name in workdir.
                                     (eg: -ta file:///tmp/a.tar.gz#dict1,hdfs:///$namenode_address/tmp/b.tar.gz)
     -tm,--taskManagerMemory <arg>   Memory per TaskManager Container [in MB]
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths for HA mode
{% endhighlight %}

Please note that the Client requires the `YARN_CONF_DIR` or `HADOOP_CONF_DIR` environment variable to be set to read the YARN and HDFS configuration.

**Example:** Issue the following command to allocate 10 Task Managers, with 8 GB of memory and 32 processing slots each:

{% highlight bash %}
./bin/yarn-session.sh -n 10 -tm 8192 -s 32
{% endhighlight %}

The system will use the configuration in `conf/flink-conf.yaml`. Please follow our [configuration guide]({{ site.baseurl }}/ops/config.html) if you want to change something.

Flink on YARN will overwrite the following configuration parameters `jobmanager.rpc.address` (because the JobManager is always allocated at different machines), `taskmanager.tmp.dirs` (we are using the tmp directories given by YARN) and `parallelism.default` if the number of slots has been specified.

If you don't want to change the configuration file to set configuration parameters, there is the option to pass dynamic properties via the `-D` flag. So you can pass parameters this way: `-Dfs.overwrite-files=true -Dtaskmanager.network.memory.min=536346624`.

The example invocation starts 11 containers (even though only 10 containers were requested), since there is one additional container for the ApplicationMaster and Job Manager.

Once Flink is deployed in your YARN cluster, it will show you the connection details of the Job Manager.

Stop the YARN session by stopping the unix process (using CTRL+C) or by entering 'stop' into the client.

Flink on YARN will only start all requested containers if enough resources are available on the cluster. Most YARN schedulers account for the requested memory of the containers,
some account also for the number of vcores. By default, the number of vcores is equal to the processing slots (`-s`) argument. The `taskmanager.cpu.core` allows overwriting the
number of vcores with a custom value.

#### Detached YARN Session

If you do not want to keep the Flink YARN client running all the time, it's also possible to start a *detached* YARN session.
The parameter for that is called `-d` or `--detached`.

In that case, the Flink YARN client will only submit Flink to the cluster and then close itself.
Note that in this case it's not possible to stop the YARN session using Flink.

Use the YARN utilities (`yarn application -kill <appId>`) or attach to the session to stop the YARN session.

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

### Submit Job to an existing Session

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
     -m,--jobmanager <host:port>      Address of the JobManager (master) to
                                      which to connect. Use this flag to connect
                                      to a different JobManager than the one
                                      specified in the configuration.
     -p,--parallelism <parallelism>   The parallelism with which to run the
                                      program. Optional flag to override the
                                      default value specified in the
                                      configuration
     --files <files>                  Attach custom files for job. Directory
                                      could not be supported. Use ',' to
                                      separate multiple files. The files
                                      could be in local file system or
                                      distributed file system. Use URI
                                      schema to specify which file system
                                      the file belongs. If schema is
                                      missing, would try to get the file in
                                      local file system. Use '#' after the
                                      file path to specify retrieval key in
                                      runtime. (eg: --file
                                      file:///tmp/a.txt#file_key,hdfs:///$na
                                      menode_address/tmp/b.txt)
     --libjars <libraryJars>          Attach custom library jars for job.
                                      Directory could not be supported. Use
                                      ',' to separate multiple jars. The
                                      jars could be in local file system or
                                      distributed file system. Use URI
                                      schema to specify which file system
                                      the jar belongs. If schema is missing,
                                      would try to get the jars in local
                                      file system. (eg: --libjars
                                      file:///tmp/dependency1.jar,hdfs:///
                                      $namenode_address/tmp/dependency2.jar)
{% endhighlight %}

Use the *run* action to submit a job to an existing Session. The client is able to determine the address of the JobManager. In the rare event of a problem, you can also pass the YARN applicationId using the `-yid` argument. The applicationId is visible in the YARN console.

**Example**

{% highlight bash %}
wget -O LICENSE-2.0.txt http://www.apache.org/licenses/LICENSE-2.0.txt
hadoop fs -copyFromLocal LICENSE-2.0.txt hdfs:/// ...
./bin/flink run ./examples/batch/WordCount.jar \
        hdfs:///..../LICENSE-2.0.txt hdfs:///.../wordcount-result.txt
{% endhighlight %}

If there is the following error, make sure that all TaskManagers started:

{% highlight bash %}
Exception in thread "main" org.apache.flink.compiler.CompilerException:
    Available instances could not be determined from job manager: Connection timed out.
{% endhighlight %}

You can check the number of TaskManagers in the JobManager web interface. The address of this interface is printed in the YARN session console.

If the TaskManagers do not show up after a minute, you should investigate the issue using the log files.

## Run a single Flink job on YARN

The documentation above describes how to start a Flink session cluster within a Hadoop YARN environment. It is also possible to launch Flink within YARN only for executing a single job.

***Example:***

{% highlight bash %}
./bin/flink run -m yarn-cluster -yn 2 ./examples/batch/WordCount.jar
{% endhighlight %}

The command line options of the YARN session are also available with the `./bin/flink` tool. They are prefixed with a `y` or `yarn` (for the long argument options).

Note: The client expects the `-yn` value to be set (number of TaskManagers) in attach mode.

Note: You can use a different configuration directory per job by setting the environment variable `FLINK_CONF_DIR`. To use this copy the `conf` directory from the Flink distribution and modify, for example, the logging settings on a per-job basis.

Note: It is possible to combine `-m yarn-cluster` with a detached YARN submission (`-d`) to "fire and forget" a Flink job to the YARN cluster. The `-yn` will not take effect and resource is allocated as demand. Also in this case, your application will not get any accumulator results or exceptions from the ExecutionEnvironment.execute() call!

### User jars & Classpath

By default Flink will include the user jars into the system classpath when running a single job. This behavior can be controlled with the `yarn.per-job-cluster.include-user-jar` parameter.

When setting this to `DISABLED` Flink will include the jar in the user classpath instead.

The user-jars position in the class path can be controlled by setting the parameter to one of the following:

- `ORDER`: (default) Adds the jar to the system class path based on the lexicographic order.
- `FIRST`: Adds the jar to the beginning of the system class path.
- `LAST`: Adds the jar to the end of the system class path.

## Recovery behavior of Flink on YARN

Flink's YARN client has the following configuration parameters to control how to behave in case of container failures. These parameters can be set either from the `conf/flink-conf.yaml` or when starting the YARN session, using `-D` parameters.

- `yarn.reallocate-failed`: This parameter controls whether Flink should reallocate failed TaskManager containers. Default: true
- `yarn.maximum-failed-containers`: The maximum number of failed containers the ApplicationMaster accepts until it fails the YARN session. Default: The number of initially requested TaskManagers (`-n`).
- `yarn.application-attempts`: The number of ApplicationMaster (+ its TaskManager containers) attempts. If this value is set to 1 (default), the entire YARN session will fail when the Application master fails. Higher values specify the number of restarts of the ApplicationMaster by YARN.

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

Users using Hadoop distributions from companies like Hortonworks, Cloudera or MapR might have to build Flink against their specific versions of Hadoop (HDFS) and YARN. Please read the [build instructions]({{ site.baseurl }}/start/building.html) for more details.

## Running Flink on YARN behind Firewalls

Some YARN clusters use firewalls for controlling the network traffic between the cluster and the rest of the network.
In those setups, Flink jobs can only be submitted to a YARN session from within the cluster's network (behind the firewall).
If this is not feasible for production use, Flink allows to configure a port range for all relevant services. With these
ranges configured, users can also submit jobs to Flink crossing the firewall.

Currently, two services are needed to submit a job:

 * The JobManager (ApplicationMaster in YARN)
 * The BlobServer running within the JobManager.

When submitting a job to Flink, the BlobServer will distribute the jars with the user code to all worker nodes (TaskManagers).
The JobManager receives the job itself and triggers the execution.

The two configuration parameters for specifying the ports are the following:

 * `yarn.application-master.port`
 * `blob.server.port`

These two configuration options accept single ports (for example: "50010"), ranges ("50000-50025"), or a combination of
both ("50010,50011,50020-50025,50050-50075").

(Hadoop is using a similar mechanism, there the configuration parameter is called `yarn.app.mapreduce.am.job.client.port-range`.)

## Background / Internals

This section briefly describes how Flink and YARN interact.

<img src="{{ site.baseurl }}/fig/FlinkOnYarn.svg" class="img-responsive">

There are two execution modes to start a Flink application:
* session mode: All required Flink services (JobManager and TaskManagers) will be started at first even though there is no job submitted, and keep running until the flink cluster has stopped, so that you can submit jobs to the cluster whenever you want. This mode can effectively enhance the execution efficiency for short-term jobs.
* per-job mode: Flink services will be started based on a specific job, and will be stopped after the job finished. In this mode, job will use exactly as many resources as needed, and will release them right after the tasks finished, it's better for resource isolation between jobs and resource efficiency of the cluster.

The YARN client needs to access the Hadoop configuration to connect to the YARN resource manager and to HDFS. It determines the Hadoop configuration using the following strategy:

* Test if `YARN_CONF_DIR`, `HADOOP_CONF_DIR` or `HADOOP_CONF_PATH` are set (in that order). If one of these variables are set, they are used to read the configuration.
* If the above strategy fails (this should not be the case in a correct YARN setup), the client is using the `HADOOP_HOME` environment variable. If it is set, the client tries to access `$HADOOP_HOME/etc/hadoop` (Hadoop 2) and `$HADOOP_HOME/conf` (Hadoop 1).

When starting a new Flink application, the client first checks if the requested resources (containers and memory) are available. After that, it uploads required jars (including Flink framework jar and user jars), job graph (only in per-job mode) and the configuration to HDFS as distributed cache of this application (step 1).

The next step of the client is to request (step 2) a YARN container to start the *ApplicationMaster* (step 3). Since the client registered the configuration and jar-file as a resource for the container, the NodeManager of YARN running on that particular machine will take care of preparing the container (e.g. downloading the files). Once that has finished, the *ApplicationMaster* (AM) is started.

The *JobManager* and AM are running in the same container. Once they successfully started, the AM knows the address of the JobManager (its own host). It is generating a new Flink configuration file for the TaskManagers (so that they can connect to the JobManager). The file is also uploaded to HDFS. Additionally, the *AM* container is also serving Flink's web interface. All ports the YARN code is allocating are *ephemeral ports*. This allows users to execute multiple Flink YARN applications in parallel.

After that, the AM starts allocating the containers for Flink's TaskManagers, which will download the required jars and the modified configuration from the HDFS. In session mode, TaskManagers are allocated based on configuration which defines the resource and number of TaskManagers, once these steps are completed, Flink is set up and ready to accept Jobs. In per-job mode, TaskManagers are allocated based on the job graph retrieved from distributed cache, Flink ResourceManager can internally combine several tiny slots with the same resource profile in one container to enhance scheduling efficiency.

## YARN shuffle service
### Run batch jobs with YARN shuffle service

YARN shuffle service provides an external endpoint for serving the intermediate data between tasks. It serves as an alternative to the built-in endpoint embedded in the TaskManager. With YARN shuffle service the map-side TaskManagers do not need to wait till the reduce-side tasks finish reading to shutdown, therefore the maximum resources required can be reduced.

YARN shuffle service acts as a plugin of the NodeManager and you first need to start it on each NodeManager in your YARN cluster:

1. Locate the shuffle service jar. If you build Flink from source, the shuffle service jar is located at `$FLINK_SOURCE/build-target/opt/yarn-shuffle/flink-shuffle-service-<version>.jar`. If your are using pre-packaged distribution, The shuffle service jar is located at `$FLINK_DIST_HOME/opt/yarn-shuffle//flink-shuffle-service-<version>.jar`.
2. Add this jar to the CLASSPATH of all the NodeManagers in your YARN cluster. 
3. Add the following configuration in the `yarn-site.xml`:
    ```$xslt
    <property>
      <name>yarn.nodemanager.aux-services</name>
       <!-- Add yarn_shuffle_service_for_flink to the end of the list  -->
      <value>..., yarn_shuffle_service_for_flink</value>
    </property>
    
    <property>
      <name>yarn.nodemanager.aux-services.yarn_shuffle_service_for_flink.class</name>
      <value>org.apache.flink.network.yarn.YarnShuffleService</value>
    </property>
    ```
4. By default shuffle service will use up to 300MB of direct memory and 64MB of heap memory. Increase the configured memory of the NodeManagers if needed.
5. Restart all the NodeManagers in your YARN cluster.

After the shuffle service has started, you can create batch jobs with the Table API to use the shuffle service. Batch jobs can be declared with the Table API by setting

```$xslt
sql.exec.data-exchange-mode.all-batch: true
```

By default, the intermediate data of the batch jobs are served by the embedded endpoint in the TaskManager. To change the endpoint to the YARN shuffle service, you need to set the following configuration:

```$xslt
task.blocking.shuffle.type: YARN
```

### Configure the YARN shuffle service

<div class="alert alert-warning">
  <strong>Note:</strong> The shuffle service acts as a plugin in in the YARN NodeManager, so all the following configurations should be set in the <code>yarn-site.xml</code>.
</div>

The basic architecture of the shuffle service is illustrated as follows. The map-side tasks first write data to disks, then the shuffle service reads  data out and sends them to the reduce-side tasks. 

<img src="{{ site.baseurl }}/fig/yarn-shuffle-service.png" class="img-responsive" style="width: 80%">

#### Configure the root directories
YARN shuffle service supports using multiple directories to store the intermediate data on a single machine. The default root directories are the NodeManager local directories. You can change the default directories by configuring `flink.shuffle-service.local-dirs`, but it is not recommended since other directories cannot be cleared by YARN when the applications stop. 

YARN shuffle service maintains a group of threads to read data from each directory. Suitable number of read threads may differ for directories with different disk types, therefore, YARN shuffle service allows user to specify the disk types of each directory and configure different thread number for each disk type.

By default YARN shuffle service treats all the directories to be on HDD, and the default number of threads is configured by `flink.shuffle-service.default-io-thread-number-per-disk`. To change the default behavior, you can configure the directory disk types with `flink.shuffle-service.local-dirs` and configure the thread numbers for each disk type by `flink.shuffle-service.io-thread-number-for-disk-type`. 

For example, suppose the NodeManager local directories are `/disk/1/nm-local,/disk/2/nm-local` and `/disk/1` locates on SSD, if you want the YARN shuffle service to be aware of the disk types, you need to 

1. Configure `flink.shuffle-service.local-dirs` to `[SSD]/disk/1/nm-local,/disk/2/nm-local`.
2. Configure `flink.shuffle-service.io-thread-number-for-disk-type` to `SSD: 20` to start 20 IO thread for each root directory on SSD.

When the YARN shuffle service is aware of the disk types, you can also configure to use directories on specific type of disks, as described in [Configure the jobs using external shuffle services]({{site.baseurl}}/ops/config.html#configure-the-disk-type-preferred).

#### Configure the  memory
The total direct and heap memory consumed by the YARN shuffle service is configured by `flink.shuffle-service.direct-memory-limit-in-mb` and `flink.shuffle-service.heap-memory-limit-in-mb`. The direct and heap memory of the NodeManager should also be increased accordingly.

#### TTL for cleaning data

YARN shuffle service cleans the intermediate data in two ways:

1. When one application finishes, YARN will clear its local directory and the intermediate data under the local directory will be cleared meanwhile.
2. Every intermediate data directory also has a TTL, once the interval since the intermediate data directory get inactive exceeds the TTL, the intermediate data directory will be cleared. The TTL is useful for jobs running on a long live session whose corresponding application will not finish before the session stops.

YARN shuffle service classify the intermediate data directories into four types and each type of directories can be configured separately:

- Consumed: All the reduce side tasks have read the intermediate data.
- Partial-consumed: Parts of the reduce side tasks have read the intermediate data.
- Unconsumed: None of the reduce side tasks have read the intermediate data.
- Unfinished: The intermediate data is still being written.


#### Full references 
{% include generated/external_block_shuffle_service_configuration.html %}

{% top %}
