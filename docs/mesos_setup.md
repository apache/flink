---
title:  "MESOS Setup"
---

* This will be replaced by the TOC
{:toc}

## What is Mesos?

Apache [Mesos](http://mesos.apache.org/) is a cluster resource management framework. It allows to run various distributed applications on top of a cluster. Mesos is especially useful when users want to maximize the utilization of the clusters resources.

**Requirements**

- Apache Mesos
- Apache Flink Directory on every node

### Start Flink Session

Follow these instructions to learn how to launch a Flink Session within your Mesos cluster.

A session will start all required Flink services (JobManager and TaskManagers) so that you can submit programs to the cluster. Note that you can run multiple programs per session.

#### Download Flink for MESOS usage

If you want to build the Flink Mesos client from sources, follow the [build instructions](building.html). Make sure to use the `-P mesos` profile.


#### Start a Session

Use the following command to start a session

~~~bash
./bin/start-mesos.sh -l /home/example/example/mesos/build/src/.libs -m 127.0.0.1:5050
~~~

This command will show you the following overview:

~~~bash
Usage:
   Required
     -j,--jar <arg>      Path to Flink jar file
     -l,--lib <arg>      Path to Mesos library files
     -m,--master <arg>   Address of the Mesos master node
   Optional
     -c,--confDir <arg>              Path to Flink configuration directory
     -D <arg>                        Dynamic properties
     -h,--help                       print help
     -jm,--jobManagerMemory <arg>    Memory for JobManager Container [in MB]
     -jmc,--jobManagerCores <arg>    Number of Jobmanager Cores
     -n,--container <arg>            Number of Task Managers, greedy behaviour if not specified
     -s,--slots <arg>                Number of slots per TaskManager
     -tm,--taskManagerMemory <arg>   Memory per TaskManager Container [in MB]
     -tmc,--taskManagerCores <arg>   Maximum CPU cores per TaskManager.
     -v,--verbose                    Verbose debug mode
     -w,--web                        Launch the web frontend on the jobmanager node.
~~~

Even though there are 4 required parameters listed, the config directory and the required flink jar are filled in by default by the script.

**Example:** Issue the following command to allocate 10 Task Managers, with 8 GB of memory and 32 processing slots each:

~~~bash
./bin/start-mesos.sh -l /home/example/example/mesos/build/src/.libs -m 127.0.0.1:5050 -tm 8192 -s 32
~~~

The system will use the configuration in `conf/flink-config.yaml` by default. Please follow our [configuration guide](config.html) if you want to change something. 

Flink on Mesos will overwrite the following configuration parameters `jobmanager.rpc.address` (because the JobManager is always allocated at different machines) and `parallelization.degree.default` if the number of slots has been specified.

Once Flink is deployed in your Mesos cluster, it will show you the connection details of the Job Manager. In adddition, use the Mesos web interface to monitor the status of the flink session (default location is <your mesos master address>:5050). On this page, you can also access the logs from the Mesos nodes.


## Submit Job to Flink

Use the following command to submit a Flink program to the Mesos cluster:

~~~bash
./bin/flink
~~~

Please refer to the documentation of the [commandline client](cli.html).

The command will show you a help menu like this:

~~~bash
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
~~~

Use the *run* action to submit a job to Mesos. The client is able to determine the address of the JobManager. In the rare event of a problem, you can also pass the JobManager address using the `-m` argument. The JobManager address is visible in the Mesos console.

**Example**

~~~bash
wget -O apache-license-v2.txt http://www.apache.org/licenses/LICENSE-2.0.txt

./bin/flink run -j ./examples/flink-java-examples-{{site.FLINK_VERSION_STABLE }}-WordCount.jar \
                       -a 1 file://`pwd`/apache-license-v2.txt file://`pwd`/wordcount-result.txt 
~~~

If there is the following error, make sure that all TaskManagers started:

~~~bash
Exception in thread "main" org.apache.flinkcompiler.CompilerException:
    Available instances could not be determined from job manager: Connection timed out.
~~~

You can check the number of TaskManagers in the JobManager web interface. The address of this interface is printed in the Mesos session console.

If the TaskManagers do not show up after a minute, you should investigate the issue using the log files.
