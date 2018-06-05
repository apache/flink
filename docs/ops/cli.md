---
title:  "Command-Line Interface"
nav-title: CLI
nav-parent_id: ops
nav-pos: 6
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

Flink provides a command-line interface to run programs that are packaged
as JAR files, and control their execution.  The command line interface is part
of any Flink setup, available in local single node setups and in
distributed setups. It is located under `<flink-home>/bin/flink`
and connects by default to the running Flink master (JobManager) that was
started from the same installation directory.

A prerequisite to using the command line interface is that the Flink
master (JobManager) has been started (via
`<flink-home>/bin/start-cluster.sh`) or that a YARN environment is
available.

The command line can be used to

- submit jobs for execution,
- cancel a running job,
- provide information about a job,
- list running and waiting jobs,
- trigger and dispose savepoints, and
- modify a running job

* This will be replaced by the TOC
{:toc}

## Examples

-   Run example program with no arguments.

        ./bin/flink run ./examples/batch/WordCount.jar

-   Run example program with arguments for input and result files

        ./bin/flink run ./examples/batch/WordCount.jar \
                             --input file:///home/user/hamlet.txt --output file:///home/user/wordcount_out

-   Run example program with parallelism 16 and arguments for input and result files

        ./bin/flink run -p 16 ./examples/batch/WordCount.jar \
                             --input file:///home/user/hamlet.txt --output file:///home/user/wordcount_out

-   Run example program with flink log output disabled

            ./bin/flink run -q ./examples/batch/WordCount.jar

-   Run example program in detached mode

            ./bin/flink run -d ./examples/batch/WordCount.jar

-   Run example program on a specific JobManager:

        ./bin/flink run -m myJMHost:8081 \
                               ./examples/batch/WordCount.jar \
                               --input file:///home/user/hamlet.txt --output file:///home/user/wordcount_out

-   Run example program with a specific class as an entry point:

        ./bin/flink run -c org.apache.flink.examples.java.wordcount.WordCount \
                               ./examples/batch/WordCount.jar \
                               --input file:///home/user/hamlet.txt --output file:///home/user/wordcount_out

-   Run example program using a [per-job YARN cluster]({{site.baseurl}}/ops/deployment/yarn_setup.html#run-a-single-flink-job-on-hadoop-yarn) with 2 TaskManagers:

        ./bin/flink run -m yarn-cluster -yn 2 \
                               ./examples/batch/WordCount.jar \
                               --input hdfs:///user/hamlet.txt --output hdfs:///user/wordcount_out

-   Display the optimized execution plan for the WordCount example program as JSON:

        ./bin/flink info ./examples/batch/WordCount.jar \
                                --input file:///home/user/hamlet.txt --output file:///home/user/wordcount_out

-   List scheduled and running jobs (including their JobIDs):

        ./bin/flink list

-   List scheduled jobs (including their JobIDs):

        ./bin/flink list -s

-   List running jobs (including their JobIDs):

        ./bin/flink list -r

-   List running Flink jobs inside Flink YARN session:

        ./bin/flink list -m yarn-cluster -yid <yarnApplicationID> -r

-   Cancel a job:

        ./bin/flink cancel <jobID>

-   Cancel a job with a savepoint:

        ./bin/flink cancel -s [targetDirectory] <jobID>

-   Stop a job (streaming jobs only):

        ./bin/flink stop <jobID>
        
-   Modify a running job (streaming jobs only):
        ./bin/flink modify <jobID> -p <newParallelism>


The difference between cancelling and stopping a (streaming) job is the following:

On a cancel call, the operators in a job immediately receive a `cancel()` method call to cancel them as
soon as possible.
If operators are not not stopping after the cancel call, Flink will start interrupting the thread periodically
until it stops.

A "stop" call is a more graceful way of stopping a running streaming job. Stop is only available for jobs
which use sources that implement the `StoppableFunction` interface. When the user requests to stop a job,
all sources will receive a `stop()` method call. The job will keep running until all sources properly shut down.
This allows the job to finish processing all inflight data.

### Savepoints

[Savepoints]({{site.baseurl}}/ops/state/savepoints.html) are controlled via the command line client:

#### Trigger a Savepoint

{% highlight bash %}
./bin/flink savepoint <jobId> [savepointDirectory]
{% endhighlight %}

This will trigger a savepoint for the job with ID `jobId`, and returns the path of the created savepoint. You need this path to restore and dispose savepoints.


Furthermore, you can optionally specify a target file system directory to store the savepoint in. The directory needs to be accessible by the JobManager.

If you don't specify a target directory, you need to have [configured a default directory]({{site.baseurl}}/ops/state/savepoints.html#configuration). Otherwise, triggering the savepoint will fail.

#### Trigger a Savepoint with YARN

{% highlight bash %}
./bin/flink savepoint <jobId> [savepointDirectory] -yid <yarnAppId>
{% endhighlight %}

This will trigger a savepoint for the job with ID `jobId` and YARN application ID `yarnAppId`, and returns the path of the created savepoint.

Everything else is the same as described in the above **Trigger a Savepoint** section.

#### Cancel with a savepoint

You can atomically trigger a savepoint and cancel a job.

{% highlight bash %}
./bin/flink cancel -s  [savepointDirectory] <jobID>
{% endhighlight %}

If no savepoint directory is configured, you need to configure a default savepoint directory for the Flink installation (see [Savepoints]({{site.baseurl}}/ops/state/savepoints.html#configuration)).

The job will only be cancelled if the savepoint succeeds.

#### Restore a savepoint

{% highlight bash %}
./bin/flink run -s <savepointPath> ...
{% endhighlight %}

The run command has a savepoint flag to submit a job, which restores its state from a savepoint. The savepoint path is returned by the savepoint trigger command.

By default, we try to match all savepoint state to the job being submitted. If you want to allow to skip savepoint state that cannot be restored with the new job you can set the `allowNonRestoredState` flag. You need to allow this if you removed an operator from your program that was part of the program when the savepoint was triggered and you still want to use the savepoint.

{% highlight bash %}
./bin/flink run -s <savepointPath> -n ...
{% endhighlight %}

This is useful if your program dropped an operator that was part of the savepoint.

#### Dispose a savepoint

{% highlight bash %}
./bin/flink savepoint -d <savepointPath>
{% endhighlight %}

Disposes the savepoint at the given path. The savepoint path is returned by the savepoint trigger command.

If you use custom state instances (for example custom reducing state or RocksDB state), you have to specify the path to the program JAR with which the savepoint was triggered in order to dispose the savepoint with the user code class loader:

{% highlight bash %}
./bin/flink savepoint -d <savepointPath> -j <jarFile>
{% endhighlight %}

Otherwise, you will run into a `ClassNotFoundException`.

## Usage

The command line syntax is as follows:

{% highlight bash %}
./flink <ACTION> [OPTIONS] [ARGUMENTS]

The following actions are available:

Action "run" compiles and runs a program.

  Syntax: run [OPTIONS] <jar-file> <arguments>
  "run" action options:
     -c,--class <classname>               Class with the program entry point
                                          ("main" method or "getPlan()" method.
                                          Only needed if the JAR file does not
                                          specify the class in its manifest.
     -C,--classpath <url>                 Adds a URL to each user code
                                          classloader  on all nodes in the
                                          cluster. The paths must specify a
                                          protocol (e.g. file://) and be
                                          accessible on all nodes (e.g. by means
                                          of a NFS share). You can use this
                                          option multiple times for specifying
                                          more than one URL. The protocol must
                                          be supported by the {@link
                                          java.net.URLClassLoader}.
     -d,--detached                        If present, runs the job in detached
                                          mode
     -n,--allowNonRestoredState           Allow to skip savepoint state that
                                          cannot be restored. You need to allow
                                          this if you removed an operator from
                                          your program that was part of the
                                          program when the savepoint was
                                          triggered.
     -p,--parallelism <parallelism>       The parallelism with which to run the
                                          program. Optional flag to override the
                                          default value specified in the
                                          configuration.
     -q,--sysoutLogging                   If present, suppress logging output to
                                          standard out.
     -s,--fromSavepoint <savepointPath>   Path to a savepoint to restore the job
                                          from (for example
                                          hdfs:///flink/savepoint-1537).
  Options for yarn-cluster mode:
     -d,--detached                        If present, runs the job in detached
                                          mode
     -m,--jobmanager <arg>                Address of the JobManager (master) to
                                          which to connect. Use this flag to
                                          connect to a different JobManager than
                                          the one specified in the
                                          configuration.
     -yD <property=value>                 use value for given property
     -yd,--yarndetached                   If present, runs the job in detached
                                          mode (deprecated; use non-YARN
                                          specific option instead)
     -yh,--yarnhelp                       Help for the Yarn session CLI.
     -yid,--yarnapplicationId <arg>       Attach to running YARN session
     -yj,--yarnjar <arg>                  Path to Flink jar file
     -yjm,--yarnjobManagerMemory <arg>    Memory for JobManager Container [in
                                          MB]
     -yn,--yarncontainer <arg>            Number of YARN container to allocate
                                          (=Number of Task Managers)
     -ynm,--yarnname <arg>                Set a custom name for the application
                                          on YARN
     -yq,--yarnquery                      Display available YARN resources
                                          (memory, cores)
     -yqu,--yarnqueue <arg>               Specify YARN queue.
     -ys,--yarnslots <arg>                Number of slots per TaskManager
     -yst,--yarnstreaming                 Start Flink in streaming mode
     -yt,--yarnship <arg>                 Ship files in the specified directory
                                          (t for transfer)
     -ytm,--yarntaskManagerMemory <arg>   Memory per TaskManager Container [in
                                          MB]
     -yz,--yarnzookeeperNamespace <arg>   Namespace to create the Zookeeper
                                          sub-paths for high availability mode
     -ynl,--yarnnodeLabel <arg>           Specify YARN node label for 
                                          the YARN application 
     -z,--zookeeperNamespace <arg>        Namespace to create the Zookeeper
                                          sub-paths for high availability mode

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager (master) to which
                                     to connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode



Action "info" shows the optimized execution plan of the program (JSON).

  Syntax: info [OPTIONS] <jar-file> <arguments>
  "info" action options:
     -c,--class <classname>           Class with the program entry point ("main"
                                      method or "getPlan()" method. Only needed
                                      if the JAR file does not specify the class
                                      in its manifest.
     -p,--parallelism <parallelism>   The parallelism with which to run the
                                      program. Optional flag to override the
                                      default value specified in the
                                      configuration.


Action "list" lists running and scheduled programs.

  Syntax: list [OPTIONS]
  "list" action options:
     -r,--running     Show only running programs and their JobIDs
     -s,--scheduled   Show only scheduled programs and their JobIDs
  Options for yarn-cluster mode:
     -m,--jobmanager <arg>            Address of the JobManager (master) to
                                      which to connect. Use this flag to connect
                                      to a different JobManager than the one
                                      specified in the configuration.
     -yid,--yarnapplicationId <arg>   Attach to running YARN session
     -z,--zookeeperNamespace <arg>    Namespace to create the Zookeeper
                                      sub-paths for high availability mode

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager (master) to which
                                     to connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode



Action "stop" stops a running program (streaming jobs only).

  Syntax: stop [OPTIONS] <Job ID>
  "stop" action options:

  Options for yarn-cluster mode:
     -m,--jobmanager <arg>            Address of the JobManager (master) to
                                      which to connect. Use this flag to connect
                                      to a different JobManager than the one
                                      specified in the configuration.
     -yid,--yarnapplicationId <arg>   Attach to running YARN session
     -z,--zookeeperNamespace <arg>    Namespace to create the Zookeeper
                                      sub-paths for high availability mode

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager (master) to which
                                     to connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode



Action "cancel" cancels a running program.

  Syntax: cancel [OPTIONS] <Job ID>
  "cancel" action options:
     -s,--withSavepoint <targetDirectory>   Trigger savepoint and cancel job.
                                            The target directory is optional. If
                                            no directory is specified, the
                                            configured default directory
                                            (state.savepoints.dir) is used.
  Options for yarn-cluster mode:
     -m,--jobmanager <arg>            Address of the JobManager (master) to
                                      which to connect. Use this flag to connect
                                      to a different JobManager than the one
                                      specified in the configuration.
     -yid,--yarnapplicationId <arg>   Attach to running YARN session
     -z,--zookeeperNamespace <arg>    Namespace to create the Zookeeper
                                      sub-paths for high availability mode

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager (master) to which
                                     to connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode



Action "savepoint" triggers savepoints for a running job or disposes existing ones.

  Syntax: savepoint [OPTIONS] <Job ID> [<target directory>]
  "savepoint" action options:
     -d,--dispose <arg>       Path of savepoint to dispose.
     -j,--jarfile <jarfile>   Flink program JAR file.
  Options for yarn-cluster mode:
     -m,--jobmanager <arg>            Address of the JobManager (master) to
                                      which to connect. Use this flag to connect
                                      to a different JobManager than the one
                                      specified in the configuration.
     -yid,--yarnapplicationId <arg>   Attach to running YARN session
     -z,--zookeeperNamespace <arg>    Namespace to create the Zookeeper
                                      sub-paths for high availability mode

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager (master) to which
                                     to connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode



Action "modify" modifies a running job (e.g. change of parallelism).

  Syntax: modify <Job ID> [OPTIONS]
  "modify" action options:
     -h,--help                           Show the help message for the CLI
                                         Frontend or the action.
     -p,--parallelism <newParallelism>   New parallelism for the specified job.
     -v,--verbose                        This option is deprecated.
  Options for yarn-cluster mode:
     -m,--jobmanager <arg>            Address of the JobManager (master) to
                                      which to connect. Use this flag to connect
                                      to a different JobManager than the one
                                      specified in the configuration.
     -yid,--yarnapplicationId <arg>   Attach to running YARN session
     -z,--zookeeperNamespace <arg>    Namespace to create the Zookeeper
                                      sub-paths for high availability mode

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager (master) to which
                                     to connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode
{% endhighlight %}

{% top %}
