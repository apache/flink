---
title:  "Command-Line Interface"
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
master (JobManager) has been started (via `<flink-home>/bin/start-
local.sh` or `<flink-home>/bin/start-cluster.sh`) or that a YARN
environment is available.

The command line can be used to

- submit jobs for execution,
- cancel a running job,
- provide information about a job, and
- list running and waiting jobs.

* This will be replaced by the TOC
{:toc}

## Examples

-   Run example program with no arguments.

        ./bin/flink run ./examples/flink-java-examples-{{ site.version }}-WordCount.jar

-   Run example program with arguments for input and result files

        ./bin/flink run ./examples/flink-java-examples-{{ site.version }}-WordCount.jar \
                               file:///home/user/hamlet.txt file:///home/user/wordcount_out

-   Run example program with parallelism 16 and arguments for input and result files

        ./bin/flink run -p 16 ./examples/flink-java-examples-{{ site.version }}-WordCount.jar \
                                file:///home/user/hamlet.txt file:///home/user/wordcount_out

-   Run example program on a specific JobManager:

        ./bin/flink run -m myJMHost:6123 \
                               ./examples/flink-java-examples-{{ site.version }}-WordCount.jar \
                               file:///home/user/hamlet.txt file:///home/user/wordcount_out

-   Run example program with a specific class as an entry point:

        ./bin/flink run -c org.apache.flink.examples.java.wordcount.WordCount \
                               ./examples/flink-java-examples-{{ site.version }}-WordCount.jar \
                               file:///home/user/hamlet.txt file:///home/user/wordcount_out

-   Run example program using a [per-job YARN cluster]({{site.baseurl}}/setup/yarn_setup.html#run-a-single-flink-job-on-hadoop-yarn) with 2 TaskManagers:

        ./bin/flink run -m yarn-cluster -yn 2 \
                               ./examples/flink-java-examples-{{ site.version }}-WordCount.jar \
                               hdfs:///user/hamlet.txt hdfs:///user/wordcount_out

-   Display the optimized execution plan for the WordCount example program as JSON:

        ./bin/flink info ./examples/flink-java-examples-{{ site.version }}-WordCount.jar \
                                file:///home/user/hamlet.txt file:///home/user/wordcount_out

-   List scheduled and running jobs (including their JobIDs):

        ./bin/flink list

-   List scheduled jobs (including their JobIDs):

        ./bin/flink list -s

-   List running jobs (including their JobIDs):

        ./bin/flink list -r

-   Cancel a job:

        ./bin/flink cancel <jobID>

## Usage

The command line syntax is as follows:

~~~
./flink <ACTION> [OPTIONS] [ARGUMENTS]

The following actions are available:

Action "run" compiles and runs a program.

  Syntax: run [OPTIONS] <jar-file> <arguments>
  "run" action options:
     -c,--class <classname>           Class with the program entry point ("main"
                                      method or "getPlan()" method. Only needed
                                      if the JAR file does not specify the class
                                      in its manifest.
     -m,--jobmanager <host:port>      Address of the JobManager (master) to
                                      which to connect. Specify 'yarn-cluster'
                                      as the JobManager to deploy a YARN cluster
                                      for the job. Use this flag to connect to a
                                      different JobManager than the one
                                      specified in the configuration.
     -p,--parallelism <parallelism>   The parallelism with which to run the
                                      program. Optional flag to override the
                                      default value specified in the
                                      configuration.
  Additional arguments if -m yarn-cluster is set:
     -yD <arg>                            Dynamic properties
     -yd,--yarndetached                   Start detached
     -yj,--yarnjar <arg>                  Path to Flink jar file
     -yjm,--yarnjobManagerMemory <arg>    Memory for JobManager Container [in
                                          MB]
     -yn,--yarncontainer <arg>            Number of YARN container to allocate
                                          (=Number of Task Managers)
     -yq,--yarnquery                      Display available YARN resources
                                          (memory, cores)
     -yqu,--yarnqueue <arg>               Specify YARN queue.
     -ys,--yarnslots <arg>                Number of slots per TaskManager
     -yt,--yarnship <arg>                 Ship files in the specified directory
                                          (t for transfer)
     -ytm,--yarntaskManagerMemory <arg>   Memory per TaskManager Container [in
                                          MB]


Action "info" shows the optimized execution plan of the program (JSON).

  Syntax: info [OPTIONS] <jar-file> <arguments>
  "info" action options:
     -c,--class <classname>           Class with the program entry point ("main"
                                      method or "getPlan()" method. Only needed
                                      if the JAR file does not specify the class
                                      in its manifest.
     -m,--jobmanager <host:port>      Address of the JobManager (master) to
                                      which to connect. Specify 'yarn-cluster'
                                      as the JobManager to deploy a YARN cluster
                                      for the job. Use this flag to connect to a
                                      different JobManager than the one
                                      specified in the configuration.
     -p,--parallelism <parallelism>   The parallelism with which to run the
                                      program. Optional flag to override the
                                      default value specified in the
                                      configuration.


Action "list" lists running and scheduled programs.

  Syntax: list [OPTIONS]
  "list" action options:
     -m,--jobmanager <host:port>   Address of the JobManager (master) to which
                                   to connect. Specify 'yarn-cluster' as the
                                   JobManager to deploy a YARN cluster for the
                                   job. Use this flag to connect to a different
                                   JobManager than the one specified in the
                                   configuration.
     -r,--running                  Show only running programs and their JobIDs
     -s,--scheduled                Show only scheduled programs and their JobIDs


Action "cancel" cancels a running program.

  Syntax: cancel [OPTIONS] <Job ID>
  "cancel" action options:
     -m,--jobmanager <host:port>   Address of the JobManager (master) to which
                                   to connect. Specify 'yarn-cluster' as the
                                   JobManager to deploy a YARN cluster for the
                                   job. Use this flag to connect to a different
                                   JobManager than the one specified in the
                                   configuration.
~~~
