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

* This will be replaced by the TOC
{:toc}


Flink provides a command-line interface to run programs that are packaged
as JAR files, and control their execution.  The command line interface is part
of any Flink setup, available in local single node setups and in
distributed setups. It is located under `<flink-home>/bin/flink`
and connects by default to the running Flink master (JobManager) that was
started from the same installation directory.

A prerequisite to using the command line interface is that the Flink
master (JobManager) has been started (via `<flink-home>/bin/start-
local.sh` or `<flink-home>/bin/start-cluster.sh`).

The command line can be used to

- submit jobs for execution,
- cancel a running job,
- provide information about a job, and
- list running and waiting jobs.

## Examples

-   Run example program with no arguments.

        ./bin/flink run ./examples/flink-java-examples-{{ site.FLINK_VERSION_STABLE }}-WordCount.jar

-   Run example program with arguments for input and result files

        ./bin/flink run ./examples/flink-java-examples-{{ site.FLINK_VERSION_STABLE }}-WordCount.jar \
                               file:///home/user/hamlet.txt file:///home/user/wordcount_out

-   Run example program with parallelism 16 and arguments for input and result files

        ./bin/flink run -p 16 ./examples/flink-java-examples-{{ site.FLINK_VERSION_STABLE }}-WordCount.jar \
                                file:///home/user/hamlet.txt file:///home/user/wordcount_out

-   Run example program on a specific JobManager:

        ./bin/flink run -m myJMHost:6123 \
                               ./examples/flink-java-examples-{{ site.FLINK_VERSION_STABLE }}-WordCount.jar \
                               -file:///home/user/hamlet.txt file:///home/user/wordcount_out


-   Display the expected arguments for the WordCount example program:

        ./bin/flink info -d ./examples/flink-java-examples-{{ site.FLINK_VERSION_STABLE }}-WordCount.jar

-   Display the optimized execution plan for the WordCount example program as JSON:

        ./bin/flink info -e 
                                ./examples/flink-java-examples-{{ site.FLINK_VERSION_STABLE }}-WordCount.jar \
                                file:///home/user/hamlet.txt file:///home/user/wordcount_out

-   List scheduled and running jobs (including their JobIDs):

        ./bin/flink list -s -r

-   Cancel a job:

        ./bin/flink cancel -i <jobID>

## Usage

The command line syntax is as follows:

~~~
./flink <ACTION> [OPTIONS] [ARGUMENTS]

General options:
     -h,--help      Show the help for the CLI Frontend, or a specific action.
     -v,--verbose   Print more detailed error messages.


Action "run" - compiles and submits a Flink program that is given in the form of a JAR file.

  "run" options:

     -p,--parallelism <parallelism> The degree of parallelism for the execution. This value is used unless the program overrides the degree of parallelism on the execution environment or program plan. If this option is not set, then the execution will use the default parallelism specified in the flink-conf.yaml file.

     -c,--class <classname>         The class with the entry point (main method, or getPlan() method). Needs only be specified if the JAR file has no manifest pointing to that class. See program packaging instructions for details.

     -m,--jobmanager <host:port>    Option to submit the program to a different Flink master (JobManager).

  "run" arguments:

     - The first argument is the path to the JAR file of the program.
     - All successive arguments are passed to the program's main method (or getPlan() method).


Action "info" - displays information about a Flink program.

  "info" action arguments:
     -d,--description               Show description of the program, if the main class implements the 'ProgramDescription' interface.

     -e,--executionplan             Show the execution data flow plan of the program, in JSON representation.

     -p,--parallelism <parallelism> The degree of parallelism for the execution, see above. The parallelism is relevant for the execution plan. The option is only evaluated if used together with the -e option.

     -c,--class <classname>         The class with the entry point (main method, or getPlan() method). Needs only be specified if the JAR file has no manifest pointing to that class. See program packaging instructions for details.

     -m,--jobmanager <host:port>    Option to connect to a different Flink master (JobManager). Connecting to a master is relevant to compile the execution plan. The option is only evaluated if used together with the -e option.

  "info" arguments:

     - The first argument is the path to the JAR file of the program.
     - All successive arguments are passed to the program's main method (or getPlan() method).


Action "list" lists submitted Flink programs.

  "list" action arguments:

     -r,--running                   Show running programs and their JobIDs

     -s,--scheduled                 Show scheduled programs and their JobIDs

     -m,--jobmanager <host:port>    Option to connect to a different Flink master (JobManager).


Action "cancel" cancels a submitted Flink program.

  "cancel" action arguments:

     -i,--jobid <jobID>             JobID of program to cancel
     
     -m,--jobmanager <host:port>    Option to connect to a different Flink master (JobManager).
~~~
