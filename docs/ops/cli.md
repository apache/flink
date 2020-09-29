---
title:  "Command-Line Interface"
nav-title: CLI
nav-parent_id: ops
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

Flink provides a Command-Line Interface (CLI) to run programs that are packaged
as JAR files, and control their execution.  The CLI is part
of any Flink setup, available in local single node setups and in
distributed setups. It is located under `<flink-home>/bin/flink`
and connects by default to the running JobManager that was
started from the same installation directory.

The command line can be used to

- submit jobs for execution,
- cancel a running job,
- provide information about a job,
- list running and waiting jobs,
- trigger and dispose savepoints, and

A prerequisite to using the command line interface is that the Flink
master (JobManager) has been started (via
`<flink-home>/bin/start-cluster.sh`) or that another deployment target such as YARN or Kubernetes is
available.

### Deployment targets

Flink has the concept of executors for defining available deployment targets. You can see the
available executors in the output of `bin/flink --help`, for example:

```
Options for Generic CLI mode:
     -D <property=value>   Generic configuration options for
                           execution/deployment and for the configured executor.
                           The available options can be found at
                           https://ci.apache.org/projects/flink/flink-docs-stabl
                           e/ops/config.html
     -t,--target <arg>     The deployment target for the given application,
                           which is equivalent to the "execution.target" config
                           option. The currently available targets are:
                           "remote", "local", "kubernetes-session", "yarn-per-job",
                           "yarn-session", "yarn-application" and "kubernetes-application".
```

When running one of the `bin/flink` actions, the executor is specified using the `--executor`
option.                  



* This will be replaced by the TOC
{:toc}

## Examples
### Job Submission Examples
-----------------------------

These examples about how to submit a job in CLI.
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

-   Run example program with no arguments:

        ./bin/flink run ./examples/batch/WordCount.jar

-   Run example program with arguments for input and result files:

        ./bin/flink run ./examples/batch/WordCount.jar \
                             --input file:///home/user/hamlet.txt --output file:///home/user/wordcount_out

-   Run example program with parallelism 16 and arguments for input and result files:

        ./bin/flink run -p 16 ./examples/batch/WordCount.jar \
                             --input file:///home/user/hamlet.txt --output file:///home/user/wordcount_out

-   Run example program in detached mode:

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

        ./bin/flink run -m yarn-cluster \
                               ./examples/batch/WordCount.jar \
                               --input hdfs:///user/hamlet.txt --output hdfs:///user/wordcount_out

</div>

<div data-lang="python" markdown="1">

<span class="label label-info">Note</span> When submitting Python job via `flink run`, Flink will run the command “python”. Please run the following command to confirm that the command “python” in current environment points to a specified Python version 3.5, 3.6, 3.7 or 3.8:

{% highlight bash %}
$ python --version
# the version printed here must be 3.5, 3.6, 3.7 or 3.8
{% endhighlight %}

-   Run Python Table program:

        ./bin/flink run -py examples/python/table/batch/word_count.py

-   Run Python Table program with pyFiles:

        ./bin/flink run -py examples/python/table/batch/word_count.py \
                                -pyfs file:///user.txt,hdfs:///$namenode_address/username.txt

-   Run Python Table program with a JAR file:

        ./bin/flink run -py examples/python/table/batch/word_count.py -j <jarFile>

-   Run Python Table program with pyFiles and pyModule:

        ./bin/flink run -pym batch.word_count -pyfs examples/python/table/batch

-   Run Python Table program with parallelism 16:

        ./bin/flink run -p 16 -py examples/python/table/batch/word_count.py

-   Run Python Table program with flink log output disabled:

        ./bin/flink run -q -py examples/python/table/batch/word_count.py

-   Run Python Table program in detached mode:

        ./bin/flink run -d -py examples/python/table/batch/word_count.py

-   Run Python Table program on a specific JobManager:

        ./bin/flink run -m myJMHost:8081 \
                               -py examples/python/table/batch/word_count.py

-   Run Python Table program using a [per-job YARN cluster]({{site.baseurl}}/ops/deployment/yarn_setup.html#run-a-single-flink-job-on-hadoop-yarn) with 2 TaskManagers:

        ./bin/flink run -m yarn-cluster \
                               -py examples/python/table/batch/word_count.py
</div>

### Job Management Examples
-----------------------------

These examples about how to manage a job in CLI.

-   Display the optimized execution plan for the WordCount example program as JSON:

        ./bin/flink info ./examples/batch/WordCount.jar \
                                --input file:///home/user/hamlet.txt --output file:///home/user/wordcount_out

-   List scheduled and running jobs (including their JobIDs):

        ./bin/flink list

-   List scheduled jobs (including their JobIDs):

        ./bin/flink list -s

-   List running jobs (including their JobIDs):

        ./bin/flink list -r

-   List all existing jobs (including their JobIDs):

        ./bin/flink list -a

-   List running Flink jobs inside Flink YARN session:

        ./bin/flink list -m yarn-cluster -yid <yarnApplicationID> -r

-   Cancel a job:

        ./bin/flink cancel <jobID>

-   Cancel a job with a savepoint (deprecated; use "stop" instead):

        ./bin/flink cancel -s [targetDirectory] <jobID>

-   Gracefully stop a job with a savepoint (streaming jobs only):

        ./bin/flink stop [-p targetDirectory] [-d] <jobID>

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

#### Stop

Use the `stop` to gracefully stop a running streaming job with a savepoint.

{% highlight bash %}
./bin/flink stop [-p targetDirectory] [-d] <jobID>
{% endhighlight %}

A "stop" call is a more graceful way of stopping a running streaming job, as the "stop" signal flows from
source to sink. When the user requests to stop a job, all sources will be requested to send the last checkpoint barrier
that will trigger a savepoint, and after the successful completion of that savepoint, they will finish by calling their
`cancel()` method. If the `-d` flag is specified, then a `MAX_WATERMARK` will be emitted before the last checkpoint
barrier. This will result all registered event-time timers to fire, thus flushing out any state that is waiting for
a specific watermark, e.g. windows. The job will keep running until all sources properly shut down. This allows the
 job to finish processing all in-flight data.


#### Cancel with a savepoint (deprecated)

You can atomically trigger a savepoint and cancel a job.

{% highlight bash %}
./bin/flink cancel -s [savepointDirectory] <jobID>
{% endhighlight %}

If no savepoint directory is configured, you need to configure a default savepoint directory for the Flink installation (see [Savepoints]({{site.baseurl}}/ops/state/savepoints.html#configuration)).

The job will only be cancelled if the savepoint succeeds.

<p style="border-radius: 5px; padding: 5px" class="bg-danger">
    <b>Note</b>: Cancelling a job with savepoint is deprecated. Use "stop" instead.
</p>

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
                                          ("main()" method). Only needed if the
                                          JAR file does not specify the class in
                                          its manifest.
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
     -py,--python <pythonFile>            Python script with the program entry
                                          point. The dependent resources can be
                                          configured with the `--pyFiles`
                                          option.
     -pyarch,--pyArchives <arg>           Add python archive files for job. The
                                          archive files will be extracted to the
                                          working directory of python UDF
                                          worker. Currently only zip-format is
                                          supported. For each archive file, a
                                          target directory be specified. If the
                                          target directory name is specified,
                                          the archive file will be extracted to
                                          a name can directory with the
                                          specified name. Otherwise, the archive
                                          file will be extracted to a directory
                                          with the same name of the archive
                                          file. The files uploaded via this
                                          option are accessible via relative
                                          path. '#' could be used as the
                                          separator of the archive file path and
                                          the target directory name. Comma (',')
                                          could be used as the separator to
                                          specify multiple archive files. This
                                          option can be used to upload the
                                          virtual environment, the data files
                                          used in Python UDF (e.g.: --pyArchives
                                          file:///tmp/py37.zip,file:///tmp/data.
                                          zip#data --pyExecutable
                                          py37.zip/py37/bin/python). The data
                                          files could be accessed in Python UDF,
                                          e.g.: f = open('data/data.txt', 'r').
     -pyexec,--pyExecutable <arg>         Specify the path of the python
                                          interpreter used to execute the python
                                          UDF worker (e.g.: --pyExecutable
                                          /usr/local/bin/python3). The python
                                          UDF worker depends on a specified Python
                                          version 3.5, 3.6 3.7 or 3.8, Apache Beam
                                          (version == 2.23.0), Pip (version >= 7.1.0)
                                          and SetupTools (version >= 37.0.0).
                                          Please ensure that the specified environment
                                          meets the above requirements.
     -pyfs,--pyFiles <pythonFiles>        Attach custom python files for job.
                                          These files will be added to the
                                          PYTHONPATH of both the local client
                                          and the remote python UDF worker. The
                                          standard python resource file suffixes
                                          such as .py/.egg/.zip or directory are
                                          all supported. Comma (',') could be
                                          used as the separator to specify
                                          multiple files (e.g.: --pyFiles
                                          file:///tmp/myresource.zip,hdfs:///$na
                                          menode_address/myresource2.zip).
     -pym,--pyModule <pythonModule>       Python module with the program entry
                                          point. This option must be used in
                                          conjunction with `--pyFiles`.
     -pyreq,--pyRequirements <arg>        Specify a requirements.txt file which
                                          defines the third-party dependencies.
                                          These dependencies will be installed
                                          and added to the PYTHONPATH of the
                                          python UDF worker. A directory which
                                          contains the installation packages of
                                          these dependencies could be specified
                                          optionally. Use '#' as the separator
                                          if the optional parameter exists
                                          (e.g.: --pyRequirements
                                          file:///tmp/requirements.txt#file:///t
                                          mp/cached_dir).
     -s,--fromSavepoint <savepointPath>   Path to a savepoint to restore the job
                                          from (for example
                                          hdfs:///flink/savepoint-1537).
     -sae,--shutdownOnAttachedExit        If the job is submitted in attached
                                          mode, perform a best-effort cluster
                                          shutdown when the CLI is terminated
                                          abruptly, e.g., in response to a user
                                          interrupt, such as typing Ctrl + C.
  Options for Generic CLI mode:
     -D <property=value>   Generic configuration options for
                           execution/deployment and for the configured executor.
                           The available options can be found at
                           https://ci.apache.org/projects/flink/flink-docs-stabl
                           e/ops/config.html
     -e,--executor <arg>   DEPRECATED: Please use the -t option instead which is
                           also available with the "Application Mode".
                           The name of the executor to be used for executing the
                           given job, which is equivalent to the
                           "execution.target" config option. The currently
                           available executors are: "remote", "local",
                           "kubernetes-session", "yarn-per-job", "yarn-session".
     -t,--target <arg>     The deployment target for the given application,
                           which is equivalent to the "execution.target" config
                           option. The currently available targets are:
                           "remote", "local", "kubernetes-session",
                           "yarn-per-job", "yarn-session", "yarn-application"
                           and "kubernetes-application".

  Options for yarn-cluster mode:
     -d,--detached                        If present, runs the job in detached
                                          mode
     -m,--jobmanager <arg>                Set to yarn-cluster to use YARN
                                          execution mode.
     -yat,--yarnapplicationType <arg>     Set a custom application type for the
                                          application on YARN
     -yD <property=value>                 use value for given property
     -yd,--yarndetached                   If present, runs the job in detached
                                          mode (deprecated; use non-YARN
                                          specific option instead)
     -yh,--yarnhelp                       Help for the Yarn session CLI.
     -yid,--yarnapplicationId <arg>       Attach to running YARN session
     -yj,--yarnjar <arg>                  Path to Flink jar file
     -yjm,--yarnjobManagerMemory <arg>    Memory for JobManager Container with
                                          optional unit (default: MB)
     -ynl,--yarnnodeLabel <arg>           Specify YARN node label for the YARN
                                          application
     -ynm,--yarnname <arg>                Set a custom name for the application
                                          on YARN
     -yq,--yarnquery                      Display available YARN resources
                                          (memory, cores)
     -yqu,--yarnqueue <arg>               Specify YARN queue.
     -ys,--yarnslots <arg>                Number of slots per TaskManager
     -yt,--yarnship <arg>                 Ship files in the specified directory
                                          (t for transfer)
     -ytm,--yarntaskManagerMemory <arg>   Memory per TaskManager Container with
                                          optional unit (default: MB)
     -yz,--yarnzookeeperNamespace <arg>   Namespace to create the Zookeeper
                                          sub-paths for high availability mode
     -z,--zookeeperNamespace <arg>        Namespace to create the Zookeeper
                                          sub-paths for high availability mode

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager to which to
                                     connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration. Attention: This
                                     option is respected only if the
                                     high-availability configuration is NONE.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode



Action "info" shows the optimized execution plan of the program (JSON).

  Syntax: info [OPTIONS] <jar-file> <arguments>
  "info" action options:
     -c,--class <classname>           Class with the program entry point
                                      ("main()" method). Only needed if the JAR
                                      file does not specify the class in its
                                      manifest.
     -p,--parallelism <parallelism>   The parallelism with which to run the
                                      program. Optional flag to override the
                                      default value specified in the
                                      configuration.


Action "list" lists running and scheduled programs.

  Syntax: list [OPTIONS]
  "list" action options:
     -a,--all         Show all programs and their JobIDs
     -r,--running     Show only running programs and their JobIDs
     -s,--scheduled   Show only scheduled programs and their JobIDs
  Options for Generic CLI mode:
     -D <property=value>   Generic configuration options for
                           execution/deployment and for the configured executor.
                           The available options can be found at
                           https://ci.apache.org/projects/flink/flink-docs-stabl
                           e/ops/config.html
     -e,--executor <arg>   DEPRECATED: Please use the -t option instead which is
                           also available with the "Application Mode".
                           The name of the executor to be used for executing the
                           given job, which is equivalent to the
                           "execution.target" config option. The currently
                           available executors are: "remote", "local",
                           "kubernetes-session", "yarn-per-job", "yarn-session".
     -t,--target <arg>     The deployment target for the given application,
                           which is equivalent to the "execution.target" config
                           option. The currently available targets are:
                           "remote", "local", "kubernetes-session",
                           "yarn-per-job", "yarn-session", "yarn-application"
                           and "kubernetes-application".

  Options for yarn-cluster mode:
     -m,--jobmanager <arg>            Set to yarn-cluster to use YARN execution
                                      mode.
     -yid,--yarnapplicationId <arg>   Attach to running YARN session
     -z,--zookeeperNamespace <arg>    Namespace to create the Zookeeper
                                      sub-paths for high availability mode

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager to which to
                                     connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration. Attention: This
                                     option is respected only if the
                                     high-availability configuration is NONE.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode



Action "stop" stops a running program with a savepoint (streaming jobs only).

  Syntax: stop [OPTIONS] <Job ID>
  "stop" action options:
     -d,--drain                           Send MAX_WATERMARK before taking the
                                          savepoint and stopping the pipelne.
     -p,--savepointPath <savepointPath>   Path to the savepoint (for example
                                          hdfs:///flink/savepoint-1537). If no
                                          directory is specified, the configured
                                          default will be used
                                          ("state.savepoints.dir").
  Options for Generic CLI mode:
     -D <property=value>   Generic configuration options for
                           execution/deployment and for the configured executor.
                           The available options can be found at
                           https://ci.apache.org/projects/flink/flink-docs-stabl
                           e/ops/config.html
     -e,--executor <arg>   DEPRECATED: Please use the -t option instead which is
                           also available with the "Application Mode".
                           The name of the executor to be used for executing the
                           given job, which is equivalent to the
                           "execution.target" config option. The currently
                           available executors are: "remote", "local",
                           "kubernetes-session", "yarn-per-job", "yarn-session".
     -t,--target <arg>     The deployment target for the given application,
                           which is equivalent to the "execution.target" config
                           option. The currently available targets are:
                           "remote", "local", "kubernetes-session",
                           "yarn-per-job", "yarn-session", "yarn-application"
                           and "kubernetes-application".

  Options for yarn-cluster mode:
     -m,--jobmanager <arg>            Set to yarn-cluster to use YARN execution
                                      mode.
     -yid,--yarnapplicationId <arg>   Attach to running YARN session
     -z,--zookeeperNamespace <arg>    Namespace to create the Zookeeper
                                      sub-paths for high availability mode

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager to which to
                                     connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration. Attention: This
                                     option is respected only if the
                                     high-availability configuration is NONE.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode



Action "cancel" cancels a running program.

  Syntax: cancel [OPTIONS] <Job ID>
  "cancel" action options:
     -s,--withSavepoint <targetDirectory>   **DEPRECATION WARNING**: Cancelling
                                            a job with savepoint is deprecated.
                                            Use "stop" instead.
                                            Trigger savepoint and cancel job.
                                            The target directory is optional. If
                                            no directory is specified, the
                                            configured default directory
                                            (state.savepoints.dir) is used.
  Options for Generic CLI mode:
     -D <property=value>   Generic configuration options for
                           execution/deployment and for the configured executor.
                           The available options can be found at
                           https://ci.apache.org/projects/flink/flink-docs-stabl
                           e/ops/config.html
     -e,--executor <arg>   DEPRECATED: Please use the -t option instead which is
                           also available with the "Application Mode".
                           The name of the executor to be used for executing the
                           given job, which is equivalent to the
                           "execution.target" config option. The currently
                           available executors are: "remote", "local",
                           "kubernetes-session", "yarn-per-job", "yarn-session".
     -t,--target <arg>     The deployment target for the given application,
                           which is equivalent to the "execution.target" config
                           option. The currently available targets are:
                           "remote", "local", "kubernetes-session",
                           "yarn-per-job", "yarn-session", "yarn-application"
                           and "kubernetes-application".

  Options for yarn-cluster mode:
     -m,--jobmanager <arg>            Set to yarn-cluster to use YARN execution
                                      mode.
     -yid,--yarnapplicationId <arg>   Attach to running YARN session
     -z,--zookeeperNamespace <arg>    Namespace to create the Zookeeper
                                      sub-paths for high availability mode

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager to which to
                                     connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration. Attention: This
                                     option is respected only if the
                                     high-availability configuration is NONE.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode



Action "savepoint" triggers savepoints for a running job or disposes existing ones.

  Syntax: savepoint [OPTIONS] <Job ID> [<target directory>]
  "savepoint" action options:
     -d,--dispose <arg>       Path of savepoint to dispose.
     -j,--jarfile <jarfile>   Flink program JAR file.
  Options for Generic CLI mode:
     -D <property=value>   Generic configuration options for
                           execution/deployment and for the configured executor.
                           The available options can be found at
                           https://ci.apache.org/projects/flink/flink-docs-stabl
                           e/ops/config.html
     -e,--executor <arg>   DEPRECATED: Please use the -t option instead which is
                           also available with the "Application Mode".
                           The name of the executor to be used for executing the
                           given job, which is equivalent to the
                           "execution.target" config option. The currently
                           available executors are: "remote", "local",
                           "kubernetes-session", "yarn-per-job", "yarn-session".
     -t,--target <arg>     The deployment target for the given application,
                           which is equivalent to the "execution.target" config
                           option. The currently available targets are:
                           "remote", "local", "kubernetes-session",
                           "yarn-per-job", "yarn-session", "yarn-application"
                           and "kubernetes-application".

  Options for yarn-cluster mode:
     -m,--jobmanager <arg>            Set to yarn-cluster to use YARN execution
                                      mode.
     -yid,--yarnapplicationId <arg>   Attach to running YARN session
     -z,--zookeeperNamespace <arg>    Namespace to create the Zookeeper
                                      sub-paths for high availability mode

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager to which to
                                     connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration. Attention: This
                                     option is respected only if the
                                     high-availability configuration is NONE.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode
{% endhighlight %}

{% top %}
