---
title: 命令行界面
weight: 5
type: docs
aliases:
  - /zh/deployment/cli.html
  - /zh/apis/cli.html
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

# 命令行界面

Flink provides a Command-Line Interface (CLI) `bin/flink` to run programs that 
are packaged as JAR files and to control their execution. The CLI is part of any 
Flink setup, available in local single node setups and in distributed setups. 
It connects to the running JobManager specified in `conf/flink-conf.yaml`.



## Job Lifecycle Management

A prerequisite for the commands listed in this section to work is to have a running Flink deployment 
like [Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}), 
[YARN]({{< ref "docs/deployment/resource-providers/yarn" >}}) or any other option available. Feel free to 
[start a Flink cluster locally]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}#starting-a-standalone-cluster-session-mode) 
to try the commands on your own machine.
 
### Submitting a Job

Submitting a job means uploading the job's JAR and related dependencies to the Flink cluster and 
initiating the job execution. For the sake of this example, we select a long-running job like 
`examples/streaming/StateMachineExample.jar`. Feel free to select any other JAR archive from the 
`examples/` folder or deploy your own job.
```bash
$ ./bin/flink run \
      --detached \
      ./examples/streaming/StateMachineExample.jar
```
Submitting the job using `--detached` will make the command return after the submission is done.
The output contains (besides other things) the ID of the newly submitted job.
```
Usage with built-in data generator: StateMachineExample [--error-rate <probability-of-invalid-transition>] [--sleep <sleep-per-record-in-ms>]
Usage with Kafka: StateMachineExample --kafka-topic <topic> [--brokers <brokers>]
Options for both the above setups:
        [--backend <file|rocks>]
        [--checkpoint-dir <filepath>]
        [--async-checkpoints <true|false>]
        [--incremental-checkpoints <true|false>]
        [--output <filepath> OR null for stdout]

Using standalone source with error rate 0.000000 and sleep delay 1 millis

Job has been submitted with JobID cca7bc1061d61cf15238e92312c2fc20
```
The usage information printed lists job-related parameters that can be added to the end of the job 
submission command if necessary. For the purpose of readability, we assume that the returned JobID is 
stored in a variable `JOB_ID` for the commands below:
```bash
$ export JOB_ID="cca7bc1061d61cf15238e92312c2fc20"
```

There is another action called `run-application` available to run the job in 
[Application Mode]({{< ref "docs/deployment/overview" >}}#application-mode). This documentation does not address
this action individually as it works similarly to the `run` action in terms of the CLI frontend.

The `run` and `run-application` commands support passing additional configuration parameters via the
`-D` argument. For example setting the [maximum parallelism]({{< ref "docs/deployment/config#pipeline-max-parallelism" >}}#application-mode) 
for a job can be done by setting `-Dpipeline.max-parallelism=120`. This argument is very useful for
configuring per-job or application mode clusters, because you can pass any configuration parameter 
to the cluster, without changing the configuration file.

When submitting a job to an existing session cluster, only [execution configuration parameters]({{< ref "docs/deployment/config#execution" >}}) are supported.

### Job Monitoring

You can monitor any running jobs using the `list` action:
```bash
$ ./bin/flink list
```
```
Waiting for response...
------------------ Running/Restarting Jobs -------------------
30.11.2020 16:02:29 : cca7bc1061d61cf15238e92312c2fc20 : State machine job (RUNNING)
--------------------------------------------------------------
No scheduled jobs.
```
Jobs that were submitted but not started, yet, would be listed under "Scheduled Jobs".

### Creating a Savepoint

[Savepoints]({{< ref "docs/ops/state/savepoints" >}}) can be created to save the current state a job is 
in. All that's needed is the JobID:
```bash
$ ./bin/flink savepoint \
      $JOB_ID \ 
      /tmp/flink-savepoints
```
```
Triggering savepoint for job cca7bc1061d61cf15238e92312c2fc20.
Waiting for response...
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
You can resume your program from this savepoint with the run command.
```
The savepoint folder is optional and needs to be specified if 
[state.savepoints.dir]({{< ref "docs/deployment/config" >}}#state-savepoints-dir) isn't set.

The path to the savepoint can be used later on to [restart the Flink job](#starting-a-job-from-a-savepoint).

#### Disposing a Savepoint

The `savepoint` action can be also used to remove savepoints. `--dispose` with the corresponding 
savepoint path needs to be added:
```bash
$ ./bin/flink savepoint \ 
      --dispose \
      /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \ 
      $JOB_ID
```
```
Disposing savepoint '/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab'.
Waiting for response...
Savepoint '/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab' disposed.
```

If you use custom state instances (for example custom reducing state or RocksDB state), you have to 
specify the path to the program JAR with which the savepoint was triggered. Otherwise, you will run 
into a `ClassNotFoundException`:
```bash
$ ./bin/flink savepoint \
      --dispose <savepointPath> \ 
      --jarfile <jarFile>
```

Triggering the savepoint disposal through the `savepoint` action does not only remove the data from 
the storage but makes Flink clean up the savepoint-related metadata as well.

### Terminating a Job

#### Stopping a Job Gracefully Creating a Final Savepoint

Another action for stopping a job is `stop`. It is a more graceful way of stopping a running streaming 
job as the `stop`  flows from source to sink. When the user requests to stop a job, all sources will 
be requested to send the last checkpoint barrier that will trigger a savepoint, and after the successful 
completion of that savepoint, they will finish by calling their `cancel()` method. 

```bash
$ ./bin/flink stop \
      --savepointPath /tmp/flink-savepoints \
      $JOB_ID
```
```
Suspending job "cca7bc1061d61cf15238e92312c2fc20" with a savepoint.
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
```
We have to use `--savepointPath` to specify the savepoint folder if 
[state.savepoints.dir]({{< ref "docs/deployment/config" >}}#state-savepoints-dir) isn't set.

If the `--drain` flag is specified, then a `MAX_WATERMARK` will be emitted before the last checkpoint 
barrier. This will make all registered event-time timers fire, thus flushing out any state that 
is waiting for a specific watermark, e.g. windows. The job will keep running until all sources properly 
shut down. This allows the job to finish processing all in-flight data, which can produce some
records to process after the savepoint taken while stopping.

{{< hint danger >}}
Use the `--drain` flag if you want to terminate the job permanently.
If you want to resume the job at a later point in time, then do not drain the pipeline because it could lead to incorrect results when the job is resumed.
{{< /hint >}}

#### Cancelling a Job Ungracefully

Cancelling a job can be achieved through the `cancel` action:
```bash
$ ./bin/flink cancel $JOB_ID
```
```
Cancelling job cca7bc1061d61cf15238e92312c2fc20.
Cancelled job cca7bc1061d61cf15238e92312c2fc20.
```
The corresponding job's state will be transitioned from `Running` to `Cancelled`. Any computations 
will be stopped.

{{< hint danger >}}
The `--withSavepoint` flag allows creating a savepoint as part of the job cancellation.
This feature is deprecated.
Use the [stop](#stopping-a-job-gracefully-creating-a-final-savepoint) action instead.
{{< /hint >}}

### Starting a Job from a Savepoint

Starting a job from a savepoint can be achieved using the `run` (and `run-application`) action.
```bash
$ ./bin/flink run \
      --detached \ 
      --fromSavepoint /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \
      ./examples/streaming/StateMachineExample.jar
```
```
Usage with built-in data generator: StateMachineExample [--error-rate <probability-of-invalid-transition>] [--sleep <sleep-per-record-in-ms>]
Usage with Kafka: StateMachineExample --kafka-topic <topic> [--brokers <brokers>]
Options for both the above setups:
        [--backend <file|rocks>]
        [--checkpoint-dir <filepath>]
        [--async-checkpoints <true|false>]
        [--incremental-checkpoints <true|false>]
        [--output <filepath> OR null for stdout]

Using standalone source with error rate 0.000000 and sleep delay 1 millis

Job has been submitted with JobID 97b20a0a8ffd5c1d656328b0cd6436a6
```

See how the command is equal to the [initial run command](#submitting-a-job) except for the 
`--fromSavepoint` parameter which is used to refer to the state of the 
[previously stopped job](#stopping-a-job-gracefully-creating-a-final-savepoint). A new JobID is 
generated that can be used to maintain the job.

By default, we try to match the whole savepoint state to the job being submitted. If you want to 
allow to skip savepoint state that cannot be restored with the new job you can set the 
`--allowNonRestoredState` flag. You need to allow this if you removed an operator from your program 
that was part of the program when the savepoint was triggered and you still want to use the savepoint.

```bash
$ ./bin/flink run \
      --fromSavepoint <savepointPath> \
      --allowNonRestoredState ...
```
This is useful if your program dropped an operator that was part of the savepoint.

{{< top >}}

## CLI Actions

Here's an overview of actions supported by Flink's CLI tool:
<table class="table table-bordered">
    <thead>
        <tr>
          <th class="text-left" style="width: 25%">Action</th>
          <th class="text-left" style="width: 50%">Purpose</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><code class="highlighter-rouge">run</code></td>
            <td>
                This action executes jobs. It requires at least the jar containing the job. Flink-
                or job-related arguments can be passed if necessary.
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">run-application</code></td>
            <td>
                This action executes jobs in <a href="{{< ref "docs/deployment/overview" >}}#application-mode">
                Application Mode</a>. Other than that, it requires the same parameters as the 
                <code class="highlighter-rouge">run</code> action.
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">info</code></td>
            <td>
                This action can be used to print an optimized execution graph of the passed job. Again,
                the jar containing the job needs to be passed.
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">list</code></td>
            <td>
                This action lists all running or scheduled jobs.
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">savepoint</code></td>
            <td>
                This action can be used to create or disposing savepoints for a given job. It might be
                necessary to specify a savepoint directory besides the JobID, if the 
                <a href="{{< ref "docs/deployment/config" >}}#state-savepoints-dir">state.savepoints.dir</a> 
                parameter was not specified in <code class="highlighter-rouge">conf/flink-conf.yaml</code>.
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">cancel</code></td>
            <td>
                This action can be used to cancel running jobs based on their JobID.
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">stop</code></td>
            <td>
                This action combines the <code class="highlighter-rouge">cancel</code> and 
                <code class="highlighter-rouge">savepoint</code> actions to stop a running job 
                but also create a savepoint to start from again.
            </td>
        </tr>
    </tbody>
</table>

A more fine-grained description of all actions and their parameters can be accessed through `bin/flink --help` 
or the usage information of each individual action `bin/flink <action> --help`.

{{< top >}}

## Advanced CLI
 
### REST API

The Flink cluster can be also managed using the [REST API]({{< ref "docs/ops/rest_api" >}}). The commands 
described in previous sections are a subset of what is offered by Flink's REST endpoints. Therefore, 
tools like `curl` can be used to get even more out of Flink.

### Selecting Deployment Targets

Flink is compatible with multiple cluster management frameworks like 
[Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}) or 
[YARN]({{< ref "docs/deployment/resource-providers/yarn" >}}) which are described in more detail in the 
Resource Provider section. Jobs can be submitted in different [Deployment Modes]({{< ref "docs/deployment/overview" >}}#deployment-modes). 
The parameterization of a job submission differs based on the underlying framework and Deployment Mode. 

`bin/flink` offers a parameter `--target` to handle the different options. In addition to that, jobs 
have to be submitted using either `run` (for [Session]({{< ref "docs/deployment/overview" >}}#session-mode) 
and [Per-Job Mode]({{< ref "docs/deployment/overview" >}}#per-job-mode)) or `run-application` (for 
[Application Mode]({{< ref "docs/deployment/overview" >}}#application-mode)). See the following summary of 
parameter combinations: 
* YARN
  * `./bin/flink run --target yarn-session`: Submission to an already running Flink on YARN cluster
  * `./bin/flink run --target yarn-per-job`: Submission spinning up a Flink on YARN cluster in Per-Job Mode
  * `./bin/flink run-application --target yarn-application`: Submission spinning up Flink on YARN cluster in Application Mode
* Kubernetes
  * `./bin/flink run --target kubernetes-session`: Submission to an already running Flink on Kubernetes cluster
  * `./bin/flink run-application --target kubernetes-application`: Submission spinning up a Flink on Kubernetes cluster in Application Mode
* Standalone:
  * `./bin/flink run --target local`: Local submission using a MiniCluster in Session Mode
  * `./bin/flink run --target remote`: Submission to an already running Flink cluster

The `--target` will overwrite the [execution.target]({{< ref "docs/deployment/config" >}}#execution-target) 
specified in the `conf/flink-conf.yaml`.

For more details on the commands and the available options, please refer to the Resource Provider-specific 
pages of the documentation.

### Submitting PyFlink Jobs

Currently, users are able to submit a PyFlink job via the CLI. It does not require to specify the
JAR file path or the entry main class, which is different from the Java job submission.

{{< hint info >}}
When submitting Python job via `flink run`, Flink will run the command "python". Please run the following command to confirm that the python executable in current environment points to a supported Python version of 3.6+.
{{< /hint >}}
```bash
$ python --version
# the version printed here must be 3.6+
```

The following commands show different PyFlink job submission use-cases:

- Run a PyFlink job:
```bash
$ ./bin/flink run --python examples/python/table/word_count.py
```

- Run a PyFlink job with additional source and resource files. Files specified in `--pyFiles` will be
added to the `PYTHONPATH` and, therefore, available in the Python code.
```bash
$ ./bin/flink run \
      --python examples/python/table/word_count.py \
      --pyFiles file:///user.txt,hdfs:///$namenode_address/username.txt
```

- Run a PyFlink job which will reference Java UDF or external connectors. JAR file specified in `--jarfile` will be uploaded
to the cluster.
```bash
$ ./bin/flink run \
      --python examples/python/table/word_count.py \
      --jarfile <jarFile>
```

- Run a PyFlink job with pyFiles and the main entry module specified in `--pyModule`:
```bash
$ ./bin/flink run \
      --pyModule table.word_count \
      --pyFiles examples/python/table
```

- Submit a PyFlink job on a specific JobManager running on host `<jobmanagerHost>` (adapt the command accordingly):
```bash
$ ./bin/flink run \
      --jobmanager <jobmanagerHost>:8081 \
      --python examples/python/table/word_count.py
```

- Run a PyFlink job using a [YARN cluster in Per-Job Mode]({{< ref "docs/deployment/resource-providers/yarn" >}}#per-job-cluster-mode):
```bash
$ ./bin/flink run \
      --target yarn-per-job
      --python examples/python/table/word_count.py
```

- Run a PyFlink application on a native Kubernetes cluster having the cluster ID `<ClusterId>`, it requires a docker image with PyFlink installed, please refer to [Enabling PyFlink in docker]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#enabling-python):
```bash
$ ./bin/flink run-application \
      --target kubernetes-application \
      --parallelism 8 \
      -Dkubernetes.cluster-id=<ClusterId> \
      -Dtaskmanager.memory.process.size=4096m \
      -Dkubernetes.taskmanager.cpu=2 \
      -Dtaskmanager.numberOfTaskSlots=4 \
      -Dkubernetes.container.image=<PyFlinkImageName> \
      --pyModule word_count \
      --pyFiles /opt/flink/examples/python/table/word_count.py
```

To learn more available options, please refer to [Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}})
or [YARN]({{< ref "docs/deployment/resource-providers/yarn" >}}) which are described in more detail in the
Resource Provider section.

Besides `--pyFiles`, `--pyModule` and `--python` mentioned above, there are also some other Python
related options. Here's an overview of all the Python related options for the actions
`run` and `run-application` supported by Flink's CLI tool:
<table class="table table-bordered">
    <thead>
        <tr>
          <th class="text-left" style="width: 25%">Option</th>
          <th class="text-left" style="width: 50%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><code class="highlighter-rouge">-py,--python</code></td>
            <td>
                Python script with the program entry. The dependent resources can be configured
                with the <code class="highlighter-rouge">--pyFiles</code> option.
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pym,--pyModule</code></td>
            <td>
                Python module with the program entry point.
                This option must be used in conjunction with <code class="highlighter-rouge">--pyFiles</code>.
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pyfs,--pyFiles</code></td>
            <td>
                Attach custom files for job. The standard resource file suffixes such as .py/.egg/.zip/.whl or directory are all supported.
                These files will be added to the PYTHONPATH of both the local client and the remote python UDF worker.
                Files suffixed with .zip will be extracted and added to PYTHONPATH.
                Comma (',') could be used as the separator to specify multiple files
                (e.g., --pyFiles file:///tmp/myresource.zip,hdfs:///$namenode_address/myresource2.zip).
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pyarch,--pyArchives</code></td>
            <td>
                Add python archive files for job. The archive files will be extracted to the working directory
                of python UDF worker. For each archive file, a target directory
                be specified. If the target directory name is specified, the archive file will be extracted to a
                directory with the specified name. Otherwise, the archive file will be extracted to a
                directory with the same name of the archive file. The files uploaded via this option are accessible
                via relative path. '#' could be used as the separator of the archive file path and the target directory
                name. Comma (',') could be used as the separator to specify multiple archive files.
                This option can be used to upload the virtual environment, the data files used in Python UDF
                (e.g., --pyArchives file:///tmp/py37.zip,file:///tmp/data.zip#data --pyExecutable
                py37.zip/py37/bin/python). The data files could be accessed in Python UDF, e.g.:
                f = open('data/data.txt', 'r').
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pyclientexec,--pyClientExecutable</code></td>
            <td>
                The path of the Python interpreter used to launch the Python process when submitting
                the Python jobs via \"flink run\" or compiling the Java/Scala jobs containing
                Python UDFs.
                (e.g., --pyArchives file:///tmp/py37.zip --pyClientExecutable py37.zip/py37/python)
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pyexec,--pyExecutable</code></td>
            <td>
                Specify the path of the python interpreter used to execute the python UDF worker
                (e.g.: --pyExecutable /usr/local/bin/python3).
                The python UDF worker depends on Python 3.6+, Apache Beam (version == 2.27.0),
                Pip (version >= 7.1.0) and SetupTools (version >= 37.0.0).
                Please ensure that the specified environment meets the above requirements.
            </td>
        </tr>
        <tr>
            <td><code class="highlighter-rouge">-pyreq,--pyRequirements</code></td>
            <td>
                Specify the requirements.txt file which defines the third-party dependencies.
                These dependencies will be installed and added to the PYTHONPATH of the python UDF worker.
                A directory which contains the installation packages of these dependencies could be specified
                optionally. Use '#' as the separator if the optional parameter exists
                (e.g., --pyRequirements file:///tmp/requirements.txt#file:///tmp/cached_dir).
            </td>
        </tr>
    </tbody>
</table>

In addition to the command line options during submitting the job, it also supports to specify the
dependencies via configuration or Python API inside the code. Please refer to the
[dependency management]({{< ref "docs/dev/python/dependency_management" >}}) for more details.

{{< top >}}
