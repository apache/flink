---
title:  "命令行界面"
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

Flink provides a Command-Line Interface (CLI) `bin/flink` to run programs that 
are packaged as JAR files and to control their execution. The CLI is part of any 
Flink setup, available in local single node setups and in distributed setups. 
It connects to the running JobManager specified in `conf/flink-config.yaml`.

* This will be replaced by the TOC
{:toc}

## Job Lifecycle Management

A prerequisite for the commands listed in this section to work is to have a running Flink deployment 
like [Kubernetes]({% link deployment/resource-providers/native_kubernetes.zh.md %}), 
[YARN]({% link deployment/resource-providers/yarn.zh.md %}) or any other option available. Feel free to 
[start a Flink cluster locally]({% link deployment/resource-providers/standalone/index.zh.md %}#starting-a-standalone-cluster-session-mode) 
to try the commands on your own machine.
 
### Submitting a Job

Submitting a job means uploading the job's JAR and related dependencies to the Flink cluster and 
initiating the job execution. For the sake of this example, we select a long-running job like 
`examples/streaming/StateMachineExample.jar`. Feel free to select any other JAR archive from the 
`examples/` folder or deploy your own job.
{% highlight bash %}
$ ./bin/flink run \
      --detached \
      ./examples/streaming/StateMachineExample.jar
{% endhighlight %}
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
{% highlight bash %}
$ export JOB_ID="cca7bc1061d61cf15238e92312c2fc20"
{% endhighlight %}

There is another action called `run-application` available to run the job in 
[Application Mode]({% link deployment/index.zh.md %}#application-mode). This documentation does not address
this action individually as it works similarly to the `run` action in terms of the CLI frontend.

### Job Monitoring

You can monitor any running jobs using the `list` action:
{% highlight bash %}
$ ./bin/flink list
{% endhighlight %}
```
Waiting for response...
------------------ Running/Restarting Jobs -------------------
30.11.2020 16:02:29 : cca7bc1061d61cf15238e92312c2fc20 : State machine job (RUNNING)
--------------------------------------------------------------
No scheduled jobs.
```
Jobs that were submitted but not started, yet, would be listed under "Scheduled Jobs".

### Creating a Savepoint

[Savepoints]({% link ops/state/savepoints.zh.md %}) can be created to save the current state a job is 
in. All that's needed is the JobID:
{% highlight bash %}
$ ./bin/flink savepoint \
      $JOB_ID \ 
      /tmp/flink-savepoints
{% endhighlight %}
```
Triggering savepoint for job cca7bc1061d61cf15238e92312c2fc20.
Waiting for response...
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
You can resume your program from this savepoint with the run command.
```
The savepoint folder is optional and needs to be specified if 
[state.savepoints.dir]({% link deployment/config.zh.md %}#state-savepoints-dir) isn't set.

The path to the savepoint can be used later on to [restart the Flink job](#starting-a-job-from-a-savepoint).

#### Disposing a Savepoint

The `savepoint` action can be also used to remove savepoints. `--dispose` with the corresponding 
savepoint path needs to be added:
{% highlight bash %}
$ ./bin/flink savepoint \ 
      --dispose \
      /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \ 
      $JOB_ID
{% endhighlight %}
```
Disposing savepoint '/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab'.
Waiting for response...
Savepoint '/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab' disposed.
```

If you use custom state instances (for example custom reducing state or RocksDB state), you have to 
specify the path to the program JAR with which the savepoint was triggered. Otherwise, you will run 
into a `ClassNotFoundException`:
{% highlight bash %}
$ ./bin/flink savepoint \
      --dispose <savepointPath> \ 
      --jarfile <jarFile>
{% endhighlight %}

Triggering the savepoint disposal through the `savepoint` action does not only remove the data from 
the storage but makes Flink clean up the savepoint-related metadata as well.

### Terminating a Job

#### Stopping a Job Gracefully Creating a Final Savepoint

Another action for stopping a job is `stop`. It is a more graceful way of stopping a running streaming 
job as the `stop`  flows from source to sink. When the user requests to stop a job, all sources will 
be requested to send the last checkpoint barrier that will trigger a savepoint, and after the successful 
completion of that savepoint, they will finish by calling their `cancel()` method. 

{% highlight bash %}
$ ./bin/flink stop \
      --savepointPath /tmp-flink-savepoints \
      $JOB_ID
{% endhighlight %}
```
Suspending job "cca7bc1061d61cf15238e92312c2fc20" with a savepoint.
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
```
We have to use `--savepointPath` to specify the savepoint folder if 
[state.savepoints.dir]({% link deployment/config.zh.md %}#state-savepoints-dir) isn't set.

If the `--drain` flag is specified, then a `MAX_WATERMARK` will be emitted before the last checkpoint 
barrier. This will make all registered event-time timers fire, thus flushing out any state that 
is waiting for a specific watermark, e.g. windows. The job will keep running until all sources properly 
shut down. This allows the job to finish processing all in-flight data.

#### Cancelling a Job Ungracefully

Cancelling a job can be achieved through the `cancel` action:
{% highlight bash %}
$ ./bin/flink cancel $JOB_ID
{% endhighlight %}
```
Cancelling job cca7bc1061d61cf15238e92312c2fc20.
Cancelled job cca7bc1061d61cf15238e92312c2fc20.
```
The corresponding job's state will be transitioned from `Running` to `Cancelled`. Any computations 
will be stopped.

<p style="border-radius: 5px; padding: 5px" class="bg-danger">
    <b>Note</b>: The <code class="highlighter-rouge">--withSavepoint</code> flag allows creating a 
    savepoint as part of the job cancellation. This feature is deprecated. Use the 
    <a href="#stopping-a-job-gracefully-creating-a-final-savepoint">stop</a> action instead.
</p>

### Starting a Job from a Savepoint

Starting a job from a savepoint can be achieved using the `run` (and `run-application`) action.
{% highlight bash %}
$ ./bin/flink run \
      --detached \ 
      --fromSavepoint /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \
      ./examples/streaming/StateMachineExample.jar
{% endhighlight %}
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

{% highlight bash %}
$ ./bin/flink run \
      --fromSavepoint <savepointPath> \
      --allowNonRestoredState ...
{% endhighlight %}
This is useful if your program dropped an operator that was part of the savepoint.

{% top %}

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
                This action executes jobs in <a href="{% link deployment/index.zh.md %}#application-mode">
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
                <a href="{% link deployment/config.zh.md %}#state-savepoints-dir">state.savepoints.dir</a> 
                parameter was not specified in <code class="highlighter-rouge">conf/flink-config.yaml</code>.
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

{% top %}

## Advanced CLI
 
### REST API

The Flink cluster can be also managed using the [REST API]({% link ops/rest_api.zh.md %}). The commands 
described in previous sections are a subset of what is offered by Flink's REST endpoints. Therefore, 
tools like `curl` can be used to get even more out of Flink.

### Selecting Deployment Targets

Flink is compatible with multiple cluster management frameworks like 
[Kubernetes]({% link deployment/resource-providers/native_kubernetes.zh.md %}) or 
[YARN]({% link deployment/resource-providers/yarn.zh.md %}) which are described in more detail in the 
Resource Provider section. Jobs can be submitted in different [Deployment Modes]({% link deployment/index.zh.md %}#deployment-modes). 
The parameterization of a job submission differs based on the underlying framework and Deployment Mode. 

`bin/flink` offers a parameter `--target` to handle the different options. In addition to that, jobs 
have to be submitted using either `run` (for [Session]({% link deployment/index.zh.md %}#session-mode) 
and [Per-Job Mode]({% link deployment/index.zh.md %}#per-job-mode)) or `run-application` (for 
[Application Mode]({% link deployment/index.zh.md %}#application-mode)). See the following summary of 
parameter combinations: 
* YARN
  * `./bin/flink run --target yarn-session`: Submission to an already running Flink on YARN cluster
  * `./bin/flink run --target yarn-per-job`: Submission spinning up a Flink on YARN cluster in Per-Job Mode
  * `./bin/flink run-application --target yarn-application`: Submission spinning up Flink on YARN cluster in Application Mode
* Kubernetes
  * `./bin/flink run --target kubernetes-session`: Submission to an already running Flink on Kubernetes cluster
  * `./bin/flink run-application --target kubernetes-application`: Submission spinning up a Flink on Kubernetes cluster in Application Mode
* Mesos
  * `./bin/flink run --target remote`: Submission to an already running Flink on Mesos cluster
* Standalone:
  * `./bin/flink run --target local`: Local submission using a MiniCluster in Session Mode
  * `./bin/flink run --target remote`: Submission to an already running Flink cluster

The `--target` will overwrite the [execution.target]({% link deployment/config.zh.md %}#execution-target) 
specified in the `config/flink-config.yaml`.

For more details on the commands and the available options, please refer to the Resource Provider-specific 
pages of the documentation.

{% top %}
