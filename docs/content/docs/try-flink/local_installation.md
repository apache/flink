---
title: 'First steps'
weight: 1
type: docs
aliases:
  - /try-flink/local_installation.html
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

# First steps

<span style="font-size:larger;">Welcome to Flink! :)</span>

Flink is designed to process continuous streams of data at a lightning fast pace. This short guide
will show you how to download the latest stable version of Flink, install, and run it. You will 
also run an example Flink job and view it in the web UI. 


## Downloading Flink

{{< hint info >}}
__Note:__ Flink is also available as a [Docker image](https://hub.docker.com/_/flink).
{{< /hint >}}

Flink runs on all UNIX-like environments, i.e. Linux, Mac OS X, and Cygwin (for Windows). You need 
to have __Java 8 or 11__ installed. To check the Java version installed, type in your terminal: 

```bash
$ java -version
```

Next, [download the latest binary release]({{< downloads >}}) of Flink, 
then extract the archive: 

```bash
$ tar -xzf flink-*.tgz
```

## Browsing the project directory

Navigate to the extracted directory and list the contents by issuing:

```bash
$ cd flink-* && ls -l
```

You should see something like:

{{< img src="/fig/try-flink/projectdirectory.png" alt="project directory" >}}

For now, you may want to note that:
- __bin/__ directory contains the `flink` binary as well as several bash scripts that manage various jobs and tasks
- __conf/__ directory contains configuration files, including `flink-conf.yaml`
- __examples/__ directory contains sample applications that can be used as is with Flink


## Starting and stopping a local cluster

To start a local cluster, run the bash script that comes with Flink:

```bash
$ ./bin/start-cluster.sh
```

You should see an output like this:

{{< img src="/fig/try-flink/output.png" alt="output" >}}

Flink is now running as a background process. You can check its status with the following command:

```bash
$ ps aux | grep flink
```

You should be able to navigate to the web UI at [localhost:8081](http://localhost:8081) to view
the Flink dashboard and see that the cluster is up and running. 

To quickly stop the cluster and all running components, you can use the provided script:

```bash
$ ./bin/stop-cluster.sh
```

## Submitting a Flink job

Flink provides a CLI tool, __bin/flink__, that can run programs packaged as Java ARchives (JAR)
and control their execution. Submitting a [job]({{< ref "docs/concepts/glossary" >}}#ﬂink-job) means uploading the job’s JAR ﬁle and related dependencies to the running Flink cluster
and executing it.

Flink releases come with example jobs, which you can ﬁnd in the __examples/__ folder.

To deploy the example word count job to the running cluster, issue the following command:

```bash
$ ./bin/flink run examples/streaming/WordCount.jar
```

You can verify the output by viewing the logs:

```bash
$ tail log/flink-*-taskexecutor-*.out
```

Sample output:

```bash
  (nymph,1)
  (in,3)
  (thy,1)
  (orisons,1)
  (be,4)
  (all,2)
  (my,1)
  (sins,1)
  (remember,1)
  (d,4)
```

Additionally, you can check Flink's [web UI](http://localhost:8081) to monitor the status of the cluster and running job.

You can view the data flow plan for the execution:

{{< img src="/fig/try-flink/dataflowplan.png" alt="data flow plan" >}}

Here for the job execution, Flink has two operators. The ﬁrst is the source operator which reads data from the
collection source. The second operator is the transformation operator which aggregates counts of words. Learn
more about [DataStream operators]({{< ref "docs/dev/datastream/operators" >}}).

You can also look at the timeline of the job execution:

{{< img src="/fig/try-flink/timeline.png" alt="data flow timeline" >}}

You have successfully ran a [Flink application]({{< ref "docs/concepts/glossary" >}}#ﬂink-application)! Feel free to select any other JAR archive from the __examples/__
folder or deploy your own job!

# Summary

In this guide, you downloaded Flink, explored the project directory, started and stopped a local cluster, and submitted a sample Flink job!

To learn more about Flink fundamentals, check out the [concepts]({{< ref "docs/concepts" >}}) section. If you want to try something more hands-on, try one of the tutorials.
