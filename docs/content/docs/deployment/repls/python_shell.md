---
title: "Python REPL"
weight: 7
type: docs
aliases:
  - /deployment/repls/python_shell.html
  - /apis/python_shell.html
  - /ops/python_shell.html
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

# Python REPL

Flink comes with an integrated interactive Python Shell.
It can be used in a local setup as well as in a cluster setup.
See the [standalone resource provider page]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}) for more information about how to setup a local Flink.
You can also [build a local setup from source]({{< ref "docs/flinkDev/building" >}}).

<span class="label label-info">Note</span> The Python Shell will run the command “python”. Please refer to the Python Table API [installation guide]({{< ref "docs/dev/python/installation" >}}) on how to set up the Python execution environments.

To use the shell with an integrated Flink cluster, you can simply install PyFlink with PyPi and execute the shell directly:

```bash
# install PyFlink
$ python -m pip install apache-flink
# execute the shell
$ pyflink-shell.sh local
```

To run the shell on a cluster, please see the Setup section below.

## Usage

The shell only supports Table API currently.
The Table Environments are automatically prebound after startup. 
Use "bt_env" and "st_env" to access BatchTableEnvironment and StreamTableEnvironment respectively.

### Table API

The example below is a simple program in the Python shell:
{{< tabs "44fff8c0-41a8-4681-8e72-fb450e43c720" >}}
{{< tab "stream" >}}
```python
>>> import tempfile
>>> import os
>>> import shutil
>>> sink_path = tempfile.gettempdir() + '/streaming.csv'
>>> if os.path.exists(sink_path):
...     if os.path.isfile(sink_path):
...         os.remove(sink_path)
...     else:
...         shutil.rmtree(sink_path)
>>> s_env.set_parallelism(1)
>>> t = st_env.from_elements([(1, 'hi', 'hello'), (2, 'hi', 'hello')], ['a', 'b', 'c'])
>>> st_env.connect(FileSystem().path(sink_path))\
...     .with_format(OldCsv()
...         .field_delimiter(',')
...         .field("a", DataTypes.BIGINT())
...         .field("b", DataTypes.STRING())
...         .field("c", DataTypes.STRING()))\
...     .with_schema(Schema()
...         .field("a", DataTypes.BIGINT())
...         .field("b", DataTypes.STRING())
...         .field("c", DataTypes.STRING()))\
...     .create_temporary_table("stream_sink")
>>> t.select("a + 1, b, c")\
...     .execute_insert("stream_sink").wait()
>>> # If the job runs in local mode, you can exec following code in Python shell to see the result:
>>> with open(sink_path, 'r') as f:
...     print(f.read())
```
{{< /tab >}}
{{< tab "batch" >}}
```python
>>> import tempfile
>>> import os
>>> import shutil
>>> sink_path = tempfile.gettempdir() + '/batch.csv'
>>> if os.path.exists(sink_path):
...     if os.path.isfile(sink_path):
...         os.remove(sink_path)
...     else:
...         shutil.rmtree(sink_path)
>>> b_env.set_parallelism(1)
>>> t = bt_env.from_elements([(1, 'hi', 'hello'), (2, 'hi', 'hello')], ['a', 'b', 'c'])
>>> bt_env.connect(FileSystem().path(sink_path))\
...     .with_format(OldCsv()
...         .field_delimiter(',')
...         .field("a", DataTypes.BIGINT())
...         .field("b", DataTypes.STRING())
...         .field("c", DataTypes.STRING()))\
...     .with_schema(Schema()
...         .field("a", DataTypes.BIGINT())
...         .field("b", DataTypes.STRING())
...         .field("c", DataTypes.STRING()))\
...     .create_temporary_table("batch_sink")
>>> t.select("a + 1, b, c")\
...     .execute_insert("batch_sink").wait()
>>> # If the job runs in local mode, you can exec following code in Python shell to see the result:
>>> with open(sink_path, 'r') as f:
...     print(f.read())
```
{{< /tab >}}
{{< /tabs >}}

## Setup

To get an overview of what options the Python Shell provides, please use

```bash
pyflink-shell.sh --help
```

### Local

To use the shell with an integrated Flink cluster just execute:

```bash
pyflink-shell.sh local
```


### Remote

To use it with a running cluster, please start the Python shell with the keyword `remote`
and supply the host and port of the JobManager with:

```bash
pyflink-shell.sh remote <hostname> <portnumber>
```

### Yarn Python Shell cluster

The shell can deploy a Flink cluster to YARN, which is used exclusively by the
shell.
The shell deploys a new Flink cluster on YARN and connects the
cluster. You can also specify options for YARN cluster such as memory for
JobManager, name of YARN application, etc.

For example, to start a Yarn cluster for the Python Shell with two TaskManagers
use the following:

```bash
pyflink-shell.sh yarn -n 2
```

For all other options, see the full reference at the bottom.


### Yarn Session

If you have previously deployed a Flink cluster using the Flink Yarn Session,
the Python shell can connect with it using the following command:

```bash
pyflink-shell.sh yarn
```


## Full Reference

```bash
Flink Python Shell
Usage: pyflink-shell.sh [local|remote|yarn] [options] <args>...

Command: local [options]
Starts Flink Python shell with a local Flink cluster
usage:
     -h,--help   Show the help message with descriptions of all options.
Command: remote [options] <host> <port>
Starts Flink Python shell connecting to a remote cluster
  <host>
        Remote host name as string
  <port>
        Remote port as integer

usage:
     -h,--help   Show the help message with descriptions of all options.
Command: yarn [options]
Starts Flink Python shell connecting to a yarn cluster
usage:
     -h,--help                       Show the help message with descriptions of
                                     all options.
     -jm,--jobManagerMemory <arg>    Memory for JobManager Container with
                                     optional unit (default: MB)
     -nm,--name <arg>                Set a custom name for the application on
                                     YARN
     -qu,--queue <arg>               Specify YARN queue.
     -s,--slots <arg>                Number of slots per TaskManager
     -tm,--taskManagerMemory <arg>   Memory per TaskManager Container with
                                     optional unit (default: MB)
-h | --help
      Prints this usage text
```

{{< top >}}
