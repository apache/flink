---
title: "执行模式"
weight: 40
type: docs
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

# Execution Mode

The Python API supports different runtime execution modes from which you can choose depending on the
requirements of your use case and the characteristics of your job. The Python runtime execution mode
defines how the Python user-defined functions will be executed.

Prior to release-1.15, there is the only execution mode called `PROCESS` execution mode. The `PROCESS`
mode means that the Python user-defined functions will be executed in separate Python processes.

In release-1.15, it has introduced a new execution mode called `THREAD` execution mode. The `THREAD`
mode means that the Python user-defined functions will be executed in JVM.

**NOTE:** Multiple Python user-defined functions running in the same JVM are still affected by GIL.

## When can/should I use THREAD execution mode?

The purpose of the introduction of `THREAD` mode is to overcome the overhead of serialization/deserialization
and network communication introduced of inter-process communication in the `PROCESS` mode.
So if performance is not your concern, or the computing logic of your Python user-defined functions is the performance bottleneck of the job,
`PROCESS` mode will be the best choice as `PROCESS` mode provides the best isolation compared to `THREAD` mode.

## Configuring Python execution mode

The execution mode can be configured via the `python.execution-mode` setting.
There are two possible values:

 - `PROCESS`: The Python user-defined functions will be executed in separate Python process. (default)
 - `THREAD`: The Python user-defined functions will be executed in JVM.

You could specify the execution mode in Python Table API or Python DataStream API jobs as following:

```python
## Python Table API
# Specify `PROCESS` mode
table_env.get_config().set("python.execution-mode", "process")

# Specify `THREAD` mode
table_env.get_config().set("python.execution-mode", "thread")


## Python DataStream API

config = Configuration()

# Specify `PROCESS` mode
config.set_string("python.execution-mode", "process")

# Specify `THREAD` mode
config.set_string("python.execution-mode", "thread")

# Create the corresponding StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment(config)
```

## Supported Cases

### Python Table API

The following table shows where the `THREAD` execution mode is supported in Python Table API.

| UDFs | `PROCESS` | `THREAD`|
|-----|-----------|---------|
| Python UDF | Yes | Yes |
| Python UDTF | Yes | Yes |
| Python UDAF | Yes | No  |
| Pandas UDF & Pandas UDAF | Yes | No  |

### Python DataStream API

The following Table shows the supported cases in Python DataStream API.

| Operators | `PROCESS` | `THREAD` |
|-----------|-----------|----------|
| Map | Yes | Yes |
| FlatMap | Yes | Yes |
| Filter | Yes | Yes |
| Reduce | Yes | Yes |
| Union | Yes | Yes |
| Connect | Yes | Yes |
| CoMap | Yes | Yes |
| CoFlatMap | Yes | Yes |
| Process Function | Yes | Yes |
| Window Apply | Yes | Yes |
| Window Aggregate | Yes | Yes |
| Window Reduce | Yes | Yes |
| Window Process | Yes | Yes |
| Side Output | Yes | Yes |
| State | Yes | Yes |
| Iterate | No | No |
| Window CoGroup | No  | No  |
| Window Join | No  | No  |
| Interval Join | No  | No  |
| Async I/O | No  | No  |

{{< hint info >}}
Currently, it still doesn't support to execute Python UDFs in `THREAD` execution mode in all places.
It will fall back to `PROCESS` execution mode in these cases. So it may happen that you configure a job
to execute in `THREAD` execution mode, however, it's actually executed in `PROCESS` execution mode.
{{< /hint >}}
{{< hint info >}}
`THREAD` execution mode is only supported in Python 3.8+.
{{< /hint >}}

## Execution Behavior

This section provides an overview of the execution behavior of `THREAD` execution mode and contrasts
they with `PROCESS` execution mode. For more details, please refer to the FLIP that introduced this feature:
[FLIP-206](https://cwiki.apache.org/confluence/display/FLINK/FLIP-206%3A+Support+PyFlink+Runtime+Execution+in+Thread+Mode).

#### PROCESS Execution Mode

In `PROCESS` execution mode, the Python user-defined functions will be executed in separate Python Worker process.
The Java operator process communicates with the Python worker process using various Grpc services.

{{< img src="/fig/pyflink_process_execution_mode.png" alt="Process Execution Mode" >}}

#### THREAD Execution Mode

In `THREAD` execution mode, the Python user-defined functions will be executed in the same process
as Java operators. PyFlink takes use of third part library [PEMJA](https://github.com/alibaba/pemja)
to embed Python in Java Application.

{{< img src="/fig/pyflink_embedded_execution_mode.png" alt="Embedded Execution Mode" >}}
