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
defines how to execute your customized Python functions.

Prior to release-1.15, there is the only execution mode called `PROCESS` execution mode. The `PROCESS`
mode means that the Python user-defined functions will be executed in separate Python processes.

In release-1.15, it has introduced another two execution modes called `MULTI-THREAD` execution mode and
`SUB-INTERPRETER` execution mode. The `MULTI-THREAD` mode means that the Python user-defined functions
will be executed in the same thread as Java Operator, but it will be affected by GIL performance.
The `SUB-INTERPRETER` mode means that the Python user-defined functions will be executed in Python
different sub-interpreters rather than different threads of one interpreter, which can largely overcome
the effects of the GIL, but some CPython extensions libraries doesn't support it, such as numpy, tensorflow, etc.

## When can/should I use MULTI-THREAD execution mode or SUB-INTERPRETER execution mode?

The purpose of the introduction of `MULTI-THREAD` mode and `SUB-INTERPRETER` mode is to overcome the
overhead of serialization/deserialization and network communication caused in `PROCESS` mode.
So if performance is not your concern, or the computing logic of your customized Python functions is
the performance bottleneck of the job, `PROCESS` mode will be the best choice as `PROCESS` mode provides
the best isolation compared to `MULTI-THREAD` mode and `SUB-INTERPRETER` mode.

Compared to `MULTI-THREAD` execution mode, `SUB-INTERPRETER` execution mode can largely overcome the
effects of the GIL, so you can get better performance usually. However, `SUB-INTERPRETER` may fail in some CPython
extensions libraries, such as numpy, tensorflow. In this case, you should use `PROCESS` mode or `MULTI-THREAD` mode.

## Configuring Python execution mode

The execution mode can be configured via the `python.execution-mode` setting.
There are three possible values:

 - `PROCESS`: The Python user-defined functions will be executed in separate Python process. (default)
 - `MULTI-THREAD`: The Python user-defined functions will be executed in the same thread as Java Operator.
 - `SUB-INTERPRETER`: The Python user-defined functions will be executed in Python different sub-interpreters.

You could specify the Python execution mode using Python Table API as following:

```python
# Specify `PROCESS` mode
table_env.get_config().get_configuration().set_string("python.execution-mode", "process")

# Specify `MULTI-THREAD` mode
table_env.get_config().get_configuration().set_string("python.execution-mode", "multi-thread")

# Specify `SUB-INTERPRETER` mode
table_env.get_config().get_configuration().set_string("python.execution-mode", "sub-interpreter")
```

{{< hint info >}}
Currently, it still doesn't support to execute Python UDFs in `MULTI-THREAD` and `SUB-INTERPRETER` execution mode
in all places. It will fall back to `PROCESS` execution mode in these cases. So it may happen that you configure a job
to execute in `MULTI-THREAD` or `SUB-INTERPRETER` execution modes, however, it's actually executed in `PROCESS` execution mode.
{{< /hint >}}
{{< hint info >}}
`MULTI-THREAD` execution mode only supports Python 3.7+. `SUB-INTERPRETER` execution mode only supports Python 3.8+.  
{{< /hint >}}

## Execution Behavior

This section provides an overview of the execution behavior of `MULTI-THREAD` and `SUB-INTERPRETER`
execution mode and contrasts they with `PROCESS` execution mode. For more
details, please refer to the FLIP that introduced this feature:
[FLIP-206](https://cwiki.apache.org/confluence/display/FLINK/FLIP-206%3A+Support+PyFlink+Runtime+Execution+in+Thread+Mode).

#### PROCESS Execution Mode

In `PROCESS` execution mode, the Python user-defined functions will be executed in separate Python Worker process.
The Java operator process communicates with the Python worker process using various Grpc services.

{{< img src="/fig/pyflink_process_execution_mode.png" alt="Process Execution Mode" >}}

#### MULTI-THREAD and SUB-INTERPRETER Execution Mode

In `MULTI-THREAD` and `SUB-INTERPRETER` execution mode, the Python user-defined functions will be executed in
the same process as Java operators. PyFlink takes use of third part library [PEMJA](https://github.com/alibaba/pemja) to
embed Python in Java Application.

{{< img src="/fig/pyflink_embedded_execution_mode.png" alt="Embedded Execution Mode" >}}
