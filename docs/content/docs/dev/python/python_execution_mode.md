---
title: "Execution Mode"
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
requirements of your use case and the characteristics of your job. The python runtime execution mode
will decide how to execute your customized python functions.

Before release-1.15, there is the only execution mode called `PROCESS` execution mode. The `PROCESS`
mode means that the Python user-defined functions will be executed in separate Python process.

In release-1.15, there are another two execution modes called `MULTI-THREAD` execution mode and
`SUB-INTERPRETER` execution mode. The `MULTI-THREAD` mode means that the Python user-defined functions
will be executed in the same thread as Java Operator, but it will be affected by GIL performance.
The `SUB-INTERPRETER` mode means that the Python user-defined functions will be executed in python
different sub-interpreters rather than different threads of one interpreter, which can largely overcome
the effects of the GIL, but it maybe fail in some CPython extensions libraries, such as numpy, tensorflow. 

## When can/should I use MULTI-THREAD execution mode or SUB-INTERPRETER execution mode?

The purpose of the introduction of `MULTI-THREAD` mode and `SUB-INTERPRETER` mode is to overcome the
overhead of serialization/deserialization and network communication caused in `PROCESS` mode.
So if performance is not your concern, or the computing logic of your customized python functions is
the performance bottleneck of the job, `PROCESS` mode will be the best choice as `PROCESS` mode provides
the best isolation compared to `MULTI-THREAD` mode and `SUB-INTERPRETER` mode.

Compared to `MULTI-THREAD` execution mode, `SUB-INTERPRETER` execution mode can largely overcome the
effects of the GIL, so it can get better performance. but `SUB-INTERPRETER` maybe fail in some CPython
extensions libraries, such as numpy, tensorflow.

## Configuring python execution mode

The execution mode can be configured via the `python.execution-mode` setting.
There are three possible values:

 - `PROCESS`: The Python user-defined functions will be executed in separate Python process. (default)
 - `MULTI-THREAD`: The Python user-defined functions will be executed in the same thread as Java Operator.
 - `SUB-INTERPRETER`: The Python user-defined functions will be executed in python different sub-interpreters.

You could specify the python execution mode using Python Table API as following:

```python
# Specify `PROCESS` mode
table_env.get_config().get_configuration().set_string("python.execution-mode", "process")

# Specify `MULTI-THREAD` mode
table_env.get_config().get_configuration().set_string("python.execution-mode", "multi-thread")

# Specify `SUB-INTERPRETER` mode
table_env.get_config().get_configuration().set_string("python.execution-mode", "sub-interpreter")
```

{{< hint info >}}
if the python operator dose not support `MULTI-THREAD` and `SUB-INTERPRETER` execution mode, we will still use `PROCESS` execution mode.
Currently, pyflink only support general python udf in `MULTI-THREAD` or `SUB-INTERPRETER` execution mode.
{{< /hint >}}

## Execution Behavior

This section provides an overview of the execution behavior of `MULTI-THREAD` and `SUB-INTERPRETER`
execution mode and contrasts they with `PROCESS` execution mode. For more
details, please refer to the FLIP that introduced this feature:
[FLIP-206](https://cwiki.apache.org/confluence/display/FLINK/FLIP-206%3A+Support+PyFlink+Runtime+Execution+in+Thread+Mode).

#### PROCESS Execution Mode

In `PROCESS` execution mode, the Python user-defined functions will be executed in separate Python Worker process.
The Java Operator Process communicates with the Python Worker Process using various Grpc Services.

{{< img src="/fig/pyflink_process_execution_mode.png" alt="Process Execution Mode" >}}

#### MULTI-THREAD and SUB-INTERPRETER Execution Mode

In `MULTI-THREAD` and `SUB-INTERPRETER` execution mode, the Python user-defined functions will be executed in
the same process as Java Operators. PyFlink takes use of third part library [PEMJA](https://github.com/alibaba/pemja) to
embed Python in Java Application.

{{< img src="/fig/pyflink_embedded_execution_mode.png" alt="Embedded Execution Mode" >}}
