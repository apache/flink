---
title: "配置"
weight: 121
type: docs
aliases:
  - /zh/dev/python/python_config.html
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

# 配置

Depending on the requirements of a Python API program, it might be necessary to adjust certain parameters for optimization.

For Python DataStream API program, the config options could be set as following:
```python
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.util.java_utils import get_j_env_configuration

env = StreamExecutionEnvironment.get_execution_environment()
config = Configuration(j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
config.set_integer("python.fn-execution.bundle.size", 1000)
```

For Python Table API program, all the config options available for Java/Scala Table API
program could also be used in the Python Table API program.
You could refer to the [Table API Configuration]({{< ref "docs/dev/table/config" >}}) for more details
on all the available config options for Table API programs.
The config options could be set as following in a Table API program:
```python
from pyflink.table import TableEnvironment, EnvironmentSettings

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

config = t_env.get_config().get_configuration()
config.set_integer("python.fn-execution.bundle.size", 1000)
```

## Python Options

{{< generated/python_configuration >}}
