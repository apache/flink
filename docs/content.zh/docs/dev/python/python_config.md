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

config = Configuration()
config.set_integer("python.fn-execution.bundle.size", 1000)
env = StreamExecutionEnvironment.get_execution_environment(config)
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
t_env.get_config().set("python.fn-execution.bundle.size", "1000")
```

The config options could also be set when creating EnvironmentSettings:
```python
from pyflink.common import Configuration
from pyflink.table import TableEnvironment, EnvironmentSettings

# create a streaming TableEnvironment
config = Configuration()
config.set_string("python.fn-execution.bundle.size", "1000")
env_settings = EnvironmentSettings \
    .new_instance() \
    .in_streaming_mode() \
    .with_configuration(config) \
    .build()
table_env = TableEnvironment.create(env_settings)

# or directly pass config into create method
table_env = TableEnvironment.create(config)
```

## Python Options

{{< generated/python_configuration >}}
