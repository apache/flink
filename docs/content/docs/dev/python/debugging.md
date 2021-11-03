---
title: "Debugging"
weight: 130
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

# Debugging

This page describes how to debug in PyFlink.

## Logging Infos

### Client Side Logging

You can log contextual and debug information via `print` or standard Python logging modules in
PyFlink jobs in places outside Python UDFs. The logging messages will be printed in the log files
of the client during job submission.

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')])

# use logging modules
import logging
logging.warning(table.get_schema())

# use print function
print(table.get_schema())
```

**Note:** The default logging level at client side is `WARNING` and so only messages with logging
level `WARNING` or above will appear in the log files of the client.

### Server Side Logging

You can log contextual and debug information via `print` or standard Python logging modules in Python UDFs. 
The logging messages will be printed in the log files of the `TaskManagers` during job execution.

```python
@udf(result_type=DataTypes.BIGINT())
def add(i, j):
    # use logging modules
    import logging
    logging.info("debug")
    # use print function
    print('debug')
    return i + j
```

**Note:** The default logging level at server side is `INFO` and so only messages with logging level `INFO` or above
will appear in the log files of the `TaskManagers`.

## Accessing Logs

If environment variable `FLINK_HOME` is set, logs will be written in the log directory under `FLINK_HOME`.
Otherwise, logs will be placed in the directory of the PyFlink module. You can execute the following command to find
the log directory of the PyFlink module:

```bash
$ python -c "import pyflink;import os;print(os.path.dirname(os.path.abspath(pyflink.__file__))+'/log')"
```

## Debugging Python UDFs

### Local Debug

You can debug your python functions directly in IDEs such as PyCharm.

### Remote Debug

You can make use of the [`pydevd_pycharm`](https://pypi.org/project/pydevd-pycharm/) tool of PyCharm to debug Python UDFs.

1. Create a Python Remote Debug in PyCharm

    run -> Python Remote Debug -> + -> choose a port (e.g. 6789)

2. Install the `pydevd-pycharm` tool

    ```bash
    $ pip install pydevd-pycharm
    ```

3. Add the following command in your Python UDF

    ```python
    import pydevd_pycharm
    pydevd_pycharm.settrace('localhost', port=6789, stdoutToServer=True, stderrToServer=True)
    ```

4. Start the previously created Python Remote Debug Server

5. Run your Python Code

## Profiling Python UDFs

You can enable the profile to analyze performance bottlenecks.

```python
t_env.get_config().get_configuration().set_boolean("python.profile.enabled", True)
``` 

Then you can see the profile result in [logs](#accessing-logs)
