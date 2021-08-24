---
title: "调试"
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

# 调试

本页介绍如何在PyFlink进行调试

## 打印日志信息

### 客户端日志

你可以通过 `print` 或者标准的 Python logging 模块，在 PyFlink 作业中，Python UDF 之外的地方打印上下文和调试信息。
在提交作业时，日志信息会打印在客户端的日志文件中。

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# 创建 TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')])

# 使用 logging 模块
import logging
logging.warning(table.get_schema())

# 使用 print 函数
print(table.get_schema())
```

**注意:** 客户端缺省的日志级别是 `WARNING`，因此，只有日志级别在 `WARNING` 及以上的日志信息才会打印在客户端的日志文件中。

### 服务器端日志

你可以通过标准的 Python logging 模块，在 Python UDF 中打印上下文和调试信息。
在作业运行的过程中，日志信息会打印在 `TaskManager` 的日志文件中。

```python
@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.BIGINT())
def add(i, j):
    import logging
    logging.info("debug")
    return i + j
```

**注意:** 服务器端缺省的日志级别是 `INFO`，因此，只有日志级别在 `INFO` 及以上的日志信息才会打印在 `TaskManager` 的日志文件中。

## 查看日志

如果设置了环境变量`FLINK_HOME`，日志将会放置在`FLINK_HOME`指向目录的log目录之下。否则，日志将会放在安装的Pyflink模块的
log目录下。你可以通过执行下面的命令来查找PyFlink模块的log目录的路径：

```bash
$ python -c "import pyflink;import os;print(os.path.dirname(os.path.abspath(pyflink.__file__))+'/log')"
```

## 调试Python UDFs
你可以利用PyCharm提供的[`pydevd_pycharm`](https://pypi.org/project/pydevd-pycharm/)工具进行Python UDF的调试

1. 在PyCharm里创建一个Python Remote Debug

    run -> Python Remote Debug -> + -> 选择一个port (e.g. 6789)

2. 安装`pydevd-pycharm`工具

    ```bash
    $ pip install pydevd-pycharm
    ```

3. 在你的Python UDF里面添加如下的代码

    ```python
    import pydevd_pycharm
    pydevd_pycharm.settrace('localhost', port=6789, stdoutToServer=True, stderrToServer=True)
    ```

4. 启动刚刚创建的Python Remote Dubug Server

5. 运行你的Python代码
