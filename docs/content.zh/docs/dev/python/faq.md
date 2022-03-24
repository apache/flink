---
title: "常见问题"
weight: 141
type: docs
aliases:
  - /zh/dev/python/faq.html
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

# 常见问题

本页介绍了针对PyFlink用户的一些常见问题的解决方案。



## 准备Python虚拟环境

您可以下载[便捷脚本](/downloads/setup-pyflink-virtual-env.sh)，以准备可在Mac OS和大多数Linux发行版上使用的Python虚拟环境包(virtual env zip)。
您可以指定PyFlink的版本，来生成对应的PyFlink版本所需的Python虚拟环境，否则将安装最新版本的PyFlink所对应的Python虚拟环境。

{{< stable >}}
```bash 
$ sh setup-pyflink-virtual-env.sh {{< version >}}
```
{{< /stable >}}
{{< unstable >}}
```bash 
$ sh setup-pyflink-virtual-env.sh
```
{{< /unstable >}}

#### 集群（Cluster）

```shell
$ # 指定Python虚拟环境
$ table_env.add_python_archive("venv.zip")
$ # 指定用于执行python UDF workers (用户自定义函数工作者) 的python解释器的路径
$ table_env.get_config().set_python_executable("venv.zip/venv/bin/python")
```

如果需要了解`add_python_archive`和`set_python_executable`用法的详细信息，请参阅[相关文档]({{< ref "docs/dev/python/dependency_management" >}}#python-dependencies)。

## 添加Jar文件

PyFlink作业可能依赖jar文件，比如connector，Java UDF等。
您可以在提交作业时使用以下Python Table API或通过[命令行参数]({{< ref "docs/deployment/cli" >}}#usage)来指定依赖项。

```python
# 注意：仅支持本地文件URL（以"file:"开头）。
table_env.get_config().set("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")

# 注意：路径必须指定协议（例如：文件——"file"），并且用户应确保在客户端和群集上都可以访问这些URL。
table_env.get_config().set("pipeline.classpaths", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")
```

有关添加Java依赖项的API的详细信息，请参阅[相关文档]({{< ref "docs/dev/python/dependency_management" >}}#java-dependency-in-python-program)。

## 添加Python文件
您可以使用命令行参数`pyfs`或TableEnvironment的API `add_python_file`添加python文件依赖，这些依赖可以是python文件，python软件包或本地目录。
例如，如果您有一个名为`myDir`的目录，该目录具有以下层次结构：

```
myDir
├──utils
    ├──__init__.py
    ├──my_util.py
```

您可以将添加目录`myDir`添加到Python依赖中，如下所示：

```python
table_env.add_python_file('myDir')

def my_udf():
    from utils import my_util
```

## 当在 mini cluster 环境执行作业时，显式等待作业执行结束

当在 mini cluster 环境执行作业（比如，在IDE中执行作业）且在作业中使用了如下API（比如 Python Table API 的
TableEnvironment.execute_sql, StatementSet.execute 和 Python DataStream API 的 StreamExecutionEnvironment.execute_async）
的时候，因为这些API是异步的，请记得显式地等待作业执行结束。否则程序会在已提交的作业执行结束之前退出，以致无法观测到已提交作业的执行结果。
请参考如下示例代码，了解如何显式地等待作业执行结束：

```python
# 异步执行 SQL / Table API 作业
t_result = table_env.execute_sql(...)
t_result.wait()

# 异步执行 DataStream 作业
job_client = stream_execution_env.execute_async('My DataStream Job')
job_client.get_job_execution_result().result()
```

<strong>注意:</strong> 当往远程集群提交作业时，无需显式地等待作业执行结束，所以当往远程集群提交作业之前，请记得移除这些等待作业执行结束的代码逻辑。
