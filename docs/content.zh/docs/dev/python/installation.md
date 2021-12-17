---
title: "环境安装"
weight: 16
type: docs
aliases:
  - /zh/dev/python/installation.html
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

# 环境安装



## 环境要求
<span class="label label-info">注意</span> PyFlink 需要 Python 3.6 以上版本（3.6, 3.7 或 3.8）。请运行以下命令，以确保 Python 版本满足要求。

```bash
$ python --version
# the version printed here must be 3.6, 3.7 or 3.8
```

## 环境设置

你的系统也许安装了好几个版本的 Python。你可以运行下面的 `ls` 命令来查看当前系统中安装的 Python 版本有哪些:

```bash
$ ls /usr/bin/python*
```

为了满足 Python 版本要求，你可以选择通过软链接的方式将` python` 指向 `python3` 解释器:

```bash
ln -s /usr/bin/python3 python
```

除了软链接的方式，你也可以选择创建一个 Python virtual env（`venv`）的方式。关于如何创建一个 virtual env，你可以参考[准备 Python 虚拟环境]({{< ref "docs/dev/python/faq" >}}#preparing-python-virtual-environment)。

如果你不想使用软链接的方式改变系统 Python 解释器的路径，你也可以通过配置的方式指定 Python 解释器。
你可以参考配置[python.client.executable]({{< ref "docs/dev/python/python_config" >}}#python-client-executable)，了解如何指定编译作业时所使用的 Python 解释器路径，
以及参考配置[python.executable]({{< ref "docs/dev/python/python_config" >}}#python-executable)，了解如何指定执行 Python UDF 时所使用的 Python 解释器路径。

## PyFlink 安装

PyFlink 已经被发布到[PyPi](https://pypi.org/project/apache-flink/)，可以通过如下方式安装 PyFlink：

{{< stable >}}
```bash
$ python -m pip install apache-flink=={{< version >}}
```
{{< /stable >}}
{{< unstable >}}
```bash
$ python -m pip install apache-flink
```
{{< /unstable >}}

你也可以从源码手动构建 PyFlink，具体可以参见[开发指南]({{< ref "docs/flinkDev/building" >}}#build-pyflink).

<span class="label label-info">注意</span> 从Flink 1.11版本开始, PyFlink 作业支持在 Windows 系统上运行，因此您也可以在 Windows 上开发和调试 PyFlink 作业了。
