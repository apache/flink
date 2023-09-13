---
title: "Installation"
weight: 2
type: docs
aliases:
  - /dev/python/installation.html
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

# Installation

## Environment Requirements

{{< hint info >}}
Python version (3.8, 3.9 or 3.10) is required for PyFlink. Please run the following command to make sure that it meets the requirements:
{{< /hint >}}

```bash
$ python --version
# the version printed here must be 3.8, 3.9 or 3.10
```

## Environment Setup

Your system may include multiple Python versions, and thus also include multiple Python binary executables. You can run the following
`ls` command to find out what Python binary executables are available in your system:

```bash
$ ls /usr/bin/python*
```

To satisfy the PyFlink requirement regarding the Python environment version, you can choose to soft link `python` to point to your `python3` interpreter:

```bash
ln -s /usr/bin/python3 python
```

In addition to creating a soft link, you can also choose to create a Python virtual environment (`venv`). You can refer to the [Preparing Python Virtual Environment]({{< ref "docs/dev/python/faq" >}}#preparing-python-virtual-environment) documentation page for details on how to achieve that setup.

If you don’t want to use a soft link to change the system's `python` interpreter point to, you can use the configuration way to specify the Python interpreter.
For specifying the Python interpreter used to compile the jobs, you can refer to the configuration [python.client.executable]({{< ref "docs/dev/python/python_config" >}}#python-client-executable).
For specifying the Python interpreter used to execute the Python UDF, you can refer to the configuration [python.executable]({{< ref "docs/dev/python/python_config" >}}#python-executable).

## Installation of PyFlink

PyFlink is available in [PyPi](https://pypi.org/project/apache-flink/) and can be installed as follows:

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

You can also build PyFlink from source by following the [development guide]({{< ref "docs/flinkDev/building" >}}#build-pyflink).

<span class="label label-info">Note</span> Starting from Flink 1.11, it’s also supported to run
PyFlink jobs locally on Windows and so you could develop and debug PyFlink jobs on Windows.
