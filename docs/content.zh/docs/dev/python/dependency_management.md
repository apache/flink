---
title: "依赖管理"
weight: 30
type: docs
aliases:
  - /zh/dev/python/table-api-users-guide/dependency_management.html
  - /zh/dev/python/datastream-api-users-guide/dependency_management.html
  - /zh/dev/python/dependency_management.html
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

# Dependency Management

There are requirements to use dependencies inside the Python API programs. For example, users
may need to use third-party Python libraries in Python user-defined functions.
In addition, in scenarios such as machine learning prediction, users may want to load a machine
learning model inside the Python user-defined functions.

When the PyFlink job is executed locally, users could install the third-party Python libraries into
the local Python environment, download the machine learning model to local, etc.
However, this approach doesn't work well when users want to submit the PyFlink jobs to remote clusters.
In the following sections, we will introduce the options provided in PyFlink for these requirements.

<span class="label label-info">Note</span> Both Python DataStream API and Python Table API have provided
APIs for each kind of dependency. If you are mixing use of Python DataStream API and Python Table API
in a single job, you should specify the dependencies via Python DataStream API to make them work for
both the Python DataStream API and Python Table API.

## JAR Dependencies

If third-party JARs are used, you can specify the JARs in the Python Table API as following:

```python
# Specify a list of jar URLs via "pipeline.jars". The jars are separated by ";"
# and will be uploaded to the cluster.
# NOTE: Only local file URLs (start with "file://") are supported.
table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")

# Specify a list of URLs via "pipeline.classpaths". The URLs are separated by ";" 
# and will be added to the classpath during job execution.
# NOTE: The paths must specify a protocol (e.g. file://) and users should ensure that the URLs are accessible on both the client and the cluster.
table_env.get_config().get_configuration().set_string("pipeline.classpaths", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")
```

or in the Python DataStream API as following:

```python
# Use the add_jars() to add local jars and the jars will be uploaded to the cluster.
# NOTE: Only local file URLs (start with "file://") are supported.
stream_execution_environment.add_jars("file:///my/jar/path/connector1.jar", "file:///my/jar/path/connector2.jar")

# Use the add_classpaths() to add the dependent jars URLs into the classpath.
# The URLs will also be added to the classpath of both the client and the cluster.
# NOTE: The paths must specify a protocol (e.g. file://) and users should ensure that the 
# URLs are accessible on both the client and the cluster.
stream_execution_environment.add_classpaths("file:///my/jar/path/connector1.jar", "file:///my/jar/path/connector2.jar")
```

or through the [command line arguments]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs) `--jarfile` when submitting the job.

<span class="label label-info">Note</span> It only supports to specify one jar file with the command
line argument `--jarfile` and so you need to build a fat jar if there are multiple jar files.

## Python Dependencies

### Python libraries

You may want to use third-part Python libraries in Python user-defined functions.
There are multiple ways to specify the Python libraries.

You could specify them inside the code using Python Table API as following:

```python
table_env.add_python_file(file_path)
```

or using Python DataStream API as following:

```python
stream_execution_environment.add_python_file(file_path)
```

You could also specify the Python libraries using configuration
[`python.files`]({{< ref "docs/dev/python/python_config" >}}#python-files)
or via [command line arguments]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs) `-pyfs` or `--pyFiles`
when submitting the job.

<span class="label label-info">Note</span> The Python libraries could be local files or
local directories. They will be added to the PYTHONPATH of the Python UDF worker.

### requirements.txt

It also allows to specify a `requirements.txt` file which defines the third-party Python dependencies.
These Python dependencies will be installed into the working directory and added to the PYTHONPATH of
the Python UDF worker.

You could prepare the `requirements.txt` manually as following:

```shell
echo numpy==1.16.5 >> requirements.txt
echo pandas==1.0.0 >> requirements.txt
```

or using `pip freeze` which lists all the packages installed in the current Python environment:

```shell
pip freeze > requirements.txt
```

The content of the requirements.txt file may look like the following:

```shell
numpy==1.16.5
pandas==1.0.0
```

You could manually edit it by removing unnecessary entries or adding extra entries, etc.

The `requirements.txt` file could then be specified inside the code using Python Table API as following:

```python
# requirements_cache_dir is optional
table_env.set_python_requirements(
    requirements_file_path="/path/to/requirements.txt",
    requirements_cache_dir="cached_dir")
```

or using Python DataStream API as following:

```python
# requirements_cache_dir is optional
stream_execution_environment.set_python_requirements(
    requirements_file_path="/path/to/requirements.txt",
    requirements_cache_dir="cached_dir")
```

<span class="label label-info">Note</span> For the dependencies which could not be accessed in
the cluster, a directory which contains the installation packages of these dependencies could be
specified using the parameter `requirements_cached_dir`. It will be uploaded to the cluster to
support offline installation. You could prepare the `requirements_cache_dir` as following:

```shell
pip download -d cached_dir -r requirements.txt --no-binary :all:
```

<span class="label label-info">Note</span> Please make sure that the prepared packages match
the platform of the cluster, and the Python version used.

You could also specify the `requirements.txt` file using configuration
[`python.requirements`]({{< ref "docs/dev/python/python_config" >}}#python-requirements)
or via [command line arguments]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs)
`-pyreq` or `--pyRequirements` when submitting the job.

<span class="label label-info">Note</span> It will install the packages specified in the
`requirements.txt` file using pip, so please make sure that pip (version >= 7.1.0)
and setuptools (version >= 37.0.0) are available.

### Archives

You may also want to specify archive files. The archive files could be used to specify custom
Python virtual environments, data files, etc.

You could specify the archive files inside the code using Python Table API as following:

```python
table_env.add_python_archive(archive_path="/path/to/archive_file", target_dir=None)
```

or using Python DataStream API as following:

```python
stream_execution_environment.add_python_archive(archive_path="/path/to/archive_file", target_dir=None)
```

<span class="label label-info">Note</span> The parameter `target_dir` is optional. If specified,
the archive file will be extracted to a directory with the specified name of `target_dir` during execution.
Otherwise, the archive file will be extracted to a directory with the same name as the archive file.

Suppose you have specified the archive file as following:

```python
table_env.add_python_archive("/path/to/py_env.zip", "myenv")
```

Then, you could access the content of the archive file in Python user-defined functions as following:

```python
def my_udf():
    with open("myenv/py_env/data/data.txt") as f:
        ...
```

If you have not specified the parameter `target_dir`:

```python
table_env.add_python_archive("/path/to/py_env.zip")
```

You could then access the content of the archive file in Python user-defined functions as following:

```python
def my_udf():
    with open("py_env.zip/py_env/data/data.txt") as f:
        ...
```

<span class="label label-info">Note</span> The archive file will be extracted to the working
directory of Python UDF worker and so you could access the files inside the archive file using
relative path.

You could also specify the archive files using configuration
[`python.archives`]({{< ref "docs/dev/python/python_config" >}}#python-archives)
or via [command line arguments]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs)
`-pyarch` or `--pyArchives` when submitting the job.

<span class="label label-info">Note</span> If the archive file contains a Python virtual environment,
please make sure that the Python virtual environment matches the platform that the cluster is running on.

<span class="label label-info">Note</span> Currently, only zip-format is supported, i.e. zip, jar, whl, egg, etc.

### Python interpreter

It supports to specify the path of the Python interpreter to execute Python worker.

You could specify the Python interpreter inside the code using Python Table API as following:

```python
table_env.get_config().set_python_executable("/path/to/python")
```

or using Python DataStream API as following:

```python
stream_execution_environment.set_python_executable("/path/to/python")
```

It also supports to use the Python interpreter inside an archive file.

```python
# Python Table API
table_env.add_python_archive("/path/to/py_env.zip", "venv")
table_env.get_config().set_python_executable("venv/py_env/bin/python")

# Python DataStream API
stream_execution_environment.add_python_archive("/path/to/py_env.zip", "venv")
stream_execution_environment.set_python_executable("venv/py_env/bin/python")
```

You could also specify the Python interpreter using configuration
[`python.executable`]({{< ref "docs/dev/python/python_config" >}}#python-executable)
or via [command line arguments]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs)
`-pyexec` or `--pyExecutable` when submitting the job.

<span class="label label-info">Note</span> If the path of the Python interpreter refers to the
Python archive file, relative path should be used instead of absolute path.

### Python interpreter of client

Python is needed at the client side to parse the Python user-defined functions during
compiling the job.

You could specify the custom Python interpreter used at the client side by activating
it in the current session.

```shell
source my_env/bin/activate
```

or specify it using configuration
[`python.client.executable`]({{< ref "docs/dev/python/python_config" >}}#python-client-executable)
or environment variable [PYFLINK_CLIENT_EXECUTABLE]({{< ref "docs/dev/python/environment_variables" >}})

## How to specify Python Dependencies in Java/Scala Program

It also supports to use Python user-defined functions in the Java Table API programs or pure SQL programs.
The following code shows a simple example on how to use the Python user-defined functions in a
Java Table API program:

```java
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

TableEnvironment tEnv = TableEnvironment.create(
    EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build());
tEnv.getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);

// register the Python UDF
tEnv.executeSql("create temporary system function add_one as 'add_one.add_one' language python");

tEnv.createTemporaryView("source", tEnv.fromValues(1L, 2L, 3L).as("a"));

// use Python UDF in the Java Table API program
tEnv.executeSql("select add_one(a) as a from source").collect();
```

You can refer to the SQL statement about [CREATE FUNCTION]({{< ref "docs/dev/table/sql/create" >}}#create-function)
for more details on how to create Python user-defined functions using SQL statements.

The Python dependencies could then be specified via the Python [config options]({{< ref "docs/dev/python/python_config" >}}#python-options),
such as **python.archives**, **python.files**, **python.requirements**, **python.client.executable**,
**python.executable**. etc or through [command line arguments]({{< ref "docs/deployment/cli" >}}#usage)
when submitting the job.
