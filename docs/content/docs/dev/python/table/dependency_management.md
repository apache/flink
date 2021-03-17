---
title: "Dependency Management"
weight: 46
type: docs
aliases:
  - /dev/python/table-api-users-guide/dependency_management.html
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

# Java Dependency in Python Program

If third-party Java dependencies are used, you can specify the dependencies with the following Python Table APIs or through [command line arguments]({{< ref "docs/deployment/cli" >}}#usage) directly when submitting the job.

```python
# Specify a list of jar URLs via "pipeline.jars". The jars are separated by ";" 
#and will be uploaded to the cluster.
# NOTE: Only local file URLs (start with "file://") are supported.
table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")

# Specify a list of URLs via "pipeline.classpaths". The URLs are separated by ";" 
# and will be added to the classpath of the cluster.
# NOTE: The Paths must specify a protocol (e.g. file://) and users should ensure that the URLs are accessible on both the client and the cluster.
table_env.get_config().get_configuration().set_string("pipeline.classpaths", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")
```

# Python Dependency in Python Program

If third-party Python dependencies are used, you can specify the dependencies with the following Python Table APIs or through [command line arguments]({{< ref "docs/deployment/cli" >}}#usage) directly when submitting the job.

#### add_python_file(file_path)

Adds python file dependencies which could be python files, python packages or local directories. They will be added to the PYTHONPATH of the python UDF worker.

```python
table_env.add_python_file(file_path)
```

#### set_python_requirements(requirements_file_path, requirements_cache_dir=None)

Specifies a requirements.txt file which defines the third-party dependencies. These dependencies will be installed to a temporary directory and added to the PYTHONPATH of the python UDF worker. For the dependencies which could not be accessed in the cluster, a directory which contains the installation packages of these dependencies could be specified using the parameter "requirements_cached_dir". It will be uploaded to the cluster to support offline installation.

```python
# commands executed in shell
echo numpy==1.16.5 > requirements.txt
pip download -d cached_dir -r requirements.txt --no-binary :all:

# python code
table_env.set_python_requirements("/path/to/requirements.txt", "cached_dir")
```

Please make sure the installation packages matches the platform of the cluster and the python version used. These packages will be installed using pip, so also make sure the version of Pip (version >= 7.1.0) and the version of SetupTools (version >= 37.0.0).

#### add_python_archive(archive_path, target_dir=None)

Adds a python archive file dependency. The file will be extracted to the working directory of python UDF worker. If the parameter "target_dir" is specified, the archive file will be extracted to a directory named "target_dir". Otherwise, the archive file will be extracted to a directory with the same name of the archive file.

```python
# command executed in shell
# assert the relative path of python interpreter is py_env/bin/python
zip -r py_env.zip py_env

# python code
table_env.add_python_archive("/path/to/py_env.zip")
# or
table_env.add_python_archive("/path/to/py_env.zip", "myenv")

# the files contained in the archive file can be accessed in UDF
def my_udf():
    with open("myenv/py_env/data/data.txt") as f:
        ...
```

Please make sure the uploaded python environment matches the platform that the cluster is running on. Currently only zip-format is supported. i.e. zip, jar, whl, egg, etc.

#### set_python_executable(python_exec)

Sets the path of the python interpreter which is used to execute the python udf workers, e.g., "/usr/local/bin/python3".

```python
table_env.add_python_archive("/path/to/py_env.zip")
table_env.get_config().set_python_executable("py_env.zip/py_env/bin/python")
```

Please note that if the path of the python interpreter comes from the uploaded python archive, the path specified in set_python_executable should be a relative path.

Please make sure that the specified environment matches the platform that the cluster is running on.

# Python Dependency in Java/Scala Program

It also supports to use Python UDFs in the Java Table API programs or pure SQL programs. The following example shows how to
use the Python UDFs in a Java Table API program:

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

You can refer to the SQL statement about [CREATE FUNCTION]({{< ref "docs/dev/table/sql/create" >}}#create-function) for more details
on how to create Python user-defined functions using SQL statements.

The Python dependencies could be specified via the Python [config options]({{< ref "docs/dev/python/python_config" >}}#python-options),
such as **python.archives**, **python.files**, **python.requirements**, **python.client.executable**, **python.executable**. etc or through [command line arguments]({{< ref "docs/deployment/cli" >}}#usage) when submitting the job.
