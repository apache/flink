---
title: "Dependency Management"
nav-parent_id: python_datastream_api
nav-pos: 40
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

# Java Dependency

If third-party Java dependencies are used, you can specify the dependencies with the following Python DataStream APIs or 
through [command line arguments]({% link ops/cli.md %}#usage) directly when submitting the job.

{% highlight python %}
# Use the add_jars() to add local jars and the jars will be uploaded to the cluster.
# NOTE: Only local file URLs (start with "file://") are supported.
stream_execution_environment.add_jars("file:///my/jar/path/connector.jar", ...)

# Use the add_classpaths() to add the dependent jars URL into the classpath. And the URL will also be added to the classpath of the cluster.
# NOTE: The Paths must specify a protocol (e.g. file://) and users should ensure that the URLs are accessible on both the client and the cluster.
stream_execution_environment.add_classpaths("file:///my/jar/path/connector.jar", ...)
{% endhighlight %}
**Note:** These APIs could be called multiple times.

# Python Dependency

If third-party Python dependencies are used, you can specify the dependencies with the following Python DataStream 
APIs or through [command line arguments]({% link ops/cli.md %}#usage) directly when submitting the job.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">APIs</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>add_python_file(file_path)</strong></td>
      <td>
        <p>Adds python file dependencies which could be python files, python packages or local directories. They will be added to the PYTHONPATH of the python UDF worker.</p>
{% highlight python %}
stream_execution_environment.add_python_file(file_path)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>set_python_requirements(requirements_file_path, requirements_cache_dir=None)</strong></td>
      <td>
        <p>Specifies a requirements.txt file which defines the third-party dependencies. These dependencies will be installed to a temporary directory and added to the PYTHONPATH of the python UDF worker. For the dependencies which could not be accessed in the cluster, a directory which contains the installation packages of these dependencies could be specified using the parameter "requirements_cached_dir". It will be uploaded to the cluster to support offline installation.</p>
{% highlight python %}
# commands executed in shell
echo numpy==1.16.5 > requirements.txt
pip download -d cached_dir -r requirements.txt --no-binary :all:

# python code
stream_execution_environment.set_python_requirements("requirements.txt", "cached_dir")
{% endhighlight %}
        <p>Please make sure the installation packages matches the platform of the cluster and the python version used. These packages will be installed using pip, so also make sure the version of Pip (version >= 7.1.0) and the version of Setuptools (version >= 37.0.0).</p>
      </td>
    </tr>
    <tr>
      <td><strong>add_python_archive(archive_path, target_dir=None)</strong></td>
      <td>
        <p>Adds a python archive file dependency. The file will be extracted to the working directory of python UDF worker. If the parameter "target_dir" is specified, the archive file will be extracted to a directory named "target_dir". Otherwise, the archive file will be extracted to a directory with the same name of the archive file.</p>
{% highlight python %}
# command executed in shell
# assert the relative path of python interpreter is py_env/bin/python
zip -r py_env.zip py_env

# python code
stream_execution_environment.add_python_archive("py_env.zip")
# or
stream_execution_environment.add_python_archive("py_env.zip", "myenv")

# the files contained in the archive file can be accessed in UDF
def my_func():
    with open("myenv/py_env/data/data.txt") as f:
        ...
{% endhighlight %}
        <p>Please make sure the uploaded python environment matches the platform that the cluster is running on. Currently only zip-format is supported. i.e. zip, jar, whl, egg, etc.</p>
      </td>
    </tr>
    <tr>
      <td><strong>set_python_executable(python_exec)</strong></td>
      <td>
        <p>Sets the path of the python interpreter which is used to execute the python udf workers, e.g., "/usr/local/bin/python3".</p>
{% highlight python %}
stream_execution_environment.add_python_archive("py_env.zip")
stream_execution_environment.get_config().set_python_executable("py_env.zip/py_env/bin/python")
{% endhighlight %}
        <p>Please make sure that the specified environment matches the platform that the cluster is running on.</p>
      </td>
    </tr>
  </tbody>
</table>
