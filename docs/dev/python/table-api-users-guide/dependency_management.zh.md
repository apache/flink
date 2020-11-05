---
title: "依赖管理"
nav-parent_id: python_tableapi
nav-pos: 45
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

<a name="java-dependency-in-python-program"/>

# Java 依赖管理

如果应用了第三方 Java 依赖， 用户可以通过以下 Python Table API进行配置，或者在提交作业时直接通过[命令行参数]({% link ops/cli.zh.md %}#usage)配置。

{% highlight python %}
# 通过 "pipeline.jars" 参数指定 jar 包 URL列表， 每个 URL 使用 ";" 分隔。这些 jar 包最终会被上传到集群中。
# 注意：当前支持通过本地文件 URL 进行上传(以 "file://" 开头)。
table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")

# 通过 "pipeline.jars" 参数指定依赖 URL 列表， 这些 URL 通过 ";" 分隔，它们最终会被添加到集群的 classpath 中。
# 注意： 必须指定这些文件路径的协议 (如 file://), 并确保这些 URL 在本地客户端和集群都能访问。
table_env.get_config().get_configuration().set_string("pipeline.classpaths", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")
{% endhighlight %}

<a name="python-dependency-in-python-program"/>

# Python 依赖管理

如果程序中应用到了 Python 第三方依赖，用户可以使用以下 Table API 配置依赖信息，或在提交作业时直接通过[命令行参数]({% link ops/cli.zh.md %}#usage)配置。

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
        <p>添加 Python 文件依赖，可以是 Python文件、Python 包或本地文件目录。他们最终会被添加到 Python Worker 的 PYTHONPATH 中，从而让 Python 函数能够正确访问读取。</p>
{% highlight python %}
table_env.add_python_file(file_path)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>set_python_requirements(requirements_file_path, requirements_cache_dir=None)</strong></td>
      <td>
        <p>配置一个 requirements.txt 文件用于指定 Python 第三方依赖，这些依赖会被安装到一个临时目录并添加到 Python Worker 的 PYTHONPATH 中。对于在集群中无法访问的外部依赖，用户可以通过 "requirements_cached_dir" 参数指定一个包含这些依赖安装包的目录，这个目录文件会被上传到集群并实现离线安装。</p>
{% highlight python %}
# 执行下面的 shell 命令
echo numpy==1.16.5 > requirements.txt
pip download -d cached_dir -r requirements.txt --no-binary :all:

# python 代码
table_env.set_python_requirements("requirements.txt", "cached_dir")
{% endhighlight %}
        <p>请确保这些依赖安装包和集群运行环境所使用的 Python 版本相匹配。此外，这些依赖将通过 Pip 安装， 请确保 Pip 的版本（version >= 7.1.0） 和 Setuptools 的版本（version >= 37.0.0）符合要求。</p>
      </td>
    </tr>
    <tr>
      <td><strong>add_python_archive(archive_path, target_dir=None)</strong></td>
      <td>
        <p>添加 Python 归档文件依赖。归档文件内的文件将会被提取到 Python Worker 的工作目录下。如果指定了 "target_dir" 参数，归档文件则会被提取到指定名字的目录文件中，否则文件被提取到和归档文件名相同的目录中。</p>
{% highlight python %}
# shell 命令
# 断言 python 解释器的相对路径是 py_env/bin/python
zip -r py_env.zip py_env

# python 代码
table_env.add_python_archive("py_env.zip")
# 或者
table_env.add_python_archive("py_env.zip", "myenv")

# 归档文件中的文件可以被 Python 函数读取
def my_udf():
    with open("myenv/py_env/data/data.txt") as f:
        ...
{% endhighlight %}
        <p>请确保上传的 Python 环境和集群运行环境匹配。目前只支持上传 zip 格式的文件，如 zip, jar, whl, egg等等。</p>
      </td>
    </tr>
    <tr>
      <td><strong>set_python_executable(python_exec)</strong></td>
      <td>
        <p>配置用于执行 Python Worker 的 Python 解释器路径，如 "/usr/local/bin/python3"。</p>
{% highlight python %}
table_env.add_python_archive("py_env.zip")
table_env.get_config().set_python_executable("py_env.zip/py_env/bin/python")
{% endhighlight %}
        <p>请确保配置的 Python 环境和集群运行环境匹配。</p>
      </td>
    </tr>
  </tbody>
</table>

<a name="python-dependency-in-javascala-program"/>

# Java/Scala程序中的Python依赖管理

If Python UDFs is created via the [CREATE FUNCTION SQL Statement]({% link  dev/table/sql/create.zh.md %}#create-function) 
and used in Java/Scala program, usually the Python environment need to be well prepared and the Python dependencies need 
to be well configured via [Python configuration options]({% link dev/python/table-api-users-guide/python_config.zh.md %}) 
or [Python commandline options]({% link ops/cli.zh.md %}#usage).

If you are using Python UDF in a Java/Scala program for the first time, you can prepare the Python environment and 
specify the Python dependencies as follows: 

1. Prepare a **portable** Python environment which has installed PyFlink, for Mac/Linux users we prepare a 
[convenient script]({% link downloads/setup-pyflink-virtual-env.sh %}) to create a miniconda environment with PyFlink 
installed. If your Python UDFs have additional dependencies, you need to install them in the environment.
2. Set the configuration option `python.client.executable` to the Python interpreter path of the Python environment 
above at the beginning of your program. 
3. Set the configuration option `python.files` to the paths of the Python files which define the 
Python UDFs, use "," as the separator of paths at the beginning of your program. 
4. Pack the Python environment to a zip file, and set the configuration option `python.archives` to the path of the zip 
file at the beginning of your program. 
5. Set the configuration option `python.executable` to the Python interpreter path in the zip file created above at the 
beginning of your program, e.g. for the zip file generated from the 
[convenient script]({% link downloads/setup-pyflink-virtual-env.sh %}), the value of the configuration option 
 `python.executable` should be "venv.zip/venv/bin/python".

