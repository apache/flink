---
title: "常见问题"
nav-parent_id: python
nav-pos: 140
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

本页介绍了针对PyFlink用户的一些常见问题的解决方案。

* This will be replaced by the TOC
{:toc}

## 准备Python虚拟环境

您可以下载[便捷脚本]({{ site.baseurl }}/downloads/setup-pyflink-virtual-env.sh)，以准备可在Mac OS和大多数Linux发行版上使用的Python虚拟环境包(virtual env zip)。
您可以指定PyFlink的版本，来生成对应的PyFlink版本所需的Python虚拟环境，否则将安装最新版本的PyFlink所对应的Python虚拟环境。

{% highlight bash %}
{% if site.is_stable %}
$ sh setup-pyflink-virtual-env.sh {{ site.version }}
{% else %}
$ sh setup-pyflink-virtual-env.sh
{% endif %}
{% endhighlight bash %}

## 使用Python虚拟环境执行PyFlink任务
在设置了[python虚拟环境](#准备python虚拟环境)之后（如上一节所述），您应该在执行PyFlink作业之前激活虚拟环境。

#### 本地（Local）

{% highlight shell %}
# activate the conda python virtual environment
$ source venv/bin/activate
$ python xxx.py
{% endhighlight %}

#### 集群（Cluster）

{% highlight shell %}
$ # 指定Python虚拟环境
$ table_env.add_python_archive("venv.zip")
$ # 指定用于执行python UDF workers (用户自定义函数工作者) 的python解释器的路径
$ table_env.get_config().set_python_executable("venv.zip/venv/bin/python")
{% endhighlight %}

如果需要了解`add_python_archive`和`set_python_executable`用法的详细信息，请参阅[相关文档]({% link dev/python/user-guide/table/dependency_management.zh.md %}#python-dependency)。

## 添加Jar文件

PyFlink作业可能依赖jar文件，比如connector，Java UDF等。
您可以在提交作业时使用以下Python Table API或通过[命令行参数]({% link ops/cli.zh.md %}#usage)来指定依赖项。

{% highlight python %}
# 注意：仅支持本地文件URL（以"file:"开头）。
table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")

# 注意：路径必须指定协议（例如：文件——"file"），并且用户应确保在客户端和群集上都可以访问这些URL。
table_env.get_config().get_configuration().set_string("pipeline.classpaths", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")
{% endhighlight %}

有关添加Java依赖项的API的详细信息，请参阅[相关文档]({% link dev/python/user-guide/table/dependency_management.zh.md %}#java-dependency)。

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

{% highlight python %}
table_env.add_python_file('myDir')

def my_udf():
    from utils import my_util
{% endhighlight %}
