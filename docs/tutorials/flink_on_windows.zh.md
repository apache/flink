---
title:  "在Windows上运行Flink"
nav-parent_id: setuptutorials
nav-pos: 30
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you  may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

如果您想在Windows计算机本地上运行Flink，则需要[下载](http://flink.apache.org/downloads.html)并解压Flink二进制发行版压缩包。之后，您可以使用Windows批处理文件（.bat），也可以使用Cygwin运行Flink Jobmanager。

## 从Windows批处理文件开始
从Windows命令行启动Flink，请打开命令窗口，进入Flink的`bin/`目录下并运行`start-cluster.bat`命令。

注意：Java运行环境的``bin``文件夹必须包含在Window的%PATH%变量中。按照本[指南](http://www.java.com/en/download/help/path.xml)将Java添加到%PATH%变量中。

{% highlight bash lineno %}
$ cd flink
$ cd bin
$ start-cluster.bat
Starting a local cluster with one JobManager process and one TaskManager process.
You can terminate the processes via CTRL-C in the spawned shell windows.
Web interface by default on http://localhost:8081/.
{% endhighlight %}

在这之后，您需要打开另一个终端来使用`flink.bat`运行作业。

{% top %}

## 从Cygwin和Unix脚本开始
您需要先启动*Cygwin*终端，然后进入Flink目录并运行start-cluster.sh脚本：

{% highlight bash %}
$ cd flink
$ bin/start-cluster.sh
Starting cluster.
{% endhighlight %}

{% top %}

## 从Git上安装Flink
如果您正在从git仓库上Flink并且正在使用Windows git shell，那么Cygwin可能会产生类似这样的一个错误：

{% highlight bash %}
c:/flink/bin/start-cluster.sh: line 30: $'\r': command not found
{% endhighlight %}

该错误的发生是因为在Windows环境下运行时，git会将UNIX行尾自动转换为Windows样式的行尾。问题是Cygwin只能处理UNIX样式的行尾。解决方案是按照以下三个步骤调整Cygwin设置以处理正确的行尾：

1. 启动一个Cygwin shell。

2. 通过输入命令来确定你的home目录
    
    {% highlight bash %}
    cd; pwd
    {% endhighlight %}
    
    这将返回Cygwin根路径下的路径。
    
3. 使用NotePad,WordPad或其他文本编辑器打开在home目录下的`.bash_profile`文件并附加以下内容:(如果文件不存在，则必须创建它)

{% highlight bash %}
export SHELLOPTS
set -o igncr
{% endhighlight %}

保存文件并打开一个新的bash shell。

{% top %}
    

