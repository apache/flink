---
title: "本地集群"
nav-title: '本地集群'
nav-parent_id: standalone
nav-pos: 2
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

通过几个简单的步骤启动并运行 Flink 本地集群。

* This will be replaced by the TOC
{:toc}

<a name="setup-download-and-start-flink"></a>

## 步骤：下载并启动 Flink

Flink 运行在 __Linux 和 Mac OS X__ 上。
<span class="label label-info">注意：</span> Windows 用户可以在 Cygwin 或 WSL 中运行 Flink。

为了运行 Flink，唯一的要求是安装了 __Java 8 或 11__ 的环境。

你可以通过以下命令来检查是否正确安装了 Java：

{% highlight bash %}
java -version
{% endhighlight %}

如果你安装了 Java 8，则会有类似如下的输出：

{% highlight bash %}
java version "1.8.0_111"
Java(TM) SE Runtime Environment (build 1.8.0_111-b14)
Java HotSpot(TM) 64-Bit Server VM (build 25.111-b14, mixed mode)
{% endhighlight %}

{% if site.is_stable %}
<div class="codetabs" markdown="1">
<div data-lang="下载并解压" markdown="1">
1. 从[下载页面](https://flink.apache.org/downloads.html)下载一个二进制发行版本。你可以根据需要选择不同的 Scala 版本。对于某些功能，你可能还需要下载一个预先打包好的 Hadoop  jar 文件，并把它放到 `lib` 目录下。
2. 前往下载目录。
3. 解压下载的文档。

{% highlight bash %}
$ cd ~/Downloads        # 前往下载目录
$ tar xzf flink-*.tgz   # 解压下载的文档
$ cd flink-{{site.version}}
{% endhighlight %}
</div>

<div data-lang="MacOS X" markdown="1">
对于 MacOS X 用户，Flink 可以通过 [Homebrew](https://brew.sh/) 来安装。

{% highlight bash %}
$ brew install apache-flink
...
$ flink --version
Version: 1.2.0, Commit ID: 1c659cf
{% endhighlight %}
</div>

</div>

{% else %}
<a name="download-and-compile"></a>

### 下载并编译
你可以从我们的[仓库](https://flink.apache.org/community.html#source-code)之一克隆源代码，例如：

{% highlight bash %}
$ git clone https://github.com/apache/flink.git
$ cd flink

# 构建 Flink 可能最多需要 25 分钟
# 你可以通过传递参数 '-Pskip-webui-build'
# 以跳过 web ui 来加速构建

$ mvn clean package -DskipTests -Pfast 

# 这是 Flink 会被安装到的目录
$ cd build-target               
{% endhighlight %}
{% endif %}

<a name="start-a-local-flink-cluster"></a>

### 启动 Flink 本地集群

{% highlight bash %}
$ ./bin/start-cluster.sh  # 启动 Flink
{% endhighlight %}

你可以检查 __JobManager 的 web 页面__  [http://localhost:8081](http://localhost:8081) 以确认本地集群正常启动并运行着。web 页面应该会显示有一个可用的 TaskManager。

<a href="{% link /page/img/quickstart-setup/jobmanager-1.png %}" ><img class="img-responsive" src="{% link /page/img/quickstart-setup/jobmanager-1.png %}" alt="JobManager: Overview"/></a>

你也可以通过检查 `logs` 目录中的日志文件来确认系统正在运行。

{% highlight bash %}
$ tail log/flink-*-standalonesession-*.log
INFO ... - Rest endpoint listening at localhost:8081
INFO ... - http://localhost:8081 was granted leadership ...
INFO ... - Web frontend listening at http://localhost:8081.
INFO ... - Starting RPC endpoint for StandaloneResourceManager at akka://flink/user/resourcemanager .
INFO ... - Starting RPC endpoint for StandaloneDispatcher at akka://flink/user/dispatcher .
INFO ... - ResourceManager akka.tcp://flink@localhost:6123/user/resourcemanager was granted leadership ...
INFO ... - Starting the SlotManager.
INFO ... - Dispatcher akka.tcp://flink@localhost:6123/user/dispatcher was granted leadership ...
INFO ... - Recovering all persisted jobs.
INFO ... - Registering TaskManager ... at ResourceManager
{% endhighlight %}

<a name="windows-cygwin-users"></a>

#### Windows Cygwin 用户

如果你使用 Windows 的 git shell 从 git 仓库安装 Flink，Cygwin 可能会产生类似如下的错误：

{% highlight bash %}
c:/flink/bin/start-cluster.sh: line 30: $'\r': command not found
{% endhighlight %}

这个错误是因为当 git 运行在 Windows 上时，它会自动地将 UNIX 的行结束符转换成 Windows 风格的行结束符，但是 Cygwin 只能处理 Unix 风格的行结束符。解决方案是通过以下三步调整 Cygwin 的配置，使其能够正确处理行结束符：

1. 启动 Cygwin shell。

2. 输入以下命令进入你的用户目录

{% highlight bash %}
cd; pwd
{% endhighlight %}

    这个命令会在 Cygwin 的根目录下返回路径地址。

3. 使用 NotePad、WordPad 或其他的文本编辑器在用户目录下打开 `.bash_profile` 文件并添加以下内容：（如果这个文件不存在则需要创建）

{% highlight bash %}
export SHELLOPTS
set -o igncr
{% endhighlight %}

保存文件并打开一个新的 bash shell。

<a name="stop-a-local-flink-cluster"></a>

### 关闭 Flink 本地集群

当执行以下命令后，Flink 会被**关闭**：

<div class="codetabs" markdown="1">
<div data-lang="Bash" markdown="1">
{% highlight bash %}
$ ./bin/stop-cluster.sh
{% endhighlight %}
</div>
<div data-lang="Windows Shell" markdown="1">
你可以在打开的 shell 窗口中通过 CTRL-C 来结束进程。
</div>
</div>

{% top %}
