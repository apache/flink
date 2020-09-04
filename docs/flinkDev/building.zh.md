---
title: 从源码构建 Flink
nav-parent_id: flinkdev
nav-pos: 20
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

本篇主题是如何从版本 {{ site.version }} 的源码构建 Flink。

* This will be replaced by the TOC
{:toc}

## 构建 Flink

首先需要准备源码。可以[从发布版本下载源码]({{ site.download_url }})或者[从 Git 库克隆 Flink 源码]({{ site.github_url }})。

还需要准备 **Maven 3** 和 **JDK** (Java开发套件)。Flink 依赖 **Java 8** 或更新的版本来进行构建。

*注意：Maven 3.3.x 可以构建 Flink，但是不能正确地屏蔽掉指定的依赖。Maven 3.2.5 可以正确地构建库文件。

运行单元测试需要 Java 8u51 以上的版本，以避免使用 PowerMock Runner 的单元测试失败。

输入以下命令从 Git 克隆代码

{% highlight bash %}
git clone {{ site.github_url }}
{% endhighlight %}

最简单的构建 Flink 的方法是执行如下命令：

{% highlight bash %}
mvn clean install -DskipTests
{% endhighlight %}

上面的 [Maven](http://maven.apache.org) 指令（`mvn`）首先删除（`clean`）所有存在的构建，然后构建一个新的 Flink 运行包（`install`）。

为了加速构建，可以执行如下命令，以跳过测试，QA 的插件和 JavaDocs 的生成：

{% highlight bash %}
mvn clean install -DskipTests -Dfast
{% endhighlight %}

## 构建 PyFlink

#### 先决条件

1. 构建 Flink

    如果想构建一个可用于 pip 安装的 PyFlink 包，需要先构建 Flink 工程，如 [构建 Flink](#build-flink) 中所述。

2. Python 的版本为 3.5, 3.6, 3.7 或者 3.8.

    ```shell
    $ python --version
    # the version printed here must be 3.5, 3.6, 3.7 or 3.8
    ```

3. 构建 PyFlink 的 Cython 扩展模块（可选的）

    为了构建 PyFlink 的 Cython 扩展模块，需要 C 编译器。在不同操作系统上安装 C 编译器的方式略有不同：

    * **Linux** Linux 操作系统通常预装有 GCC。否则，需要手动安装。例如，可以在 Ubuntu 或 Debian 上使用命令`sudo apt-get install build-essential`安装。

    * **Mac OS X** 要在 Mac OS X 上安装 GCC，你需要下载并安装 [Xcode 命令行工具](https://developer.apple.com/downloads/index.action
    )，该工具可在 Apple 的开发人员页面中找到。

    还需要使用以下命令安装依赖项：

    ```shell
    $ python -m pip install -r flink-python/dev/dev-requirements.txt
    ```

#### 安装

进入 Flink 源码根目录，并执行以下命令，构建 PyFlink 的源码发布包和 wheel 包：

{% highlight bash %}
cd flink-python; python setup.py sdist bdist_wheel
{% endhighlight %}

构建好的源码发布包和 wheel 包位于`./flink-python/dist/`目录下。它们均可使用 pip 安装，比如:

{% highlight bash %}
python -m pip install dist/*.tar.gz
{% endhighlight %}

## 依赖屏蔽

Flink [屏蔽](https://maven.apache.org/plugins/maven-shade-plugin/)了一些它使用的包，这样做是为了避免与程序员自己引入的包的存在的可能的版本冲突。屏蔽掉的包包括 *Google Guava*,*Asm*,*Apache Curator*,*Apache HTTP Components*,*Netty* 等。

这种依赖屏蔽机制最近在 Maven 中有所改变。需要用户根据 Maven 的的不同版本来执行不同的命令。

**对于Maven 3.1.x and 3.2.x**
直接在 Flink 源码根目录执行命令 `mvn clean install -DskipTests` 就足够了。

**Maven 3.3.x**
如下的构建需要两步走：第一步需要在基础目录下执行编译构建；第二步需要在编译后的 flink-dist 目录下执行：

{% highlight bash %}
mvn clean install -DskipTests
cd flink-dist
mvn clean install
{% endhighlight %}

*注意:* 运行 `mvn --version` 以查看Maven的版本。

{% top %}

## Hadoop 版本

请查看 [Hadoop 集成模块]({{ site.baseurl }}/ops/deployment/hadoop.html) 一节中关于处理 Hadoop 的类和版本问题的方法。

## Scala 版本

{% info %} 只是用 Java 库和 API 的用户可以 *忽略* 这一部分。

Flink 有使用 [Scala](http://scala-lang.org) 来写的 API，库和运行时模块。使用 Scala API 和库的同学必须配置 Flink 的 Scala 版本和自己的 Flink 版本（因为 Scala 
并不严格的向后兼容）。

从 1.7 版本开始，Flink 可以使用 Scala 2.11（默认）和 2.12 来构建。

如果使用 Scala 2.12 来进行构建，执行如下命令：
{% highlight bash %}
mvn clean install -DskipTests -Dscala-2.12
{% endhighlight %}

{% top %}

## 加密的文件系统

如果你的 home 目录是加密的，可能遇到如下异常 `java.io.IOException: File name too long`。一些像 Ubuntu 的 enfs 这样的加密文件系统因为不支持长文件名会产生这个异常。

解决方法是添加如下内容到 pom.xml 文件中出现这个错误的模块的编译器配置项下。

{% highlight xml %}
<args>
    <arg>-Xmax-classfile-name</arg>
    <arg>128</arg>
</args>
{% endhighlight %}

例如，如果错误出现在 `flink-yarn` 模块下，上述的代码需要添加到 `scala-maven-plugin` 的 `<configuration>` 项下。请查看[这个问题](https://issues.apache.org/jira/browse/FLINK-2003)的链接获取更多信息。

{% top %}
