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

本篇主题是如何从版本{{ site.version }}的源码构建Flink。

* This will be replaced by the TOC
{:toc}

## 构建Flink

首先需要准备一份源码。有两种方法：第一种参考[从发布版本下载源码]({{ site.download_url }})；第二种参考[从Git库克隆Flink源码]({{ site.github_url }})

还需要准备，**Maven 3** 和 **JDK** (Java开发套件)。Flink至少依赖 **Java 8** 来进行构建。

*注意：Maven 3.3.x 可以构建Flink，但是不会去掉指定的依赖。Maven 3.2.5 可以很好地构建对应的库文件。
To build unit tests use Java 8u51 or above to prevent failures in unit tests that use the PowerMock runner.*
如果运行单元，则需要 Java 8u51 以上的版本来去掉PowerMock运行器带来的单元测试失败情况。

从Git克隆代码，输入：

{% highlight bash %}
git clone {{ site.github_url }}
{% endhighlight %}

最简单的构建Flink的方法，执行如下命令：

{% highlight bash %}
mvn clean install -DskipTests
{% endhighlight %}

上面的[Maven](http://maven.apache.org)指令，(`mvn`)首先删除(`clean`)所有存在的构建，然后构建(`install`)一个新的Flink运行包。

为了加速构建，你可以跳过测试，QA的插件和JavaDocs的生成，执行如下命令：

{% highlight bash %}
mvn clean install -DskipTests -Dfast
{% endhighlight %}

## 构建PyFlink

#### 先决条件

1. 构建Flink

    如果您想构建一个可用于pip安装的PyFlink包，您需要先构建Flink工程，如[构建Flink](#build-flink)中所述。

2. Python的版本为3.5, 3.6 或者 3.7.

    ```shell
    $ python --version
    # the version printed here must be 3.5, 3.6 or 3.7
    ```

3. 构建PyFlink的Cython扩展模块（可选的）

    为了构建PyFlink的Cython扩展模块，您需要C编译器。在不同操作系统上安装C编译器的方式略有不同：

    * **Linux** Linux操作系统通常预装有GCC。否则，您需要手动安装。例如，您可以在Ubuntu或Debian上使用命令`sudo apt-get install build-essential`安装。

    * **Mac OS X** 要在Mac OS X上安装GCC，您需要下载并安装 [Xcode命令行工具](https://developer.apple.com/downloads/index.action)，该工具可在Apple的开发人员页面中找到。

    您还需要使用以下命令安装依赖项：

    ```shell
    $ python -m pip install -r flink-python/dev/dev-requirements.txt
    ```

#### 安装

进入Flink源码根目录，并执行以下命令，构建PyFlink的源码发布包和wheel包：

{% highlight bash %}
cd flink-python; python setup.py sdist bdist_wheel
{% endhighlight %}

构建好的源码发布包和wheel包位于`./flink-python/dist/`目录下。它们均可使用pip安装,比如:

{% highlight bash %}
python -m pip install dist/*.tar.gz
{% endhighlight %}

## 依赖屏蔽

Flink [依赖屏蔽](https://maven.apache.org/plugins/maven-shade-plugin/) 一些它使用的包，这样做是为了避免与程序员自己引入的包的存在的可能的版本冲突。屏蔽掉的包包括 *Google Guava*, *Asm* , *Apache Curator*, *Apache HTTP Components
*, *Netty* 等。

这种依赖屏蔽机制最近在Maven中有所改变。需要用户根据Maven的的不同版本来执行不同的命令。

**对于Maven 3.1.x and 3.2.x**
直接在Flink源码根目录执行命令 `mvn clean install -DskipTests` 就足够了。

**Maven 3.3.x**
如下的构建需要两步走：第一步需要在基础目录下执行编译构建；第二步需要在编译后的flink-dist目录下执行：

{% highlight bash %}
mvn clean install -DskipTests
cd flink-dist
mvn clean install
{% endhighlight %}

*注意:* 查看Maven的版本, 执行 `mvn --version`.

{% top %}

## Hadoop 版本

请查看 [Hadoop 集成模块]({{ site.baseurl }}/ops/deployment/hadoop.html) 来处理Hadoop的类和版本问题。

## Scala 版本

{% info %} 只是用Jave库和API的同学可以 *忽略* 这一部分。

Flink has APIs, libraries, and runtime modules written in [Scala](http://scala-lang.org). Users of the Scala API and libraries may have to match the Scala version of Flink with the Scala version of their projects (because Scala is not strictly backwards compatible).
Flink有使用[Scala](http://scala-lang.org)来写的API，库和运行时模块。使用Scala API和库的同学必须配置Flink的Scala版本和自己的Flink版本（因为Scala并不严格的向后兼容）。

Since version 1.7 Flink builds with Scala version 2.11 (default) and 2.12.
从Flink 1.7版本开始，默认使用Scala 2.11（默认）和 2.12 来构建。

如果使用Scala 2.12 来进行构建，执行如下命令：
{% highlight bash %}
mvn clean install -DskipTests -Dscala-2.12
{% endhighlight %}

{% top %}

## 加密的文件系统

如果你的家目录是加密的，可能遇到如下异常`java.io.IOException: File name too long`。一些像Ubuntu的enfs的文件系统不支持长文件名，才导致了这个异常的产生。

解决方法是添加如下内容

{% highlight xml %}
<args>
    <arg>-Xmax-classfile-name</arg>
    <arg>128</arg>
</args>
{% endhighlight %}

到pom.xml文件中引起这个错误的编译器的配置项下。例如，如果错误出现在`flink-yarn`模块下，上述的代码需要添加到`scala-maven-plugin`的`<configuration
>`项下。请查看[这个问题](https://issues.apache.org/jira/browse/FLINK-2003)的链接获取更多信息。

{% top %}

