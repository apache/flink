---
title: "Scala 项目模板"
nav-title: Scala 项目模板
nav-parent_id: projectsetup
nav-pos: 1
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

* This will be replaced by the TOC
{:toc}


## 构建工具

可以使用不同的构建工具来构建Flink项目。
为了快速入门，Flink为以下构建工具提供了项目模板：

- [SBT](#sbt)
- [Maven](#maven)

这些模板将帮助你建立项目的框架并创建初始化的构建文件。

## SBT

### 创建项目

你可以通过以下两种方法之一构建新项目:

<ul class="nav nav-tabs" style="border-bottom: none;">
    <li class="active"><a href="#sbt_template" data-toggle="tab">使用 <strong>sbt 模版</strong></a></li>
    <li><a href="#quickstart-script-sbt" data-toggle="tab">运行 <strong>quickstart 脚本</strong></a></li>
</ul>

<div class="tab-content">
    <div class="tab-pane active" id="sbt_template">
    {% highlight bash %}
    $ sbt new tillrohrmann/flink-project.g8
    {% endhighlight %}
    这里将提示你输入几个参数 (项目名称，Flink版本...) 然后从 <a href="https://github.com/tillrohrmann/flink-project.g8">Flink项目模版</a>创建一个Flink项目。
    你的sbt版本需要不小于0.13.13才能执行这个命令。如有必要，你可以参考这个<a href="http://www.scala-sbt.org/download.html">安装指南</a>获取合适版本的sbt。
    </div>
    <div class="tab-pane" id="quickstart-script-sbt">
    {% highlight bash %}
    $ bash <(curl https://flink.apache.org/q/sbt-quickstart.sh)
    {% endhighlight %}
    这将在<strong>指定的</strong>目录创建一个Flink项目。
    </div>
</div>

### 构建项目

为了构建你的项目，仅需简单的运行 `sbt clean assembly` 命令。
这将在 __target/scala_your-major-scala-version/__ 目录中创建一个 fat-jar __your-project-name-assembly-0.1-SNAPSHOT.jar__。

### 运行项目

为了构建你的项目，需要运行 `sbt run`。

默认情况下，这会在运行 `sbt` 的 JVM 中运行你的作业。
为了在不同的 JVM 中运行，请添加以下内容添加到 `build.sbt`

{% highlight scala %}
fork in run := true
{% endhighlight %}


#### IntelliJ

我们建议你使用 [IntelliJ](https://www.jetbrains.com/idea/) 来开发Flink作业。
开始，你需要将新建的项目导入到 IntelliJ。
通过 `File -> New -> Project from Existing Sources...` 操作路径，然后选择项目目录。之后 IntelliJ 将自动检测到 `build.sbt` 文件并设置好所有内容。

为了运行你的Flink作业，建议选择 `mainRunner` 模块作为 __Run/Debug Configuration__ 的类路径。
这将确保在作业执行时可以使用所有设置为 _provided_ 的依赖项。
你可以通过 `Run -> Edit Configurations...` 配置  __Run/Debug Configurations__，然后从 _Use classpath of module_ 下拉框中选择 `mainRunner`。

#### Eclipse

为了将新建的项目导入 [Eclipse](https://eclipse.org/), 首先需要创建一个 Eclipse 项目文件。
通过插件 [sbteclipse](https://github.com/typesafehub/sbteclipse) 创建项目文件，并将下面的内容添加到 `PROJECT_DIR/project/plugins.sbt` 文件中:

{% highlight bash %}
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")
{% endhighlight %}

在 `sbt` 中使用以下命令创建 Eclipse 项目文件

{% highlight bash %}
> eclipse
{% endhighlight %}

现在你可以通过 `File -> Import... -> Existing Projects into Workspace` 将项目导入 Eclipse，然后选择项目目录。

## Maven

### 环境要求

唯一的要求是安装 __Maven 3.0.4__ (或更高版本) 和 __Java 8.x__。


### 创建项目

使用以下命令之一来 __创建项目__:

<ul class="nav nav-tabs" style="border-bottom: none;">
    <li class="active"><a href="#maven-archetype" data-toggle="tab">使用 <strong>Maven archetypes</strong></a></li>
    <li><a href="#quickstart-script" data-toggle="tab">运行 <strong>quickstart 开始脚本</strong></a></li>
</ul>

<div class="tab-content">
    <div class="tab-pane active" id="maven-archetype">
    {% highlight bash %}
    $ mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-scala     \{% unless site.is_stable %}
      -DarchetypeCatalog=https://repository.apache.org/content/repositories/snapshots/ \{% endunless %}
      -DarchetypeVersion={{site.version}}
    {% endhighlight %}
    这将允许你 <strong>为新项目命名</strong>。同时以交互式的方式询问你项目的 groupId，artifactId 和 package 名称.
    </div>
    <div class="tab-pane" id="quickstart-script">
{% highlight bash %}
{% if site.is_stable %}
    $ curl https://flink.apache.org/q/quickstart-scala.sh | bash -s {{site.version}}
{% else %}
    $ curl https://flink.apache.org/q/quickstart-scala-SNAPSHOT.sh | bash -s {{site.version}}
{% endif %}
{% endhighlight %}
    </div>
    {% unless site.is_stable %}
    <p style="border-radius: 5px; padding: 5px" class="bg-danger">
        <b>注意</b>: 对于 Maven 3.0 及更高版本, 不再能够通过命令行指定仓库 (-DarchetypeCatalog). 如果你想使用快照仓库，需要在 settings.xml 文件中添加仓库条目。有关该设置的详细信息，请参阅 <a href="http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html">Maven 官方文档</a>
    </p>
    {% endunless %}
</div>


### 检查项目

项目创建后，工作目录中将多出一个新目录。如果你使用的是 _curl_ 方式创建项目，目录称为 `quickstart`，如果是另外一种创建方式，目录则称为你指定的 `artifactId`。

{% highlight bash %}
$ tree quickstart/
quickstart/
├── pom.xml
└── src
    └── main
        ├── resources
        │   └── log4j.properties
        └── scala
            └── org
                └── myorg
                    └── quickstart
                        ├── BatchJob.scala
                        └── StreamingJob.scala
{% endhighlight %}

样例项目是一个 __Maven 项目__, 包含了两个类： _StreamingJob_ 和 _BatchJob_ 是 *DataStream* 和 *DataSet* 程序的基本框架程序.
_main_ 方法是程序的入口, 既用于 IDE 内的测试/执行，也用于合理部署。

我们建议你将 __此项目导入你的 IDE__。

IntelliJ IDEA 支持 Maven 开箱即用，并为Scala开发提供插件。
从我们的经验来看，IntelliJ 提供了最好的Flink应用程序开发体验。

对于 Eclipse，需要以下的插件，你可以从提供的 Eclipse Update Sites 安装这些插件：

* _Eclipse 4.x_
  * [Scala IDE](http://download.scala-ide.org/sdk/lithium/e44/scala211/stable/site)
  * [m2eclipse-scala](http://alchim31.free.fr/m2e-scala/update-site)
  * [Build Helper Maven Plugin](https://repo1.maven.org/maven2/.m2e/connectors/m2eclipse-buildhelper/0.15.0/N/0.15.0.201207090124/)
* _Eclipse 3.8_
  * [Scala IDE for Scala 2.11](http://download.scala-ide.org/sdk/helium/e38/scala211/stable/site) 或者 [Scala IDE for Scala 2.10](http://download.scala-ide.org/sdk/helium/e38/scala210/stable/site)
  * [m2eclipse-scala](http://alchim31.free.fr/m2e-scala/update-site)
  * [Build Helper Maven Plugin](https://repository.sonatype.org/content/repositories/forge-sites/m2e-extras/0.14.0/N/0.14.0.201109282148/)

### 构建

如果你想要 __构建/打包你的项目__, 进入到你的项目目录，并执行命令‘`mvn clean package`’。
你将 __找到一个 JAR 文件__，其中包含了你的应用程序，以及已作为依赖项添加到应用程序的连接器和库：`target/<artifact-id>-<version>.jar`。

__注意:__ 如果你使用其他类而不是 *StreamingJob* 作为应用程序的主类/入口，我们建议你相应地更改 `pom.xml` 文件中 `mainClass` 的设置。这样，Flink 运行应用程序时无需另外指定主类。


## 下一步

开始编写你的应用！

如果你准备编写流处理应用，正在寻找灵感来写什么，
可以看看[流处理应用程序教程]({{ site.baseurl }}/zh/getting-started/walkthroughs/datastream_api.html)

如果你准备编写批处理应用，正在寻找灵感来写什么，
可以看看[批处理应用程序示例]({{ site.baseurl }}/zh/dev/batch/examples.html)

有关 API 的完整概述，请查看
[DataStream API]({{ site.baseurl }}/zh/dev/datastream_api.html) 和
[DataSet API]({{ site.baseurl }}/zh/dev/batch/index.html) 部分。

在[这里]({{ site.baseurl }}/zh/ops/deployment/local.html)，你可以找到如何在IDE外的本地集群中运行应用程序。

如果你有任何问题，请发信至我们的[邮箱列表](http://mail-archives.apache.org/mod_mbox/flink-user/)。
我们很乐意提供帮助。

{% top %}
