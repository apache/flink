---
title: "Java 项目模板"
nav-title: Java 项目模板
nav-parent_id: projectsetup
nav-pos: 0
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

Flink项目可以使用不同的构建工具进行构建。
为了能够快速入门，Flink 为以下构建工具提供了项目模版：

- [Maven](#maven)
- [Gradle](#gradle)

这些模版可以帮助你搭建项目结构并创建初始构建文件。

## Maven

### 环境要求

唯一的要求是使用 __Maven 3.0.4__ （或更高版本）和安装 __Java 8.x__。

### 创建项目

使用以下命令之一来 __创建项目__：

<ul class="nav nav-tabs" style="border-bottom: none;">
    <li class="active"><a href="#maven-archetype" data-toggle="tab">使用 <strong>Maven archetypes</strong></a></li>
    <li><a href="#quickstart-script" data-toggle="tab">运行 <strong>quickstart 脚本</strong></a></li>
</ul>
<div class="tab-content">
    <div class="tab-pane active" id="maven-archetype">
    {% highlight bash %}
    $ mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-java      \{% unless site.is_stable %}
      -DarchetypeCatalog=https://repository.apache.org/content/repositories/snapshots/ \{% endunless %}
      -DarchetypeVersion={{site.version}}
    {% endhighlight %}
        这种方式允许你<strong>为新项目命名</strong>。它将以交互式的方式询问你项目的 groupId、artifactId 和 package 名称。
    </div>
    <div class="tab-pane" id="quickstart-script">
    {% highlight bash %}
{% if site.is_stable %}
    $ curl https://flink.apache.org/q/quickstart.sh | bash -s {{site.version}}
{% else %}
    $ curl https://flink.apache.org/q/quickstart-SNAPSHOT.sh | bash -s {{site.version}}
{% endif %}
    {% endhighlight %}

    </div>
    {% unless site.is_stable %}
    <p style="border-radius: 5px; padding: 5px" class="bg-danger">
        <b>注意</b>：Maven 3.0 及更高版本，不再支持通过命令行指定仓库（-DarchetypeCatalog）。
        如果你希望使用快照仓库，则需要在 settings.xml 文件中添加一个仓库条目。有关这个改动的详细信息，
        请参阅 <a href="http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html">Maven 官方文档</a>
    </p>
    {% endunless %}
</div>

### 检查项目

项目创建后，工作目录将多出一个新目录。如果你使用的是 _curl_ 方式创建项目，目录名为 `quickstart`；
如果你使用的是 _Maven archetypes_ 方式创建项目，则目录名为你指定的 `artifactId`：

{% highlight bash %}
$ tree quickstart/
quickstart/
├── pom.xml
└── src
    └── main
        ├── java
        │   └── org
        │       └── myorg
        │           └── quickstart
        │               ├── BatchJob.java
        │               └── StreamingJob.java
        └── resources
            └── log4j.properties
{% endhighlight %}

示例项目是一个 __Maven project__，它包含了两个类：_StreamingJob_ 和 _BatchJob_ 分别是 *DataStream* and *DataSet* 程序的基础骨架程序。
_main_ 方法是程序的入口，既可用于IDE测试/执行，也可用于部署。

我们建议你将 __此项目导入IDE__ 来开发和测试它。
IntelliJ IDEA 支持 Maven 项目开箱即用。如果你使用的是 Eclipse，使用[m2e 插件](http://www.eclipse.org/m2e/) 可以
[导入 Maven 项目](http://books.sonatype.com/m2eclipse-book/reference/creating-sect-importing-projects.html#fig-creating-import)。
一些 Eclipse 捆绑包默认包含该插件，其他情况需要你手动安装。

*请注意*：对 Flink 来说，默认的 JVM 堆内存可能太小，你应当手动增加堆内存。
在 Eclipse 中，选择 `Run Configurations -> Arguments` 并在 `VM Arguments` 对应的输入框中写入：`-Xmx800m`。
在 IntelliJ IDEA 中，推荐从菜单 `Help | Edit Custom VM Options` 来修改 JVM 选项。有关详细信息，请参阅[这篇文章](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties)。

### 构建项目

如果你想要 __构建/打包你的项目__，请在项目目录下运行 '`mvn clean package`' 命令。
命令执行后，你将 __找到一个JAR文件__，里面包含了你的应用程序，以及已作为依赖项添加到应用程序的连接器和库：`target/<artifact-id>-<version>.jar`。

__注意：__ 如果你使用其他类而不是 *StreamingJob* 作为应用程序的主类/入口，
我们建议你相应地修改 `pom.xml` 文件中的 `mainClass` 配置。这样，
Flink 可以从 JAR 文件运行应用程序，而无需另外指定主类。

## Gradle

### 环境要求

唯一的要求是使用 __Gradle 3.x__ (或更高版本) 和安装 __Java 8.x__ 。

### 创建项目

使用以下命令之一来 __创建项目__：

<ul class="nav nav-tabs" style="border-bottom: none;">
        <li class="active"><a href="#gradle-example" data-toggle="tab"><strong>Gradle 示例</strong></a></li>
    <li><a href="#gradle-script" data-toggle="tab">运行 <strong>quickstart 脚本</strong></a></li>
</ul>
<div class="tab-content">
    <div class="tab-pane active" id="gradle-example">

        <ul class="nav nav-tabs" style="border-bottom: none;">
            <li class="active"><a href="#gradle-build" data-toggle="tab"><tt>build.gradle</tt></a></li>
            <li><a href="#gradle-settings" data-toggle="tab"><tt>settings.gradle</tt></a></li>
        </ul>
        <div class="tab-content">
<!-- NOTE: Any change to the build scripts here should also be reflected in flink-web/q/gradle-quickstart.sh !! -->
            <div class="tab-pane active" id="gradle-build">
                {% highlight gradle %}
buildscript {
    repositories {
        jcenter() // this applies only to the Gradle 'Shadow' plugin
    }
    dependencies {
        classpath 'com.github.jengelman.gradle.plugins:shadow:2.0.4'
    }
}

plugins {
    id 'java'
    id 'application'
    // shadow plugin to produce fat JARs
    id 'com.github.johnrengelman.shadow' version '2.0.4'
}


// artifact properties
group = 'org.myorg.quickstart'
version = '0.1-SNAPSHOT'
mainClassName = 'org.myorg.quickstart.StreamingJob'
description = """Flink Quickstart Job"""

ext {
    javaVersion = '1.8'
    flinkVersion = '{{ site.version }}'
    scalaBinaryVersion = '{{ site.scala_version }}'
    slf4jVersion = '1.7.7'
    log4jVersion = '1.2.17'
}


sourceCompatibility = javaVersion
targetCompatibility = javaVersion
tasks.withType(JavaCompile) {
	options.encoding = 'UTF-8'
}

applicationDefaultJvmArgs = ["-Dlog4j.configuration=log4j.properties"]

task wrapper(type: Wrapper) {
    gradleVersion = '3.1'
}

// declare where to find the dependencies of your project
repositories {
    mavenCentral()
    maven { url "https://repository.apache.org/content/repositories/snapshots/" }
}

// 注意：我们不能使用 "compileOnly" 或者 "shadow" 配置，这会使我们无法在 IDE 中或通过使用 "gradle run" 命令运行代码。
// 我们也不能从 shadowJar 中排除传递依赖（请查看 https://github.com/johnrengelman/shadow/issues/159)。
// -> 显式定义我们想要包含在 "flinkShadowJar" 配置中的类库!
configurations {
    flinkShadowJar // dependencies which go into the shadowJar

    // 总是排除这些依赖（也来自传递依赖），因为 Flink 会提供这些依赖。
    flinkShadowJar.exclude group: 'org.apache.flink', module: 'force-shading'
    flinkShadowJar.exclude group: 'com.google.code.findbugs', module: 'jsr305'
    flinkShadowJar.exclude group: 'org.slf4j'
    flinkShadowJar.exclude group: 'log4j'
}

// declare the dependencies for your production and test code
dependencies {
    // --------------------------------------------------------------
    // 编译时依赖不应该包含在 shadow jar 中，
    // 这些依赖会在 Flink 的 lib 目录中提供。
    // --------------------------------------------------------------
    compile "org.apache.flink:flink-java:${flinkVersion}"
    compile "org.apache.flink:flink-streaming-java_${scalaBinaryVersion}:${flinkVersion}"

    // --------------------------------------------------------------
    // 应该包含在 shadow jar 中的依赖，例如：连接器。
    // 它们必须在 flinkShadowJar 的配置中！
    // --------------------------------------------------------------
    //flinkShadowJar "org.apache.flink:flink-connector-kafka-0.11_${scalaBinaryVersion}:${flinkVersion}"

    compile "log4j:log4j:${log4jVersion}"
    compile "org.slf4j:slf4j-log4j12:${slf4jVersion}"

    // Add test dependencies here.
    // testCompile "junit:junit:4.12"
}

// make compileOnly dependencies available for tests:
sourceSets {
    main.compileClasspath += configurations.flinkShadowJar
    main.runtimeClasspath += configurations.flinkShadowJar

    test.compileClasspath += configurations.flinkShadowJar
    test.runtimeClasspath += configurations.flinkShadowJar

    javadoc.classpath += configurations.flinkShadowJar
}

run.classpath = sourceSets.main.runtimeClasspath

jar {
    manifest {
        attributes 'Built-By': System.getProperty('user.name'),
                'Build-Jdk': System.getProperty('java.version')
    }
}

shadowJar {
    configurations = [project.configurations.flinkShadowJar]
}
                {% endhighlight %}
            </div>
            <div class="tab-pane" id="gradle-settings">
                {% highlight gradle %}
rootProject.name = 'quickstart'
                {% endhighlight %}
            </div>
        </div>
    </div>

    <div class="tab-pane" id="gradle-script">
    {% highlight bash %}
    bash -c "$(curl https://flink.apache.org/q/gradle-quickstart.sh)" -- {{site.version}} {{site.scala_version}}
    {% endhighlight %}
    这种方式允许你<strong>为新项目命名</strong>。它将以交互式的方式询问你的项目名称、组织机构（也用于包名）、项目版本、Scala 和 Flink 版本。
    </div>
</div>

### 检查项目

根据你提供的项目名称，工作目录中将多出一个新目录，例如 `quickstart`：

{% highlight bash %}
$ tree quickstart/
quickstart/
├── README
├── build.gradle
├── settings.gradle
└── src
    └── main
        ├── java
        │   └── org
        │       └── myorg
        │           └── quickstart
        │               ├── BatchJob.java
        │               └── StreamingJob.java
        └── resources
            └── log4j.properties
{% endhighlight %}

示例项目是一个 __Gradle 项目__，它包含了两个类：_StreamingJob_ 和 _BatchJob_ 是 *DataStream* 和 *DataSet* 程序的基础骨架程序。
_main_ 方法是程序的入口，即可用于IDE测试/执行，也可用于部署。

我们建议你将 __此项目导入你的 IDE__ 来开发和测试它。
IntelliJ IDEA 在安装 `Gradle` 插件后支持 Gradle 项目。Eclipse 则通过 [Eclipse Buildship](https://projects.eclipse.org/projects/tools.buildship) 插件支持 Gradle 项目（鉴于 `shadow` 插件对 Gradle 版本有要求，请确保在导入向导的最后一步指定 Gradle 版本 >= 3.0）。
你也可以使用 [Gradle’s IDE integration](https://docs.gradle.org/current/userguide/userguide.html#ide-integration) 从 Gradle 
创建项目文件。


*请注意*：对 Flink 来说，默认的 JVM 堆内存可能太小，你应当手动增加堆内存。
在 Eclipse中，选择 `Run Configurations -> Arguments` 并在 `VM Arguments` 对应的输入框中写入：`-Xmx800m`。
在 IntelliJ IDEA 中，推荐从菜单 `Help | Edit Custom VM Options` 来修改 JVM 选项。有关详细信息，请参阅[此文章](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties)。

### 构建项目

如果你想要 __构建/打包项目__，请在项目目录下运行 '`gradle clean shadowJar`' 命令。
命令执行后，你将 __找到一个 JAR 文件__，里面包含了你的应用程序，以及已作为依赖项添加到应用程序的连接器和库：`build/libs/<project-name>-<version>-all.jar`。 

__注意：__ 如果你使用其他类而不是 *StreamingJob* 作为应用程序的主类/入口，
我们建议你相应地修改 `build.gradle` 文件中的 `mainClassName` 配置。
这样，Flink 可以从 JAR 文件运行应用程序，而无需另外指定主类。

## 下一步

开始编写应用！

如果你准备编写流处理应用，正在寻找灵感来写什么，
可以看看[流处理应用程序教程]({{ site.baseurl }}/zh/getting-started/tutorials/datastream_api.html#writing-a-flink-program)

如果你准备编写批处理应用，正在寻找灵感来写什么，
可以看看[批处理应用程序示例]({{ site.baseurl }}/zh/dev/batch/examples.html)

有关 API 的完整概述，请查看
[DataStream API]({{ site.baseurl }}/zh/dev/datastream_api.html) 和
[DataSet API]({{ site.baseurl }}/zh/dev/batch/index.html) 章节。

在[这里]({{ site.baseurl }}/zh/getting-started/tutorials/local_setup.html)，你可以找到如何在 IDE 之外的本地集群中运行应用程序。

如果你有任何问题，请发信至我们的[邮箱列表](http://mail-archives.apache.org/mod_mbox/flink-user/)，我们很乐意提供帮助。

{% top %}
