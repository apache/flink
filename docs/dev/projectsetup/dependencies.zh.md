---
title: "配置依赖、连接器、类库"
nav-parent_id: projectsetup
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

每个 Flink 应用都需要依赖一组 Flink 类库。Flink 应用至少需要依赖 Flink APIs。许多应用还会额外依赖连接器类库(比如 Kafka、Cassandra 等)。
当用户运行 Flink 应用时(无论是在 IDE 环境下进行测试，还是部署在分布式环境下)，运行时类库都必须可用。

## Flink 核心依赖以及应用依赖

与其他运行用户自定义应用的大多数系统一样，Flink 中有两大类依赖类库

  - **Flink 核心依赖**：Flink 本身包含运行所需的一组类和依赖，比如协调、网络通讯、checkpoint、容错处理、API、算子(如窗口操作)、
    资源管理等，这些类和依赖形成了 Flink 运行时的核心。当 Flink 应用启动时，这些依赖必须可用。

    这些核心类和依赖被打包在 `flink-dist` jar 里。它们是 Flink `lib` 文件夹下的一部分，也是 Flink 基本容器镜像的一部分。
    这些依赖类似 Java `String` 和 `List` 的核心类库(`rt.jar`, `charsets.jar`等)。
    
    Flink 核心依赖不包含连接器和类库（如 CEP、SQL、ML 等），这样做的目的是默认情况下避免在类路径中具有过多的依赖项和类。
    实际上，我们希望尽可能保持核心依赖足够精简，以保证一个较小的默认类路径，并且避免依赖冲突。

  - **用户应用依赖** 是指特定的应用程序需要的类库，如连接器，formats等。

    用户应用代码和所需的连接器以及其他类库依赖通常被打包到 *application jar* 中。

    用户应用程序依赖项不需包括 Flink DataSet / DataStream API 以及运行时依赖项，因为它们已经是 Flink 核心依赖项的一部分。

## 搭建一个项目: 基础依赖

开发 Flink 应用程序需要最低限度的 API 依赖。Maven 用户，可以使用 
[Java 项目模板]({{ site.baseurl }}/zh/dev/projectsetup/java_api_quickstart.html)或者
[Scala 项目模板]({{ site.baseurl }}/zh/dev/projectsetup/scala_api_quickstart.html)来创建一个包含最初依赖的程序骨架。

手动设置项目时，需要为 Java 或 Scala API 添加以下依赖项（这里以 Maven 语法为例，但也适用于其他构建工具（Gradle、 SBT 等））。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-java</artifactId>
  <version>{{site.version }}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
</div>

**注意事项:** 所有这些依赖项的作用域都应该设置为 *provided* 。
这意味着需要这些依赖进行编译，但不应将它们打包到项目生成的应用程序jar文件中-- 
因为这些依赖项是 Flink 的核心依赖，在应用启动前已经是可用的状态了。

我们强烈建议保持这些依赖的作用域为 *provided* 。 如果它们的作用域未设置为 *provided* ，则典型的情况是因为包含了 Flink 的核心依赖而导致生成的jar包变得过大。
最糟糕的情况是添加到应用程序的 Flink 核心依赖项与你自己的一些依赖项版本冲突（通常通过反向类加载来避免）。

**IntelliJ 上的一些注意事项:** 为了可以让 Flink 应用在 IntelliJ IDEA 中运行，这些 Flink 核心依赖的作用域需要设置为 *compile* 而不是 *provided* 。
否则 IntelliJ 不会添加这些依赖到 classpath，会导致应用运行时抛出 `NoClassDefFountError` 异常。为了避免声明这些依赖的作用域为 *compile* (因为我们不推荐这样做)，
上文给出的 Java 和 Scala 项目模板使用了一个小技巧：添加了一个 profile，仅当应用程序在 IntelliJ 中运行时该 profile 才会被激活，
然后将依赖作用域设置为 *compile* ，从而不影响应用 jar 包。

## 添加连接器以及类库依赖

大多数应用需要依赖特定的连接器或其他类库，例如 Kafka、Cassandra 的连接器等。这些连接器不是 Flink 核心依赖的一部分，因此必须作为依赖项手动添加到应用程序中。

下面是添加 Kafka 0.10 连接器依赖（Maven 语法）的示例：
{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka-0.10{{ site.scala_version_suffix }}</artifactId>
    <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

我们建议将应用程序代码及其所有需要的依赖项打包到一个 *jar-with-dependencies* 的 jar 包中。
这个打包好的应用 jar 可以提交到已经运行的 Flink 集群中，或者添加到 Flink 应用容器镜像中。
 
通过[Java 项目模板]({{ site.baseurl }}/zh/dev/projectsetup/java_api_quickstart.html) 或者
[Scala 项目模板]({{ site.baseurl }}/zh/dev/projectsetup/scala_api_quickstart.html) 创建的应用，
当使用命令 `mvn clean package` 打包的时候会自动将应用依赖类库打包进应用 jar 包。
对于不是通过上面模板创建的应用，我们推荐添加 Maven Shade Plugin 去构建应用。(下面的附录会给出具体配置)

**注意:** 要使 Maven（以及其他构建工具）正确地将依赖项打包到应用程序 jar 中，必须将这些依赖项的作用域设置为 *compile* （与核心依赖项不同，后者作用域应该设置为 *provided* ）。

## Scala 版本

Scala 版本(2.10、2.11、2.12等)互相是不兼容的。因此，依赖 Scala 2.11 的 Flink 环境是不可以运行依赖 Scala 2.12 应用的。

所有依赖 Scala 的 Flink 类库都以它们依赖的 Scala 版本为后缀，例如 `flink-streaming-scala_2.11`。

只使用 Java 的开发人员可以选择任何 Scala 版本，Scala 开发人员需要选择与其应用程序相匹配的 Scala 版本。

对于指定的 Scala 版本如何构建 Flink 应用可以参考 [构建指南]({{ site.baseurl }}/zh/flinkDev/building.html#scala-versions)。

## Hadoop 版本

**一般规则：永远不需要将 Hadoop 依赖项直接添加到你的应用程序中** 
*(唯一的例外是使用 Flink 的 Hadoop 兼容包装器来处理 Hadoop 格式的输入/输出时)*

如果你想要在 Flink 应用中使用 Hadoop，你需要使用包含 Hadoop 依赖的 Flink，而非将 Hadoop 作为应用依赖进行添加。
请参考[Hadoop 构建指南]({{ site.baseurl }}/zh/ops/deployment/hadoop.html)

这样设计是出于两个主要原因:

  - 可能在用户程序启动之前，一些 Hadoop 交互操作就已经发生在 Flink 核心中了，比如为 checkpoint 设置 HDFS 路径，通过 Hadoop's Kerberos tokens 进行权限认证以及进行 YARN 部署等。

  - Flink 的反向类加载方法隐藏了核心依赖关系中的许多传递依赖关系。这不仅适用于 Flink 自己的核心依赖项，也适用于 Hadoop 在启动中存在的依赖项。
    通过这种方式，应用程序可以使用相同依赖项的不同版本，而不会引起依赖项的冲突（若非如此可能会引起严重依赖问题，因为 Hadoops 依赖树十分庞大。）

如果在 IDE 内部进行开发或测试的过程中需要 Hadoop 依赖项（例如用于 HDFS 访问），请将这些依赖项的作用域设置为 *test* 或 *provided* 。

## 附录：构建带有依赖的应用 jar 包模板

可以通过下面的 shade plugin 配置来构建包含所有依赖项的应用 jar 包

{% highlight xml %}
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.0.0</version>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                    <configuration>
                        <artifactSet>
                            <excludes>
                                <exclude>com.google.code.findbugs:jsr305</exclude>
                                <exclude>org.slf4j:*</exclude>
                                <exclude>log4j:*</exclude>
                            </excludes>
                        </artifactSet>
                        <filters>
                            <filter>
                                <!--不要拷贝 META-INF 目录下的签名，
                                否则会引起 SecurityExceptions 。 -->
                                <artifact>*:*</artifact>
                                <excludes>
                                    <exclude>META-INF/*.SF</exclude>
                                    <exclude>META-INF/*.DSA</exclude>
                                    <exclude>META-INF/*.RSA</exclude>
                                </excludes>
                            </filter>
                        </filters>
                        <transformers>
                            <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                <mainClass>my.programs.main.clazz</mainClass>
                            </transformer>
                        </transformers>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
{% endhighlight %}

{% top %}
