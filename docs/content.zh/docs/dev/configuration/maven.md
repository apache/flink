---
title: "使用 Maven"
weight: 2
type: docs
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

# 如何使用 Maven 配置您的项目

本指南将向您展示如何使用 [Maven](https://maven.apache.org) 配置 Flink 作业项目，Maven是 由 Apache Software Foundation 开源的自动化构建工具，使您能够构建、发布和部署项目。您可以使用它来管理软件项目的整个生命周期。

## 要求

- Maven 3.0.4 (or higher)
- Java 8 (deprecated) or Java 11

## 将项目导入 IDE

创建[项目目录和文件]({{< ref "docs/dev/configuration/overview#getting-started" >}})后，我们建议您将此项目导入到 IDE 进行开发和测试。

IntelliJ IDEA 支持开箱即用的 Maven 项目。Eclipse 提供了 [m2e 插件](http://www.eclipse.org/m2e/) 来[导入 Maven 项目](http://books.sonatype.com/m2eclipse-book/reference/creating-sect-importing-projects.html#fig-creating-import)。

**注意：** Java 的默认 JVM 堆大小对于 Flink 来说可能太小，您应该手动增加它。在 Eclipse 中，选中 `Run Configurations -> Arguments` 并在 `VM Arguments` 框里填上：`-Xmx800m`。在 IntelliJ IDEA 中，推荐选中 `Help | Edit Custom VM Options` 菜单修改 JVM 属性。详情请查阅[本文](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties)。

**关于 IntelliJ 的注意事项：** 要使应用程序在 IntelliJ IDEA 中运行，需要在运行配置中的 Include dependencies with "Provided" scope` 打勾。如果此选项不可用（可能是由于使用了较旧的 IntelliJ IDEA 版本），可创建一个调用应用程序 `main()` 方法的测试用例。

## 构建项目

如果您想 __构建/打包__ 您的项目，请转到您的项目目录并运行 '`mvn clean package`' 命令。您将 __找到一个 JAR 文件__，其中包含您的应用程序（还有已作为依赖项添加到应用程序的连接器和库）：`target/<artifact-id>-<version>.jar`。

__注意：__ 如果您使用不同于 `DataStreamJob` 的类作为应用程序的主类/入口点，我们建议您对 `pom.xml` 文件里的 `mainClassName` 配置进行相应的修改。这样，Flink 可以通过 JAR 文件运行应用程序，而无需额外指定主类。

## 向项目添加依赖项

打开您项目目录的 `pom.xml`，在 `dependencies` 标签内添加依赖项。

例如，您可以用如下方式添加 Kafka 连接器依赖：

```xml
<dependencies>
    
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka</artifactId>
        <version>{{< version >}}</version>
    </dependency>
    
</dependencies>
```

然后在命令行执行 `mvn install`。

当您在由 `Java Project Template`、`Scala Project Template` 或 Gradle 创建出来的项目里，运行 `mvn clean package` 会自动将应用程序依赖打包进应用程序 JAR。对于不是通过这些模板创建的项目，我们建议使用 Maven Shade 插件以将所有必需的依赖项打包进应用程序 jar。

**重要提示：** 请注意，应将所有这些（核心）依赖项的生效范围置为 [*provided*](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#dependency-scope)。这意味着需要对它们进行编译，但不应将它们打包进项目生成的应用程序 JAR 文件中。如果不设置为 *provided*，最好的情况是生成的 JAR 变得过大，因为它还包含所有 Flink 核心依赖项。最坏的情况是添加到应用程序 JAR 文件中的 Flink 核心依赖项与您自己的一些依赖项的版本冲突（通常通过反向类加载来避免）。

要将依赖项正确地打包进应用程序 JAR 中，必须把应用程序依赖项的生效范围设置为 *compile* 。

## 打包应用程序

在部署应用到 Flink 环境之前，您需要根据使用场景用不同的方式打包 Flink 应用程序。

如果您想为 Flink 作业创建 JAR 并且只使用 Flink 依赖而不使用任何第三方依赖（比如使用 JSON 格式的文件系统连接器），您不需要创建一个 uber/fat JAR 或将任何依赖打进包。

如果您想为 Flink 作业创建 JAR 并使用未内置在 Flink 发行版中的外部依赖项，您可以将它们添加到发行版的类路径中，或者将它们打包进您的 uber/fat 应用程序 JAR 中。

您可以将生成的 uber/fat JAR 提交到本地或远程集群：

```sh
bin/flink run -c org.example.MyJob myFatJar.jar
```

要了解有关如何部署 Flink 作业的更多信息，请查看[部署指南]({{< ref "docs/deployment/cli" >}})。

## 创建包含依赖项的 uber/fat JAR 的模板

为构建一个包含所有必需的连接器、 类库依赖项的应用程序 JAR，您可以使用如下 shade 插件定义：

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.1.1</version>
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
                            </excludes>
                        </artifactSet>
                        <filters>
                            <filter>
                                <!-- Do not copy the signatures in the META-INF folder.
                                Otherwise, this might cause SecurityExceptions when using the JAR. -->
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
                                <!-- Replace this with the main class of your job -->
                                <mainClass>my.programs.main.clazz</mainClass>
                            </transformer>
                            <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                        </transformers>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

[Maven shade 插件](https://maven.apache.org/plugins/maven-shade-plugin/index.html) 默认会包含所有的生效范围是 "runtime" 或 "compile" 的依赖项。
