---
title: "如何使用日志记录"
nav-title: 日志
nav-parent_id: advanced
nav-pos: 3
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

Flink 中的日志记录是使用 slf4j 日志接口实现的。使用 log4j2 作为底层日志框架。我们也支持了 logback 日志配置，只要将其配置文件作为参数传递给 JVM 即可。愿意使用 logback 而不是 log4j2 的用户只需排除 log4j2 的依赖（或从 lib/ 文件夹中删除它）即可。

* This will be replaced by the TOC
{:toc}

<a name="configuring-log4j2"></a>

## 配置 Log4j2

Log4j2 是使用配置文件指定的。在 Flink 的使用中，该文件通常命名为 `log4j.properties`。我们使用 `-Dlog4j.configurationFile=` 参数将该文件的文件名和位置传递给 JVM。

Flink 附带以下默认日志配置文件：

- `log4j-cli.properties`：由 Flink 命令行客户端使用（例如 `flink run`）（不包括在集群上执行的代码）
- `log4j-session.properties`：Flink 命令行客户端在启动 YARN 或 Kubernetes session 时使用（`yarn-session.sh`，`kubernetes-session.sh`）
- `log4j.properties`：作为 JobManager/TaskManager 日志配置使用（standalone 和 YARN 两种模式下皆使用）

<a name="compatibility-with-log4j1"></a>

### 与 Log4j1 的兼容性

Flink 附带了 [Log4j API bridge](https://logging.apache.org/log4j/log4j-2.2/log4j-1.2-api/index.html)，使得现有作业能够继续使用 log4j1 的接口。

如果你有基于 Log4j 的自定义配置文件或代码，请查看官方 Log4j [兼容性](https://logging.apache.org/log4j/2.x/manual/compatibility.html)和[迁移](https://logging.apache.org/log4j/2.x/manual/migration.html)指南。

<a name="configuring-log4j1"></a>

## 配置 Log4j1

要将 Flink 与 Log4j1 一起使用，必须确保：
- Classpath 中不存在 `org.apache.logging.log4j:log4j-core`，`org.apache.logging.log4j:log4j-slf4j-impl` 和 `org.apache.logging.log4j:log4j-1.2-api`，
- 且 Classpath 中存在 `log4j:log4j`，`org.slf4j:slf4j-log4j12`，`org.apache.logging.log4j:log4j-to-slf4j` 和 `org.apache.logging.log4j:log4j-api`。

在 IDE 中使用 log4j1，你必须在 pom 文件中使用上述 `Classpath 中存在的 jars` 依赖项替换 `Classpath 中不存在的 jars` 依赖项，并尽可能在传递依赖于 `Classpath 中不存在的 jars` 的依赖项上添加排除 `Classpath 中不存在的 jars` 配置。

对于 Flink 发行版，这意味着你必须
- 从 `lib` 目录中移除 `log4j-core`，`log4j-slf4j-impl` 和 `log4j-1.2-api` jars，
- 向 `lib` 目录中添加 `log4j`，`slf4j-log4j12` 和 `log4j-to-slf4j` jars，
- 用兼容的 Log4j1 版本替换 `conf` 目录中的所有 log4j 配置文件。

<a name="configuring-logback"></a>

## 配置 logback

对于用户和开发人员来说，控制日志框架非常重要。日志框架的配置完全由配置文件完成。必须通过设置环境参数 `-Dlogback.configurationFile=<file>` 或将 `logback.xml` 放在 classpath 中来指定配置文件。`conf` 目录包含一个 `logback.xml` 文件，该文件可以修改，如果使用附带的启动脚本在 IDE 之外启动 Flink 则会使用该日志配置文件。提供的 `logback.xml` 具有以下格式：

{% highlight xml %}
<configuration>
    <appender name="file" class="ch.qos.logback.core.FileAppender">
        <file>${log.file}</file>
        <append>false</append>
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{60} %X{sourceThread} - %msg%n</pattern>
        </encoder>
    </appender>

    <root level="INFO">
        <appender-ref ref="file"/>
    </root>
</configuration>
{% endhighlight %}

例如，为了控制 `org.apache.flink.runtime.jobgraph.JobGraph` 的日志记录级别，必须将以下行添加到配置文件中。

{% highlight xml %}
<logger name="org.apache.flink.runtime.jobgraph.JobGraph" level="DEBUG"/>
{% endhighlight %}

有关配置日志的更多信息，请参见 [LOGback 手册](http://logback.qos.ch/manual/configuration.html)。

<a name="best-practices-for-developers"></a>

## 开发人员的最佳实践

Slf4j 的 loggers 通过调用 `LoggerFactory` 的 `getLogger()` 方法创建

{% highlight java %}
import org.slf4j.LoggerFactory
import org.slf4j.Logger

Logger LOG = LoggerFactory.getLogger(Foobar.class)
{% endhighlight %}

为了最大限度地利用 slf4j，建议使用其占位符机制。使用占位符可以避免不必要的字符串构造，以防日志级别设置得太高而不会记录消息。占位符的语法如下：

{% highlight java %}
LOG.info("This message contains {} placeholders. {}", 2, "Yippie");
{% endhighlight %}

占位符也可以和要记录的异常一起使用。

{% highlight java %}
catch(Exception exception){
	LOG.error("An {} occurred.", "error", exception);
}
{% endhighlight %}

{% top %}
