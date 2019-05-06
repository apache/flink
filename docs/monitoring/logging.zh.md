---
title: "如何使用日志记录"
nav-title: 日志
nav-parent_id: monitoring
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

Flink 的日志记录是通过 slf4j 日志接口实现的。默认使用 log4j 作为日志框架，同时也提供 logback 配置文件并以属性形式传给 JVM。开发者如果想使用 logback 来代替 log4j 的话，只需要从依赖里排除或目录里删掉 log4j。

* This will be replaced by the TOC
{:toc}

## Log4j 配置

Log4j 是通过属性文件来配置的，在 Flink 中这个文件叫 `log4j.properties`。我们可以将文件名和文件路径通过 `-Dlog4j.configuration=` 传给JVM。 

Flink 默认会使用下面的 properties 文件：

- `log4j-cli.properties`: 在 Flink 命令行客户端中使用，比如 `flink run`，(在非集群环境下执行)
- `log4j-yarn-session.properties`: 在 Flink 命令行客户端开启 YARN session  (`yarn-session.sh`) 时使用
- `log4j.properties`: 在 JobManager 和 Taskmanager 日志记录时使用，包括 standalone 和 YARN 环境

## Logback 配置

对用户和开发者来说掌控日志框架非常重要，最好是仅通过配置文件就能完成日志框架的配置。配置文件既可以通过设置环境变量 `-Dlogback.configurationFile=<file>` 来指定，又可以通过添加 `logback.xml` 来指定。`conf` 目录里的 `logback.xml` 文件是可以修改的，如果 Flink 是在 IDE 之外通过附带的 shell 脚本启动的话，会使用这个配置文件。
附带的 `logback.xml` 格式如下：

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

通过添加以下配置文件可以控制 `org.apache.flink.runtime.jobgraph.JobGraph` 的日志级别。

{% highlight xml %}
<logger name="org.apache.flink.runtime.jobgraph.JobGraph" level="DEBUG"/>
{% endhighlight %}

获取 logback 更多配置信息可以参考 [LOGBack 配置手册](http://logback.qos.ch/manual/configuration.html)。

## 面向开发者的最佳实践

在调用的时候创建 slf4j logger。

{% highlight java %}
import org.slf4j.LoggerFactory
import org.slf4j.Logger

Logger LOG = LoggerFactory.getLogger(Foobar.class)
{% endhighlight %}

使用 slf4j 时推荐使用它的占位符机制。使用占位符可以避免不必要的字符串拼接，比如日志级别太高以至于有些日志信息不会被记录。占位符的语法如下所示：

{% highlight java %}
LOG.info("This message contains {} placeholders. {}", 2, "Yippie");
{% endhighlight %}

占位符也可以跟异常结合使用。

{% highlight java %}
catch(Exception exception){
	LOG.error("An {} occurred.", "error", exception);
}
{% endhighlight %}

{% top %}
