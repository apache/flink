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

The logging in Flink is implemented using the slf4j logging interface. As underlying logging framework, log4j is used. We also provide logback configuration files and pass them to the JVM's as properties. Users willing to use logback instead of log4j can just exclude log4j (or delete it from the lib/ folder).

* This will be replaced by the TOC
{:toc}

## Configuring Log4j

Log4j is controlled using property files. In Flink's case, the file is usually called `log4j.properties`. We pass the filename and location of this file using the `-Dlog4j.configuration=` parameter to the JVM.

Flink ships with the following default properties files:

- `log4j-cli.properties`: Used by the Flink command line client (e.g. `flink run`) (not code executed on the cluster)
- `log4j-yarn-session.properties`: Used by the Flink command line client when starting a YARN session (`yarn-session.sh`)
- `log4j.properties`: JobManager/Taskmanager logs (both standalone and YARN)

## Configuring logback

For users and developers alike it is important to control the logging framework.
The configuration of the logging framework is exclusively done by configuration files.
The configuration file either has to be specified by setting the environment property `-Dlogback.configurationFile=<file>` or by putting `logback.xml` in the classpath.
The `conf` directory contains a `logback.xml` file which can be modified and is used if Flink is started outside of an IDE and with the provided starting scripts.
The provided `logback.xml` has the following form:

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

In order to control the logging level of `org.apache.flink.runtime.jobgraph.JobGraph`, for example, one would have to add the following line to the configuration file.

{% highlight xml %}
<logger name="org.apache.flink.runtime.jobgraph.JobGraph" level="DEBUG"/>
{% endhighlight %}

For further information on configuring logback see [LOGback's manual](http://logback.qos.ch/manual/configuration.html).

## Best practices for developers

The loggers using slf4j are created by calling

{% highlight java %}
import org.slf4j.LoggerFactory
import org.slf4j.Logger

Logger LOG = LoggerFactory.getLogger(Foobar.class)
{% endhighlight %}

In order to benefit most from slf4j, it is recommended to use its placeholder mechanism.
Using placeholders allows to avoid unnecessary string constructions in case that the logging level is set so high that the message would not be logged.
The syntax of placeholders is the following:

{% highlight java %}
LOG.info("This message contains {} placeholders. {}", 2, "Yippie");
{% endhighlight %}

Placeholders can also be used in conjunction with exceptions which shall be logged.

{% highlight java %}
catch(Exception exception){
	LOG.error("An {} occurred.", "error", exception);
}
{% endhighlight %}

{% top %}
