---
title: "How to use logging"
nav-title: Logging
nav-parent_id: deployment
nav-pos: 18
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

The logging in Flink is implemented using the slf4j logging interface. As underlying logging framework, log4j2 is used. We also provide logback configuration files and pass them to the JVM's as properties. Users willing to use logback instead of log4j2 can just exclude log4j2 (or delete it from the lib/ folder).

* This will be replaced by the TOC
{:toc}

## Configuring Log4j2

Log4j2 is controlled using property files. In Flink's case, the file is usually called `log4j.properties`. We pass the filename and location of this file using the `-Dlog4j.configurationFile=` parameter to the JVM.

Flink ships with the following default properties files:

- `log4j-cli.properties`: Used by the Flink command line client (e.g. `flink run`) (not code executed on the cluster)
- `log4j-session.properties`: Used by the Flink command line client when starting a YARN or Kubernetes session (`yarn-session.sh`, `kubernetes-session.sh`)
- `log4j.properties`: JobManager/Taskmanager logs (both standalone and YARN)

### Compatibility with Log4j1

Flink ships with the [Log4j API bridge](https://logging.apache.org/log4j/log4j-2.2/log4j-1.2-api/index.html), allowing existing applications that work against Log4j1 classes to continue working.

If you have custom Log4j1 properties files or code that relies on Log4j1, please check out the official Log4j [compatibility](https://logging.apache.org/log4j/2.x/manual/compatibility.html) and [migration](https://logging.apache.org/log4j/2.x/manual/migration.html) guides.

## Configuring Log4j1

To use Flink with Log4j1 you must ensure that:
- `org.apache.logging.log4j:log4j-core`, `org.apache.logging.log4j:log4j-slf4j-impl` and `org.apache.logging.log4j:log4j-1.2-api` are not on the classpath,
- `log4j:log4j`, `org.slf4j:slf4j-log4j12`, `org.apache.logging.log4j:log4j-to-slf4j` and `org.apache.logging.log4j:log4j-api` are on the classpath.

In the IDE this means you have to replace such dependencies defined in your pom, and possibly add exclusions on dependencies that transitively depend on them.

For Flink distributions this means you have to
- remove the `log4j-core`, `log4j-slf4j-impl` and `log4j-1.2-api` jars from the `lib` directory,
- add the `log4j`, `slf4j-log4j12` and `log4j-to-slf4j` jars to the `lib` directory,
- replace all log4j properties files in the `conf` directory with Log4j1-compliant versions.

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
