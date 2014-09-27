---
title: "How to use logging"
---

* This will be replaced by the TOC
{:toc}

The logging in Flink is implemented using the slf4j logging interface. As underlying logging framework, logback is used.

## Configuring logback

For users and developers alike it is important to control the logging framework. 
The configuration of the logging framework is exclusively done by configuration files.
The configuration file either has to be specified by setting the environment property `-Dlogback.configurationFile=<file>` or by putting `logback.xml` in the classpath.
The `conf` directory contains a `logback.xml` file which can be modified and is used if Flink is started outside of an IDE and with the provided starting scripts.
The provided `logback.xml` has the following form:

~~~ xml
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
~~~

In order to control the logging level of `org.apache.flink.runtime.jobgraph.JobGraph`, for example, one would have to add the following line to the configuration file.

~~~ xml
<logger name="org.apache.flink.runtime.jobgraph.JobGraph" level="DEBUG"/>
~~~

For further information on configuring logback see [LOGback's manual](http://logback.qos.ch/manual/configuration.html).

## Best practices for developers

The loggers using slf4j are created by calling

~~~ java
import org.slf4j.LoggerFactory
import org.slf4j.Logger

Logger LOG = LoggerFactory.getLogger(Foobar.class)
~~~

In order to benefit most from slf4j, it is recommended to use its placeholder mechanism.
Using placeholders allows to avoid unnecessary string constructions in case that the logging level is set so high that the message would not be logged.
The syntax of placeholders is the following:

~~~ java
LOG.info("This message contains {} placeholders. {}", 2, "Yippie");
~~~

Placeholders can also be used in conjunction with exceptions which shall be logged.

~~~ java
catch(Exception exception){
	LOG.error("An {} occurred.", "error", exception);
}
~~~