---
title: "How to use logging"
nav-title: Logging
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

All Flink processes create a log text file that contains messages for various events happening in that process.
These logs provide deep insights into the inner workings of Flink, and can be used to detect problems (in the form of WARN/ERROR messages) and can help in debugging them.

The log files can be accessed via the Job-/TaskManager pages of the WebUI. The used [Resource Provider]({% link deployment/resource-providers/index.md %}) (e.g., YARN) may provide additional means of accessing them.

The logging in Flink uses the [SLF4J](http://www.slf4j.org/) logging interface.
This allows you to use any logging framework that supports SLF4J, without having to modify the Flink source code.

By default, [Log4j 2](https://logging.apache.org/log4j/2.x/index.html) is used as the underlying logging framework.

* This will be replaced by the TOC
{:toc}

## Configuring Log4j 2

Log4j 2 is controlled using property files.

The Flink distribution ships with the following log4j properties files in the `conf` directory, which are used automatically if Log4j 2 is enabled:

- `log4j-cli.properties`: used by the command line interface (e.g., `flink run`)
- `log4j-session.properties`: used by the command line interface when starting a Kubernetes/Yarn session cluster (i.e., `kubernetes-session.sh`/`yarn-session.sh`)
- `log4j-console.properties`: used for Job-/TaskManagers if they are run in the foreground (e.g., Kubernetes)
- `log4j.properties`: used for Job-/TaskManagers by default

Log4j periodically scans this file for changes and adjusts the logging behavior if necessary.
By default this check happens every 30 seconds and is controlled by the `monitorInterval` setting in the Log4j properties files.

### Compatibility with Log4j 1

Flink ships with the [Log4j API bridge](https://logging.apache.org/log4j/log4j-2.2/log4j-1.2-api/index.html), allowing existing applications that work against Log4j1 classes to continue working.

If you have custom Log4j 1 properties files or code that relies on Log4j 1, please check out the official Log4j [compatibility](https://logging.apache.org/log4j/2.x/manual/compatibility.html) and [migration](https://logging.apache.org/log4j/2.x/manual/migration.html) guides.

## Configuring Log4j1

To use Flink with [Log4j 1](https://logging.apache.org/log4j/1.2/) you must ensure that:
- `org.apache.logging.log4j:log4j-core`, `org.apache.logging.log4j:log4j-slf4j-impl` and `org.apache.logging.log4j:log4j-1.2-api` are not on the classpath,
- `log4j:log4j`, `org.slf4j:slf4j-log4j12`, `org.apache.logging.log4j:log4j-to-slf4j` and `org.apache.logging.log4j:log4j-api` are on the classpath.

In the IDE this means you have to replace such dependencies defined in your pom, and possibly add exclusions on dependencies that transitively depend on them.

For Flink distributions this means you have to
- remove the `log4j-core`, `log4j-slf4j-impl` and `log4j-1.2-api` jars from the `lib` directory,
- add the `log4j`, `slf4j-log4j12` and `log4j-to-slf4j` jars to the `lib` directory,
- replace all log4j properties files in the `conf` directory with Log4j1-compliant versions.

## Configuring logback

To use Flink with [logback](https://logback.qos.ch/) you must ensure that:
- `org.apache.logging.log4j:log4j-slf4j-impl` is not on the classpath,
- `ch.qos.logback:logback-core` and `ch.qos.logback:logback-classic` are on the classpath.

In the IDE this means you have to replace such dependencies defined in your pom, and possibly add exclusions on dependencies that transitively depend on them.

For Flink distributions this means you have to
- remove the `log4j-slf4j-impl` jar from the `lib` directory,
- add the `logback-core`, and `logback-classic` jars to the `lib` directory.

The Flink distribution ships with the following logback configuration files in the `conf` directory, which are used automatically if logback is enabled:
- `logback-session.properties`: used by the command line interface when starting a Kubernetes/Yarn session cluster (i.e., `kubernetes-session.sh`/`yarn-session.sh`)
- `logback-console.properties`: used for Job-/TaskManagers if they are run in the foreground (e.g., Kubernetes)
- `logback.xml`: used for command line interface and Job-/TaskManagers by default

<div class="alert alert-info" markdown="span">
  <strong>Note:</strong> Logback 1.3+ requires SLF4J 2, which is currently not supported.
</div>

## Best practices for developers

You can create an SLF4J logger by calling `org.slf4j.LoggerFactory#LoggerFactory.getLogger` with the `Class` of your class as an argument.

We highly recommend storing this logger in a `private static final` field.

{% highlight java %}
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Foobar {
	private static final Logger LOG = LoggerFactory.getLogger(Foobar.class);

	public static void main(String[] args) {
		LOG.info("Hello world!");
	}
}
{% endhighlight %}

In order to benefit most from SLF4J, it is recommended to use its placeholder mechanism.
Using placeholders allows avoiding unnecessary string constructions in case that the logging level is set so high that the message would not be logged.

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
