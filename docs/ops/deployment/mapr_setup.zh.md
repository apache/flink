---
title:  "MapR 安装"
nav-title: MapR
nav-parent_id: deployment
nav-pos: 7
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

This documentation provides instructions on how to prepare Flink for YARN
executions on a [MapR](https://mapr.com/) cluster.

* This will be replaced by the TOC
{:toc}

## Running Flink on YARN with MapR

The instructions below assume MapR version 5.2.0. They will guide you
to be able to start submitting [Flink on YARN]({{ site.baseurl }}/ops/deployment/yarn_setup.html)
jobs or sessions to a MapR cluster.

### Building Flink for MapR

In order to run Flink on MapR, Flink needs to be built with MapR's own
Hadoop and Zookeeper distribution. Simply build Flink using Maven with
the following command from the project root directory:

{% highlight bash %}
mvn clean install -DskipTests -Pvendor-repos,mapr \
    -Dhadoop.version=2.7.0-mapr-1607 \
    -Dzookeeper.version=3.4.5-mapr-1604
{% endhighlight %}

The `vendor-repos` build profile adds MapR's repository to the build so that
MapR's Hadoop / Zookeeper dependencies can be fetched. The `mapr` build
profile additionally resolves some dependency clashes between MapR and
Flink, as well as ensuring that the native MapR libraries on the cluster
nodes are used. Both profiles must be activated.

By default the `mapr` profile builds with Hadoop / Zookeeper dependencies
for MapR version 5.2.0, so you don't need to explicitly override
the `hadoop.version` and `zookeeper.version` properties.
For different MapR versions, simply override these properties to appropriate
values. The corresponding Hadoop / Zookeeper distributions for each MapR version
can be found on MapR documentations such as
[here](http://maprdocs.mapr.com/home/DevelopmentGuide/MavenArtifacts.html).

### Job Submission Client Setup

The client submitting Flink jobs to MapR also needs to be prepared with the below setups.

Ensure that MapR's JAAS config file is picked up to avoid login failures:

{% highlight bash %}
export JVM_ARGS=-Djava.security.auth.login.config=/opt/mapr/conf/mapr.login.conf
{% endhighlight %}

Make sure that the `yarn.nodemanager.resource.cpu-vcores` property is set in `yarn-site.xml`:

{% highlight xml %}
<!-- in /opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop/yarn-site.xml -->

<configuration>
...

<property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>...</value>
</property>

...
</configuration>
{% endhighlight %}

Also remember to set the `YARN_CONF_DIR` or `HADOOP_CONF_DIR` environment
variables to the path where `yarn-site.xml` is located:

{% highlight bash %}
export YARN_CONF_DIR=/opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop/
export HADOOP_CONF_DIR=/opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop/
{% endhighlight %}

Make sure that the MapR native libraries are picked up in the classpath:

{% highlight bash %}
export FLINK_CLASSPATH=/opt/mapr/lib/*
{% endhighlight %}

If you'll be starting Flink on YARN sessions with `yarn-session.sh`, the
below is also required:

{% highlight bash %}
export CC_CLASSPATH=/opt/mapr/lib/*
{% endhighlight %}

## Running Flink with a Secured MapR Cluster

*Note: In Flink 1.2.0, Flink's Kerberos authentication for YARN execution has
a bug that forbids it to work with MapR Security. Please upgrade to later Flink
versions in order to use Flink with a secured MapR cluster. For more details,
please see [FLINK-5949](https://issues.apache.org/jira/browse/FLINK-5949).*

Flink's [Kerberos authentication]({{ site.baseurl }}/ops/security-kerberos.html) is independent of
[MapR's Security authentication](http://maprdocs.mapr.com/home/SecurityGuide/Configuring-MapR-Security.html).
With the above build procedures and environment variable setups, Flink
does not require any additional configuration to work with MapR Security.

Users simply need to login by using MapR's `maprlogin` authentication
utility. Users that haven't acquired MapR login credentials would not be
able to submit Flink jobs, erroring with:

{% highlight plain %}
java.lang.Exception: unable to establish the security context
Caused by: o.a.f.r.security.modules.SecurityModule$SecurityInstallException: Unable to set the Hadoop login user
Caused by: java.io.IOException: failure to login: Unable to obtain MapR credentials
{% endhighlight %}

{% top %}
