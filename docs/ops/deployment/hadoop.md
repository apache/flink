---
title:  "Hadoop Integration"
nav-title: Hadoop Integration
nav-parent_id: deployment
nav-pos: 8
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


## Providing Hadoop classes

In order to use Hadoop features (e.g., YARN, HDFS) it is necessary to provide Flink with the required Hadoop classes,
as these are not bundled by default.

The recommended approach is adding the Hadoop classpath to Flink through the `HADOOP_CLASSPATH` environment variable.

Flink will use the environment variable `HADOOP_CLASSPATH` to augment the
classpath that is used when starting Flink components such as the Client,
JobManager, or TaskManager. Most Hadoop distributions and cloud environments
will not set this variable by default so if the Hadoop classpath should be
picked up by Flink the environment variable must be exported on all machines
that are running Flink components.

When running on YARN, this is usually not a problem because the components
running inside YARN will be started with the Hadoop classpaths, but it can
happen that the Hadoop dependencies must be in the classpath when submitting a
job to YARN. For this, it's usually enough to run

{% highlight bash %}
export HADOOP_CLASSPATH=`hadoop classpath`
{% endhighlight %}

in the shell. Note that `hadoop` is the hadoop binary and that `classpath` is an argument that will make it print the configured Hadoop classpath. The classpath returned by `hadoop classpath` also includes the Hadoop configuration directories.

If you are manually assembling the `HADOOP_CLASSPATH` variable, we recommend
adding the Hadoop configuration directories as well.

## Running a job locally

To run a job locally as one JVM process using the mini cluster, the required hadoop dependencies have to be explicitly
added to the classpath of the started JVM process.

To run an application using Maven (also from IDE as a Maven project), the required Hadoop dependencies can be added
as provided to the pom.xml, e.g.:

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.8.3</version>
    <scope>provided</scope>
</dependency>
```

This way it should work both in local and cluster mode where the provided dependencies are added elsewhere as described before.

To run or debug an application in IntelliJ Idea the provided dependencies can be included to the class path
in the "Run|Edit Configurations" window.


## Using `flink-shaded-hadoop-2-uber` jar for resolving dependency conflicts (legacy)

<div class="alert alert-info" markdown="span">
  <strong>Warning:</strong> Starting from Flink 1.11, using `flink-shaded-hadoop-2-uber` releases is not officially supported
  by the Flink project anymore. Users are advised to provide Hadoop dependencies through `HADOOP_CLASSPATH` (see above).
</div>

The Flink project used to (until Flink 1.10) release Hadoop distributions for specific versions, that relocate or exclude several dependencies to reduce the risk of dependency clashes.
These can be found in the [Additional Components]({{ site.download_url }}#additional-components) section of the download page.
For these versions it is sufficient to download the corresponding `Pre-bundled Hadoop` component and putting it into
the `/lib` directory of the Flink distribution.

If the used Hadoop version is not listed on the download page (possibly due to being a Vendor-specific version),
then it is necessary to build [flink-shaded](https://github.com/apache/flink-shaded) against this version.
You can find the source code for this project in the [Additional Components]({{ site.download_url }}#additional-components) section of the download page.

<span class="label label-info">Note</span> If you want to build `flink-shaded` against a vendor specific Hadoop version, you first have to configure the
vendor-specific maven repository in your local maven setup as described [here](https://maven.apache.org/guides/mini/guide-multiple-repositories.html).

Run the following command to build and install `flink-shaded` against your desired Hadoop version (e.g., for version `2.6.5-custom`):

{% highlight bash %}
mvn clean install -Dhadoop.version=2.6.5-custom
{% endhighlight %}

After this step is complete, put the `flink-shaded-hadoop-2-uber` jar into the `/lib` directory of the Flink distribution.


{% top %}
